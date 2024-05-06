//
// Created by obin1000 on 5-5-24.
//

#ifndef FSSTP_SPLIT_BLOCK_HPP
#define FSSTP_SPLIT_BLOCK_HPP

#include "fsstp.hpp"

/**
 * Read the input buffer and split it into blocks. Then split each block in given amount of subblocks.
 * @param src
 * @param input_size
 * @param tasks_queue
 * @param number_splits
 */
inline void read_blocks(
    unsigned char *src,
    const size_t input_size,
    atomic_queue::AtomicQueue2<fsst_decompression_task, QUEUE_SIZE> *tasks_queue,
    const unsigned int number_splits) {
    size_t block_size = DESERIALIZE(src);
    size_t location = FSST_BLOCKSIZE_FIELD;
    size_t uncompressed_size_hdr;
    unsigned int block_number = 1;
    while (true) {
        memcpy(&uncompressed_size_hdr, src+location, FSST_UNCOMPRESSED_FIELD);

        location += FSST_UNCOMPRESSED_FIELD;

        auto decoder = std::make_shared<fsst_decoder_t>();
        const auto hdr = fsst_import(decoder.get(), src + location);
        location += hdr;

        const auto data_size = block_size-FSST_UNCOMPRESSED_FIELD-FSST_BLOCKSIZE_FIELD-hdr;
        const auto subblock_size = data_size/number_splits;
        for (int split = 0; split < number_splits-1; split++) {
            fsst_decompression_task task{
                .seq_num = block_number,
                .decoder = decoder,
                .max_uncompressed_size = uncompressed_size_hdr, //TODO: Find uncompressed size per subblock
                .data_len = subblock_size,
                .data = src + location,
            };
            tasks_queue->push(std::move(task));
            location += subblock_size;
            block_number+=1;
        }
        const auto final_subblock_size = data_size - (subblock_size*(number_splits-1));

        fsst_decompression_task final_task{
            .seq_num = block_number,
            .decoder = decoder,
            .max_uncompressed_size = uncompressed_size_hdr,
            .data_len = final_subblock_size,
            .data = src + location,
        };
        tasks_queue->push(std::move(final_task));

        block_number+=1;
        location += final_subblock_size;

        block_size = DESERIALIZE(src+location);
        location += FSST_BLOCKSIZE_FIELD;
        if (location >= input_size) break;
    }
}

inline void decompress_task_thread(atomic_queue::AtomicQueue2<fsst_decompression_task, QUEUE_SIZE> *input_queue,
                            atomic_queue::AtomicQueue2<fsst_block, QUEUE_SIZE> *write_queue) {
    while (true) {
        auto task = input_queue->pop();
        if (task.is_empty()) break;
        fsst_block new_block{
            .seq_num = task.seq_num,
            .data_len = 0,
            .data_stored = std::make_unique<unsigned char[]>(4*task.data_len) // TODO: Find better uncompressed length for split block
        };
        const auto decomp_size = fsst_decompress(task.decoder.get(), task.data_len, task.data,
                        4*task.data_len, new_block.data_stored.get());
        new_block.data_len = decomp_size;
        new_block.data = new_block.data_stored.get();
        write_queue->push(std::move(new_block));
    }
}

/**
 * Parallel decompression of FSST data by splitting the data in blocks and splitting blocks in subblocks
 * @param src Compressed input data
 * @param input_size Length of the compressed inpupt data
 * @param dst Output buffer TODO: Check length
 * @param number_of_threads Number of decompression threads
 * @param splits Split each block in this number of subblocks
 * @return
 */
inline size_t decompress_buffer_tasks(unsigned char *src, size_t input_size, unsigned char *dst, const unsigned int number_of_threads, unsigned int splits){
    std::vector<std::thread> threads(number_of_threads);
    atomic_queue::AtomicQueue2<fsst_decompression_task, QUEUE_SIZE> tasks_queue;
    atomic_queue::AtomicQueue2<fsst_block, QUEUE_SIZE> write_queue;

    std::thread readerThread([&src, &input_size, &tasks_queue, &splits]{ read_blocks(src, input_size, &tasks_queue, splits); });

    for (unsigned int thread_num = 0 ; thread_num < number_of_threads; thread_num++) {
        threads[thread_num] = std::thread([&tasks_queue, &write_queue]{ decompress_task_thread(&tasks_queue, &write_queue); });
    }
    auto dst_size_future = std::async([&write_queue, &dst]() { return combine_results(&write_queue, dst); });

    readerThread.join();

    for (auto &decomp_thread : threads) {
        tasks_queue.push(fsst_decompression_task{.seq_num = 0});
    }

    for (auto &decomp_thread : threads) {
        decomp_thread.join();
    }

    write_queue.push(fsst_block{.seq_num = 0});
    return dst_size_future.get();
}


#endif //FSSTP_SPLIT_BLOCK_HPP
