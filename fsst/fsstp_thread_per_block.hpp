//
// Created by obin1000 on 5-5-24.
//

#ifndef FSSTP_THREAD_PER_BLOCK_HPP
#define FSSTP_THREAD_PER_BLOCK_HPP

#include "fsstp.hpp"

/**
 * Contains compressed data
 */
struct fsst_block_location {
    bool is_empty(){
        return data_len == 0;
    }
    size_t uncompressed_size;
    size_t output_start;
    size_t data_len;
    unsigned char* data;
    std::unique_ptr<unsigned char[]> data_stored;
};

/**
 * Reads the compressed input buffer and splits it into blocks
 * @param src
 * @param input_size
 * @param blocks_queue
 */
inline void read_blocks(unsigned char *src, const size_t input_size, atomic_queue::AtomicQueue2<fsst_block_location, QUEUE_SIZE> *blocks_queue) {
    size_t block_size = DESERIALIZE(src);
    size_t input_location = FSST_BLOCKSIZE_FIELD;
    size_t uncompressed_size;
    size_t output_location = 0;
    while(true) {
        fsst_block_location block{};
        memcpy(&uncompressed_size, src+input_location, FSST_UNCOMPRESSED_FIELD);

        input_location += FSST_UNCOMPRESSED_FIELD;

        const size_t data_size = block_size - FSST_UNCOMPRESSED_FIELD - FSST_BLOCKSIZE_FIELD;

        block.output_start = output_location;
        block.data_len = data_size;
        block.data = src + input_location;
        block.uncompressed_size = uncompressed_size;

        output_location += uncompressed_size;

        input_location += data_size;
        block_size = DESERIALIZE(src+input_location);
        input_location += FSST_BLOCKSIZE_FIELD;
        blocks_queue->push(std::move(block));
        if (input_location >= input_size) break;
    }
}
inline void read_blocks(std::vector<unsigned char> &src, atomic_queue::AtomicQueue2<fsst_block_location, QUEUE_SIZE> *blocks_queue) {
    read_blocks(src.data(), src.size(), blocks_queue);
}
inline fsst_block_location decompress_block(fsst_block_location& block_data) {
    block_data.data_stored = std::make_unique<unsigned char[]>(block_data.uncompressed_size);

    fsst_decoder_t decoder;
    const size_t hdr = fsst_import(&decoder, block_data.data);
    fsst_decompress(&decoder, block_data.data_len - hdr, block_data.data + hdr,
                    block_data.uncompressed_size, block_data.data_stored.get());
    block_data.data = block_data.data_stored.get();
    return std::move(block_data);
}

/**
 * Reads compressed block and decompresses it
 * @param blocks_queue
 * @param write_queue
 */
inline void decompress_thread(atomic_queue::AtomicQueue2<fsst_block_location, QUEUE_SIZE> *blocks_queue,
                              atomic_queue::AtomicQueue2<fsst_block_location, QUEUE_SIZE> *write_queue) {
    while (true) {
        auto block_data = blocks_queue->pop();
        if (block_data.is_empty()) break;
        auto new_block = decompress_block(block_data);
        write_queue->push(std::move(new_block));
    }
}

/**
 * When decompressing one block at a time (without splitting them), it is possible to precalculate the output locations
 * @param write_queue
 * @param output_buf
 * @return
 */
inline size_t combine_given_distance(atomic_queue::AtomicQueue2<fsst_block_location, QUEUE_SIZE> *write_queue, unsigned char *output_buf) {
    size_t size = 0;
    while (true) {
        auto data= write_queue->pop();
        if (data.is_empty())
            break;
        std::copy_n(data.data, data.uncompressed_size, output_buf + data.output_start);
        size += data.uncompressed_size;
    }
    return size;
}

/**
 * Parallel decompression of FSST data by splitting the data in blocks
 * @param src Compressed input data
 * @param input_size Length of the compressed inpupt data
 * @param dst Output buffer TODO: Check length
 * @param number_of_threads Number of decompression threads
 * @return
 */
inline size_t decompress_buffer(unsigned char *src, size_t input_size, unsigned char *dst, const size_t number_of_threads){
    std::vector<std::thread> threads(number_of_threads);
    atomic_queue::AtomicQueue2<fsst_block_location, QUEUE_SIZE> blocks_queue;
    atomic_queue::AtomicQueue2<fsst_block_location, QUEUE_SIZE> write_queue;

    std::thread readerThread([&src, &input_size, &blocks_queue]{ read_blocks(src, input_size, &blocks_queue); });

    for (unsigned int thread_num = 0 ; thread_num < number_of_threads; thread_num++) {
        threads[thread_num] = std::thread([&blocks_queue, &write_queue]{ decompress_thread(&blocks_queue, &write_queue); });
    }
    auto dst_size_future = std::async([&write_queue, &dst]() { return combine_given_distance(&write_queue, dst); });

    readerThread.join();

    for (auto &decomp_thread : threads) {
        blocks_queue.push(fsst_block_location{.data_len = 0});
    }

    for (auto &decomp_thread : threads) {
        decomp_thread.join();
    }

    write_queue.push(fsst_block_location{.data_len = 0});
    return dst_size_future.get();
}


#endif //FSSTP_THREAD_PER_BLOCK_HPP
