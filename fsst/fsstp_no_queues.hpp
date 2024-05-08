//
// Created by obin1000 on 5-5-24.
//

#ifndef FSSTP_NO_QUEUES_HPP
#define FSSTP_NO_QUEUES_HPP

#include "fsstp.hpp"

namespace noqueue {

    inline fsst_block_location decompress_block(fsst_block_location& block_data) {
        block_data.data_stored = std::make_unique<unsigned char[]>(block_data.uncompressed_size);

        fsst_decoder_t decoder;
        const size_t hdr = fsst_import(&decoder, block_data.data);
        fsst_decompress(&decoder, block_data.data_len - hdr, block_data.data + hdr,
                        block_data.uncompressed_size, block_data.data_stored.get());
        block_data.data = block_data.data_stored.get();
        return std::move(block_data);
    }

    // /**
    //  * Reads compressed block and decompresses it
    //  * @param blocks_queue
    //  * @param dst
    //  */
    // inline size_t decompress_thread(atomic_queue::AtomicQueue2<fsst_block_location, QUEUE_SIZE> *blocks_queue,
    //                               unsigned char *dst) {
    //     size_t size = 0;
    //     while (true) {
    //         auto block_data = blocks_queue->pop();
    //         if (block_data.is_empty()) break;
    //         auto new_block = decompress_block(block_data);
    //         std::copy_n(block_data.data, block_data.uncompressed_size, dst + block_data.output_start);
    //         size += block_data.uncompressed_size;
    //     }
    //     return size;
    // }

    /**
     * Reads the compressed input buffer and splits it into blocks
     * @param thread_num
     * @param total_threads
     * @param src
     * @param input_size
     * @param dst
     */
    inline size_t decompress_steps(const unsigned int thread_num, const unsigned int total_threads, unsigned char *src, const size_t input_size, unsigned char *dst) {
        size_t input_location = 0;
        size_t output_location = 0;
        size_t uncompressed_size;
        fsst_decoder_t decoder;
        size_t total_size = 0;

        for (unsigned int block_num = 0 ; true ; block_num++) {
            // Iterate over the blocks in the input data until a block for this thread is found
            if (input_location >= input_size) break;
            const auto block_size = DESERIALIZE(src+input_location);
            memcpy(&uncompressed_size, src+input_location+FSST_BLOCKSIZE_FIELD, FSST_UNCOMPRESSED_FIELD);

            if (total_threads > 1 && ((block_num % total_threads) != thread_num)) {
                output_location += uncompressed_size;
                input_location += block_size;
                continue;
            }
            // Decompress block
            input_location += FSST_BLOCKSIZE_FIELD;
            input_location += FSST_UNCOMPRESSED_FIELD;

            const size_t hdr = fsst_import(&decoder, src+input_location);

            input_location += hdr;

            const auto data_size = block_size - hdr - FSST_BLOCKSIZE_FIELD - FSST_UNCOMPRESSED_FIELD;

            const auto size = fsst_decompress(&decoder, data_size, src+input_location,
                            uncompressed_size, dst + output_location);

            total_size += size;

            output_location += uncompressed_size;
            input_location += data_size;
        }
        return total_size;
    }


    /**
     * Parallel decompression of FSST data by splitting the data in blocks
     * @param src Compressed input data
     * @param input_size Length of the compressed inpupt data
     * @param dst Output buffer TODO: Check length
     * @param num_decomp_threads Number of decompression threads
     * @return
     */
    inline size_t decompress_buffer(unsigned char *src, size_t input_size, unsigned char *dst, const unsigned int num_decomp_threads){
        std::vector<std::future<size_t>> decomp_threads(num_decomp_threads);

        for (unsigned int thread_num = 0 ; thread_num < num_decomp_threads; thread_num++) {
            decomp_threads[thread_num] = std::async([thread_num, num_decomp_threads, &src, input_size, &dst]()->size_t{ return decompress_steps(thread_num, num_decomp_threads, src, input_size, dst); });
        }

        size_t total_size = 0;
        for (auto &decomp_thread : decomp_threads) {
            total_size += decomp_thread.get();
        }
        return total_size;
    }
}
#endif //FSSTP_NO_QUEUES_HPP
