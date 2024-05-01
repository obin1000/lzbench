//
// Created by obin1000 on 30-4-24.
//

#ifndef FSST_FSSTP_HPP
#define FSST_FSSTP_HPP

#include "atomic_queue/atomic_queue.h"
#include <iostream>
#include <fstream>
#include <mutex>
#include <vector>
#include <thread>
#include <array>
#include <future>
#include <list>


#define FSST_BLOCKSIZE_FIELD 3
#define FSST_UNCOMPRESSED_FIELD sizeof(size_t)
#define FSST_BLOCKHEADER_SIZE (FSST_BLOCKSIZE_FIELD + FSST_UNCOMPRESSED_FIELD)

#define QUEUE_SIZE 64


#define DESERIALIZE(p) (((unsigned long long) (p)[0]) << 16) | (((unsigned long long) (p)[1]) << 8) | ((unsigned long long) (p)[2])
#define SERIALIZE(l, p) { (p)[0] = ((l)>>16)&255; (p)[1] = ((l)>>8)&255; (p)[2] = (l)&255; }

struct fsst_block {
    bool is_empty(){
        return data_len == 0;
    }
    size_t seq_num;
    size_t data_len;
    unsigned char* data;
    std::unique_ptr<unsigned char[]> data_stored;
};

size_t get_number_of_blocks(std::ifstream &src) {
    unsigned char block_size_tmp[FSST_BLOCKSIZE_FIELD];
    size_t block_size;
    unsigned int block_number;
    size_t location = 0;
    for (block_number = 1; true; block_number++) {
        src.read(reinterpret_cast<char *>(&block_size_tmp), FSST_BLOCKSIZE_FIELD);
        block_size = DESERIALIZE(block_size_tmp);
        location += block_size;
        src.seekg(location);
        if (src.peek() == EOF) break;
    }
    return block_number;
}

fsst_block decompress_block(const fsst_block& block_data) {
    fsst_block block{};
    size_t uncompressed_size_hdr;
    memcpy(&uncompressed_size_hdr, block_data.data, FSST_UNCOMPRESSED_FIELD);

    block.data_stored = std::make_unique<unsigned char[]>(uncompressed_size_hdr);

    fsst_decoder_t decoder;
    size_t hdr = fsst_import(&decoder, block_data.data+FSST_UNCOMPRESSED_FIELD);
    fsst_decompress(&decoder, block_data.data_len - FSST_UNCOMPRESSED_FIELD - hdr, block_data.data + FSST_UNCOMPRESSED_FIELD + hdr,
                    uncompressed_size_hdr, block.data_stored.get());
    block.data = block.data_stored.get();
    block.data_len = uncompressed_size_hdr;
    return block;
}

fsst_block compress_block(const fsst_block& data) {
    size_t buffer_size = 16 + 2 * data.data_len;
    fsst_block block{};
    block.seq_num = data.seq_num;
    block.data_stored = std::make_unique<unsigned char[]>(buffer_size);

    size_t compressedLen;
    unsigned char *start_ptr;

    auto encoder = fsst_create(1, &data.data_len, const_cast<const unsigned char **>(&data.data), false);
    unsigned char tmp[FSST_MAXHEADER];

    auto hdr = fsst_export(encoder, tmp);

    auto num_compressed = fsst_compress(encoder, 1, &data.data_len, const_cast<const unsigned char **>(&data.data), buffer_size,
                                        block.data_stored.get() + hdr + FSST_BLOCKHEADER_SIZE, &compressedLen, &start_ptr);

    if (num_compressed < 1) {
        std::cout << "Compression failed" << std::endl;
        return fsst_block{};
    }
    size_t total_size = compressedLen + hdr + FSST_BLOCKHEADER_SIZE;
    SERIALIZE(total_size, block.data_stored.get()); // block starts with size

    memcpy(block.data_stored.get() + FSST_BLOCKSIZE_FIELD, &data.data_len, FSST_UNCOMPRESSED_FIELD); // followed by uncompressed size
    std::copy(tmp, tmp + hdr, block.data_stored.get() + FSST_BLOCKHEADER_SIZE);
    fsst_destroy(encoder);

    block.data_len = total_size;
    block.data = block.data_stored.get();

    return block;
}

void read_blocks(std::ifstream &src, atomic_queue::AtomicQueue2<fsst_block, QUEUE_SIZE> *blocks_queue) {
    unsigned char block_size_tmp[FSST_BLOCKSIZE_FIELD];
    src.read(reinterpret_cast<char *>(&block_size_tmp), FSST_BLOCKSIZE_FIELD);
    size_t block_size = DESERIALIZE(block_size_tmp);
    for (unsigned int block_number = 0; true; block_number++) {
        fsst_block block{};
        block.seq_num = block_number;
        block.data_stored = std::make_unique<unsigned char[]>(block_size);
        src.read(reinterpret_cast<char *>(block.data_stored.get()), block_size);
        block.data = block.data_stored.get();
        block.data_len = block_size;
        if (src.peek() != EOF) {
            block_size = DESERIALIZE(block.data+block_size-FSST_BLOCKSIZE_FIELD);
        }
        blocks_queue->push(std::move(block));

        if (src.peek() == EOF) break;
    }
}

void read_blocks(unsigned char *src, size_t input_size, atomic_queue::AtomicQueue2<fsst_block, QUEUE_SIZE> *blocks_queue) {
    size_t block_size = DESERIALIZE(src);
    size_t location = FSST_BLOCKSIZE_FIELD;
    for (unsigned int block_number = 0; true; block_number++) {
        fsst_block block{};
        block.seq_num = block_number;
        block.data_len = block_size;
        block.data = src + location;

        location += block_size;
        block_size = DESERIALIZE(block.data+block_size-FSST_BLOCKSIZE_FIELD);
        blocks_queue->push(std::move(block));
        if (location >= input_size) break;
    }
}

void read_blocks(std::vector<unsigned char> &src, atomic_queue::AtomicQueue2<fsst_block, QUEUE_SIZE> *blocks_queue) {
    read_blocks(src.data(), src.size(), blocks_queue);
}

void decompress_thread(atomic_queue::AtomicQueue2<fsst_block, QUEUE_SIZE> *blocks_queue,
                       atomic_queue::AtomicQueue2<fsst_block, QUEUE_SIZE> *write_queue) {
    while (true) {
        fsst_block block_data = blocks_queue->pop();
        if (block_data.is_empty()) break;
        auto new_block = decompress_block(block_data);
        new_block.seq_num = block_data.seq_num;
        write_queue->push(std::move(new_block));
    }
}



size_t combine_results(atomic_queue::AtomicQueue2<fsst_block, QUEUE_SIZE> *write_queue, unsigned char *output_buf) {
    size_t location = 0;
    unsigned int current_block = 0;
    std::list<fsst_block> blocks;
    while (true) {
        auto data= write_queue->pop();
        if (data.is_empty()) break;
        // Check if the block is the next one in the sequence
        if (data.seq_num != current_block) {
            blocks.push_back(std::move(data));
            continue;
        }
        // Append the data to the output
        memcpy(output_buf + location, data.data, data.data_len);
        location += data.data_len;
        current_block++;

        // Check if the next blocks are already received
        auto block_it = blocks.begin();
        while (block_it != blocks.end()) {
            if (block_it->seq_num == current_block) {
                memcpy(output_buf + location, block_it->data, block_it->data_len);
                location += block_it->data_len;
                current_block++;
                blocks.erase(block_it);
                block_it = blocks.begin();
                continue;
            }
            block_it++;
        }
    }
    return location;
}

void combine_results(atomic_queue::AtomicQueue2<fsst_block, QUEUE_SIZE> *write_queue, std::vector<unsigned char> *output_buf) {
    auto size = combine_results(write_queue, output_buf->data());
    output_buf->resize(size);
}

void writer(atomic_queue::AtomicQueue2<fsst_block, QUEUE_SIZE> *write_queue, std::ofstream &dst) {
    int current_block = 0;
    std::list<fsst_block> blocks;
    while (true) {
        auto data = write_queue->pop();
        if (data.is_empty())
            break;
        // Check if the block is the next one in the sequence
        if (data.seq_num != current_block) {
            blocks.push_back(std::move(data));
            continue;
        }
        // Append the data to the output
        dst.write(reinterpret_cast<const char *>(data.data), data.data_len);
        current_block++;
        // Check if the next blocks are already received
        auto block_it = blocks.begin();
        while (block_it != blocks.end()) {
            if (block_it->seq_num == current_block) {
                dst.write(reinterpret_cast<const char *>(block_it->data), block_it->data_len);
                current_block++;
                blocks.erase(block_it);
                block_it = blocks.begin();
                continue;
            }
            block_it++;
        }
    }
}

void compress_blocks(atomic_queue::AtomicQueue2<fsst_block, QUEUE_SIZE> *data_queue,
                     atomic_queue::AtomicQueue2<fsst_block, QUEUE_SIZE> *blocks_queue) {
    while (true) {
        fsst_block block = data_queue->pop();
        if (block.is_empty()) break;
        auto new_block = compress_block(block);
        new_block.seq_num = block.seq_num;
        blocks_queue->push(std::move(new_block));
    }
}


void read_data(std::ifstream &src, atomic_queue::AtomicQueue2<fsst_block, QUEUE_SIZE> *data_queue, size_t block_size) {
    for (unsigned int block_number = 0; true; block_number++) {
        fsst_block block{};
        block.seq_num = block_number;
        block.data_stored = std::make_unique<unsigned char[]>(block_size);
        src.read(reinterpret_cast<char *>(block.data_stored.get()), block_size);
        auto num_read = src.gcount();
        block.data = block.data_stored.get();
        block.data_len = num_read;
        data_queue->push(std::move(block));
        if (num_read < block_size) break;
    }
}

void read_data(unsigned char *src, size_t src_size, atomic_queue::AtomicQueue2<fsst_block, QUEUE_SIZE> *data_queue, size_t block_size) {
    size_t current_size = 0;
    size_t read_size;
    for (unsigned int block_number = 0; true; block_number++) {
        fsst_block block{};
        block.seq_num = block_number;
        if ((src_size - current_size) < block_size) {
            read_size = src_size - current_size;
        }else {
            read_size = block_size;
        }
        block.data = src + current_size;
        block.data_len = read_size;
        data_queue->push(std::move(block));
        current_size += read_size;
        if (read_size < block_size) break;
    }
}

void read_data(std::vector<unsigned char> &src, atomic_queue::AtomicQueue2<fsst_block, QUEUE_SIZE> *data_queue, size_t block_size) {
    return read_data(src.data(), src.size(), data_queue, block_size);
}

void compress_file(std::ifstream &src, std::ofstream &dst, size_t block_size) {
    atomic_queue::AtomicQueue2<fsst_block, QUEUE_SIZE> data_queue{};
    atomic_queue::AtomicQueue2<fsst_block, QUEUE_SIZE> blocks_queue{};

    std::thread readerThread([&src, &data_queue, &block_size]{ read_data(src, &data_queue, block_size); });

    std::thread compressThread([&data_queue, &blocks_queue]{ compress_blocks(&data_queue, &blocks_queue); });

    std::thread writerThread([&blocks_queue, &dst]{ writer(&blocks_queue, dst); });

    readerThread.join();
    data_queue.push(fsst_block{.data_len = 0});

    compressThread.join();
    blocks_queue.push(fsst_block{.data_len = 0});

    writerThread.join();
}

void compress_buffer(std::vector<unsigned char> &src, std::vector<unsigned char> *dst, size_t block_size) {
    atomic_queue::AtomicQueue2<fsst_block, QUEUE_SIZE> data_queue{};
    atomic_queue::AtomicQueue2<fsst_block, QUEUE_SIZE> blocks_queue{};

    std::thread readerThread([&src, &data_queue, &block_size]{ read_data(src, &data_queue, block_size); });

    std::thread compressThread([&data_queue, &blocks_queue]{ compress_blocks(&data_queue, &blocks_queue); });

    std::thread writerThread([&blocks_queue, &dst]{ combine_results(&blocks_queue, dst); });

    readerThread.join();
    data_queue.push(fsst_block{.data_len = 0});

    compressThread.join();
    blocks_queue.push(fsst_block{.data_len = 0});

    writerThread.join();
}

size_t compress_buffer(unsigned char *src, size_t src_length, unsigned char *dst, size_t block_size) {
    atomic_queue::AtomicQueue2<fsst_block, QUEUE_SIZE> data_queue{};
    atomic_queue::AtomicQueue2<fsst_block, QUEUE_SIZE> blocks_queue{};

    std::thread readerThread([&src, &src_length, &data_queue, &block_size]{ read_data(src, src_length, &data_queue, block_size); });

    std::thread compressThread([&data_queue, &blocks_queue]{ compress_blocks(&data_queue, &blocks_queue); });

    auto dst_size_future = std::async(([&blocks_queue, &dst]{ return combine_results(&blocks_queue, dst); }));

    readerThread.join();
    data_queue.push(fsst_block{.data_len = 0});

    compressThread.join();
    blocks_queue.push(fsst_block{.data_len = 0});

    return dst_size_future.get();
}

void decompress_file(std::ifstream &src, std::ofstream &dst, const size_t number_of_threads){
    std::vector<std::thread> threads(number_of_threads);
    atomic_queue::AtomicQueue2<fsst_block, QUEUE_SIZE> blocks_queue;
    atomic_queue::AtomicQueue2<fsst_block, QUEUE_SIZE> write_queue;


    std::thread readerThread([&src, &blocks_queue]{ read_blocks(src, &blocks_queue); });
    for (unsigned int thread_num = 0 ; thread_num < number_of_threads; thread_num++) {
        threads[thread_num] = std::thread([&blocks_queue, &write_queue]{ decompress_thread(&blocks_queue, &write_queue); });
    }
    std::thread writerThread([&write_queue, &dst]{ writer(&write_queue, dst); });

    readerThread.join();
    for (auto &decomp_thread : threads) {
        blocks_queue.push(fsst_block{.data_len = 0});
    }

    for (auto &decomp_thread : threads) {
        decomp_thread.join();
    }

    write_queue.push(fsst_block{.data_len = 0});
    writerThread.join();
}

void decompress_buffer(std::vector<unsigned char> &src, std::vector<unsigned char> *dst, const size_t number_of_threads){
    std::vector<std::thread> threads(number_of_threads);
    atomic_queue::AtomicQueue2<fsst_block, QUEUE_SIZE> blocks_queue;
    atomic_queue::AtomicQueue2<fsst_block, QUEUE_SIZE> write_queue;


    std::thread readerThread([&src, &blocks_queue]{ read_blocks(src, &blocks_queue); });
    for (unsigned int thread_num = 0 ; thread_num < number_of_threads; thread_num++) {
        threads[thread_num] = std::thread([&blocks_queue, &write_queue]{ decompress_thread(&blocks_queue, &write_queue); });
    }
    std::thread writerThread([&write_queue, &dst]{ combine_results(&write_queue, dst); });

    readerThread.join();
    for (auto &decomp_thread : threads) {
        blocks_queue.push(fsst_block{.data_len = 0});
    }

    for (auto &decomp_thread : threads) {
        decomp_thread.join();
    }

    write_queue.push(fsst_block{});
    writerThread.join();
}

size_t decompress_buffer(unsigned char *src, size_t input_size, unsigned char *dst, const size_t number_of_threads){
    std::vector<std::thread> threads(number_of_threads);
    auto blocks_queue = std::make_shared<atomic_queue::AtomicQueue2<fsst_block, QUEUE_SIZE, true>>();
    auto write_queue = std::make_shared<atomic_queue::AtomicQueue2<fsst_block, QUEUE_SIZE>>();

    std::thread readerThread([&src, &input_size, &blocks_queue]{ read_blocks(src, input_size, blocks_queue.get()); });

    for (unsigned int thread_num = 0 ; thread_num < number_of_threads; thread_num++) {
        threads[thread_num] = std::thread([&blocks_queue, &write_queue]{ decompress_thread(blocks_queue.get(), write_queue.get()); });
    }
    auto dst_size_future = std::async([&write_queue, &dst]() { return combine_results(write_queue.get(), dst); });

    readerThread.join();

    for (auto &decomp_thread : threads) {
        blocks_queue->push(fsst_block{.data_len = 0});
    }

    for (auto &decomp_thread : threads) {
        decomp_thread.join();
    }

    write_queue->push(fsst_block{.data_len = 0});
    return dst_size_future.get();
}

#endif //FSST_FSSTP_HPP
