/*
 * Copyright (c) 2022, NVIDIA CORPORATION.  All rights reserved.
 *
 * NVIDIA CORPORATION and its licensors retain all intellectual property
 * and proprietary rights in and to this software, related documentation
 * and any modifications thereto.  Any use, reproduction, disclosure or
 * distribution of this software and related documentation without an express
 * license agreement from NVIDIA CORPORATION is strictly prohibited.
 */

#pragma once

#include <cuda_runtime.h>

namespace ans {
namespace hlif {

void batchCompress(const CompressArgs& compress_args, const uint32_t max_ctas, cudaStream_t stream);

void batchDecompress(
    const uint8_t* comp_buffer,
    uint8_t* decomp_buffer,
    const size_t raw_chunk_size,
    uint32_t* ix_chunk,
    const size_t num_chunks,
    const size_t* comp_chunk_offsets,
    const size_t* comp_chunk_sizes,
    const uint32_t max_ctas,
    cudaStream_t stream,
    nvcompStatus_t* output_status);

size_t getBatchedCompMaxBlockOccupancy(const int device_id);

size_t getBatchedDecompMaxBlockOccupancy(const int device_id);

size_t getBatchedCompChunksPerCTA();

size_t getChunkTmpSize();

} // namespace hlif
} // namespace ans