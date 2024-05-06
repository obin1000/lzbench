// this software is distributed under the MIT License (http://www.opensource.org/licenses/MIT):
//
// Copyright 2018-2019, CWI, TU Munich, FSU Jena
//
// Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files
// (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify,
// merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// - The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
// OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
// LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR
// IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
//
// You can contact the authors via the FSST source repository : https://github.com/cwida/fsst
#ifdef FSST12
#include "fsst12.h" // the official FSST API -- also usable by C mortals
#else
#include "fsst.h" // the official FSST API -- also usable by C mortals
#endif
#include "fsstp.hpp"
#include "fsstp_split_block.hpp"
#include "fsstp_thread_per_block.hpp"

#include <chrono>
#include <iterator>

using namespace std;

// Utility to compress and decompress (-d) data with FSST (using stdin and stdout).
//
// The utility has a poor-man's async I/O in that it uses double buffering for input and output,
// and two background pthreads for reading and writing. The idea is to make the CPU overlap with I/O.
//
// The data format is quite simple. A FSST compressed file is a sequence of blocks, each with format:
// (1) 3-byte block length field (max blocksize is hence 16MB). This byte-length includes (1), (2) and (3).
// (2) FSST dictionary as produced by fst_export().
// (3) the FSST compressed data.
//
// The natural strength of FSST is in fact not block-based compression, but rather the compression and
// *individual* decompression of many small strings separately. Think of compressed databases and (column-store)
// data formats. But, this utility is to serve as an apples-to-apples comparison point with utilities like lz4.

vector<unsigned char> read_file_chunked(ifstream &src) {
    size_t reads = 100000;
    std::vector<unsigned char> data(2000000000);
    size_t size = 0;
    while (true) {
        src.read(reinterpret_cast<char *>(data.data() + size), reads);
        auto count = src.gcount();
        size += count;
        if (count < reads) break;
    }
    data.resize(size);
    return data;
}

vector<unsigned char> read_file_direct(ifstream &src) {
    std::vector<unsigned char> fileContents;
    fileContents.reserve(2000000000);
    fileContents.assign(std::istreambuf_iterator<char>(src),
                        std::istreambuf_iterator<char>());
    return fileContents;
}

int main(int argc, char* argv[]) {
   if (argc < 2 || argc > 4 || (argc == 4 && (argv[1][0] != '-' || argv[1][1] != 'd' || argv[1][2]))) {
      cerr << "usage: " << argv[0] << " -d infile outfile" << endl;
      cerr << "       " << argv[0] << " infile outfile" << endl;
      cerr << "       " << argv[0] << " infile" << endl;
      return -1;
   }
   int decompress = (argc == 4);

   string srcfile(argv[1+decompress]);
   string dstfile;
   if (argc == 2) {
      dstfile = srcfile + ".fsst";
   } else {
      dstfile = argv[2+decompress];
   }
   ifstream src;
   ofstream dst;
   src.open(srcfile, ios::binary);
   dst.open(dstfile, ios::binary);
   dst.exceptions(ios_base::failbit);
   dst.exceptions(ios_base::badbit);
   src.exceptions(ios_base::badbit);

   size_t number_of_threads = 1;
   size_t block_size = (1ULL<<20);
   const int method = 2;


   if (decompress) {
       if (method == 1) {
           decompress_file(src, dst, number_of_threads);
       }
       if (method == 2) {
           auto data = read_file_chunked(src);
           std::vector<unsigned char> outbuf(4*data.size());
           auto start = chrono::high_resolution_clock::now();
           auto size = decompress_buffer(data.data(), data.size(), outbuf.data(), number_of_threads);
           auto end = chrono::high_resolution_clock::now();
           auto duration = chrono::duration_cast<chrono::microseconds>(end - start);
           dst.write(reinterpret_cast<const char *>(outbuf.data()), size);

           cout << (decompress ? "Dec" : "C") << "ompressed in " << duration.count()/1000 << " miliseconds" << endl;
       }

   } else {
       if (method == 1) {
           compress_file(src, dst, block_size);
       }
       if (method == 2) {
           auto data = read_file_chunked(src);
           std::vector<unsigned char> outbuf(4*data.size());
           auto size = compress_buffer(data.data(), data.size(), outbuf.data(), block_size);
           dst.write(reinterpret_cast<const char *>(outbuf.data()), size);
       }
   }



   src.close();
   dst.close();
}
