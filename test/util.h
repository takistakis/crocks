// Copyright 2017 Panagiotis Ktistakis <panktist@gmail.com>
//
// This file is part of crocks.
//
// crocks is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// crocks is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with crocks.  If not, see <http://www.gnu.org/licenses/>.

#include <stdio.h>
#include <stdlib.h>
#include <time.h>

#include <algorithm>
#include <chrono>
#include <iostream>
#include <string>
#include <type_traits>

const int kKeySize = 16;
const int kBlobSize = 1024 * 1024;

enum WriteMode { RANDOM, SEQUENTIAL };

// Based on rocksdb::Benchmark::KeyGenerator
// defined in rocksdb/util/db_bench_tool.cc
class Generator {
 public:
  Generator(WriteMode mode, int num_keys, int value_size)
      : mode_(mode), num_keys_(num_keys), value_size_(value_size), next_(0) {
    srand(time(nullptr));
    std::generate_n(blob_, kBlobSize, rand);
  }

  std::string NextKey() {
    char key[kKeySize];
    switch (mode_) {
      case SEQUENTIAL:
        sprintf(key, "%015d", next_++);
        break;
      case RANDOM:
        sprintf(key, "%015d", rand() % num_keys_);
        break;
    }
    return std::string(key);
  }

  std::string NextValue() {
    // Random index for blob_ small enough to not overflow
    int start = rand() % (kBlobSize - value_size_);
    return std::string(&blob_[start], value_size_);
  }

 private:
  char blob_[kBlobSize];
  WriteMode mode_;
  int num_keys_;
  int value_size_;
  int next_;
};

template <typename F, typename... Args>
inline double Measure(F func, Args&&... args) {
  // steady_clock: monotonic clock that will never be adjusted.
  // Another option is high_resolution_clock, which is defined as the clock with
  // the shortest tick period available, but it seems that steady_clock is the
  // best choice.
  std::chrono::steady_clock::time_point t1, t2;
  double duration;

  t1 = std::chrono::steady_clock::now();
  func(std::forward<Args>(args)...);
  t2 = std::chrono::steady_clock::now();
  duration =
      std::chrono::duration_cast<std::chrono::milliseconds>(t2 - t1).count();

  double seconds = duration / 1000.0;
  std::cout << "Done in " << seconds << " seconds" << std::endl;
  return seconds;
}
