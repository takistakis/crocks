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
const int kNumValues = 1024;
const int kValueSize = 4000;
const int kKeyValueSize = kKeySize + kValueSize;
char values[kNumValues][kValueSize];

inline void RandomInit() {
  srand(time(nullptr));
  for (int i = 0; i < kNumValues; i++)
    std::generate_n(values[i], kValueSize, rand);
}

std::string RandomValue() {
  return std::string(values[rand() % kNumValues], kValueSize);
}

enum WriteMode { RANDOM, SEQUENTIAL };

// Based on rocksdb::Benchmark::KeyGenerator
// defined in rocksdb/util/db_bench_tool.cc
class KeyGenerator {
 public:
  KeyGenerator(WriteMode mode, int num) : mode_(mode), num_(num), next_(0) {}

  std::string Next() {
    char key[kKeySize];
    switch (mode_) {
      case SEQUENTIAL:
        sprintf(key, "%015d", next_++);
        break;
      case RANDOM:
        sprintf(key, "%015d", rand() % num_);
        break;
    }
    return std::string(key);
  }

 private:
  WriteMode mode_;
  const int num_;
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
