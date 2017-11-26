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

// g++ -O2 bench.cc -o bench -l crocks -l pthread

#include <assert.h>
#include <getopt.h>
#include <math.h>
#include <stdlib.h>
#include <sys/time.h>

#include <functional>
#include <iomanip>
#include <iostream>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include <crocks/cluster.h>
#include <crocks/status.h>
#include <crocks/write_batch.h>
#include "src/common/util.h"

#include "util.h"

const double kMB = 1048576.0;
const double kGB = kMB * 1024;

std::mutex mutex;

const std::string usage_message(
    "Usage: bench [options] command [args]...\n"
    "\n"
    "Benchmark utility for crocks.\n"
    "\n"
    "Commands:\n"
    "  fill              Fill db with random data sequentially.\n"
    "  fillseq           Sequential writes.\n"
    "  fillrandom        Random writes from <num> threads.\n"
    "  readseq           Sequential reads.\n"
    "  readrandom        Random reads from <num> threads.\n"
    "  readwhilewriting  Reads and writes from <num> threads each.\n"
    "  fillbatch         Random writes in batches from <num> threads.\n"
    "\n"
    "Options:\n"
    "  -e, --etcd <address>  Etcd address [default: localhost:2379].\n"
    "  -s, --size <size>     Database size in GB [default: 1].\n"
    "  -t, --threads <num>   Number of threads [default: 1].\n"
    "  -b, --batch <size>    Batch size in operations [default: 128].\n"
    "  -d, --duration <sec>  Benchmark duration in seconds [default: 10].\n"
    "  -h, --help            Show this help message and exit.\n");

void Report(double iops) {
  std::cout << round(iops) << " ops/sec ";
  std::cout << std::fixed << std::setprecision(2);
  std::cout << "(" << iops * kKeyValueSize / kMB << " MB/sec)" << std::endl;
}

// Based on rocksdb::Duration defined in rocksdb/util/db_bench_tool.cc
class Duration {
 public:
  Duration(uint64_t max_seconds, int64_t max_ops)
      : max_seconds_(max_seconds),
        max_ops_(max_ops),
        ops_(0),
        start_at_(NowMicros()),
        now_(start_at_) {}

  double iops() const {
    double duration = (now_ - start_at_) / 1000000.0;
    return ops_ / duration;
  }

  bool Done(int64_t increment) {
    if (increment <= 0)
      increment = 1;  // avoid Done(0) and infinite loops
    ops_ += increment;

    if (max_seconds_) {
      now_ = NowMicros();
      return ((now_ - start_at_) / 1000000) >= max_seconds_;
    } else {
      // XXX: We subtract increment so it's really min_ops and not max_ops
      bool done = ops_ - increment > max_ops_;
      int index = ((double)ops_ / max_ops_) * 50;
      std::cout << "\r[" << std::string(index, '#')
                << std::string(50 - index, ' ') << "]" << std::flush;
      if (done)
        std::cout << std::endl;
      return done;
    }
  }

 private:
  uint64_t NowMicros() {
    struct timeval tv;
    gettimeofday(&tv, nullptr);
    return static_cast<uint64_t>(tv.tv_sec) * 1000000 + tv.tv_usec;
  }

  uint64_t max_seconds_;
  int64_t max_ops_;
  int64_t ops_;
  uint64_t start_at_;
  uint64_t now_;
};

// Repeatedly run the given function for max_seconds and
// add the number of operations per second to *iops.
void Run(std::function<void(crocks::Cluster*, KeyGenerator*, int)> func,
         crocks::Cluster* db, KeyGenerator* gen, int max_seconds,
         int batch_size, double* iops) {
  assert(max_seconds > 0);
  Duration duration(max_seconds, 0);
  while (!duration.Done(batch_size))
    func(db, gen, batch_size);
  {
    std::lock_guard<std::mutex> lock(mutex);
    *iops += duration.iops();
  }
}

void DoWrites(crocks::Cluster* db, KeyGenerator* gen, int batch_size) {
  for (int i = 0; i < batch_size; i++)
    EnsureRpc(db->Put(gen->Next(), RandomValue()));
}

void DoBatchWrites(crocks::Cluster* db, KeyGenerator* gen, int batch_size) {
  crocks::WriteBatch batch(db);
  for (int i = 0; i < batch_size; i++)
    batch.Put(gen->Next(), RandomValue());
  EnsureRpc(batch.Write());
}

void DoReads(crocks::Cluster* db, KeyGenerator* gen, int batch_size) {
  std::string value;
  for (int i = 0; i < batch_size; i++)
    EnsureRpc(db->Get(gen->Next(), &value));
}

void Fill(crocks::Cluster* db, int num_keys) {
  Duration duration(0, num_keys);
  KeyGenerator gen(SEQUENTIAL, 0);
  // 1MB per batch
  int batch_size = kMB / kKeyValueSize;
  while (!duration.Done(batch_size))
    DoBatchWrites(db, &gen, batch_size);
}

int main(int argc, char** argv) {
  std::string etcd_address = crocks::GetEtcdEndpoint();
  int size = 1;
  int num_threads = 1;
  int batch_size = 128;
  int duration = 10;
  const char* optstring = "e:s:t:b:d:h";
  static struct option longopts[] = {
      // clang-format off
      {"etcd",     required_argument, 0, 'e'},
      {"size",     required_argument, 0, 's'},
      {"threads",  required_argument, 0, 't'},
      {"batch",    required_argument, 0, 'b'},
      {"duration", required_argument, 0, 'd'},
      {"help",     no_argument,       0, 'h'},
      {0, 0, 0, 0},
      // clang-format on
  };
  int c, index = 0;

  while ((c = getopt_long(argc, argv, optstring, longopts, &index)) != -1) {
    switch (c) {
      case 'e':
        etcd_address = optarg;
        break;
      case 's':
        size = std::stoi(optarg);
        break;
      case 't':
        num_threads = std::stoi(optarg);
        break;
      case 'b':
        batch_size = std::stoi(optarg);
        break;
      case 'd':
        duration = std::stoi(optarg);
        break;
      case 'h':
        std::cout << usage_message;
        exit(EXIT_SUCCESS);
      default:
        std::cerr << usage_message;
        exit(EXIT_FAILURE);
    }
  }

  if (argc != optind + 1) {
    std::cerr << usage_message;
    exit(EXIT_FAILURE);
  }

  std::string command = argv[optind];
  int num_keys = size * kGB / kKeyValueSize;

  RandomInit();
  crocks::Cluster* db = crocks::DBOpen(etcd_address);

  if (command == "fill") {
    std::cout << "Filling db with " << size << "GB" << std::endl;
    double duration = Measure(Fill, db, num_keys);
    Report(num_keys / duration);

  } else if (command == "fillseq") {
    std::cout << "Sequential writes" << std::endl;
    KeyGenerator gen(SEQUENTIAL, 0);
    double iops = 0;
    Run(DoWrites, db, &gen, duration, batch_size, &iops);
    Report(iops);

  } else if (command == "fillrandom" || command == "fillbatch") {
    std::cout << "Random writes";
    if (command == "fillbatch") {
      double mb = batch_size * kKeyValueSize / kMB;
      std::cout << " in batches of " << batch_size << " (" << mb << " MB)";
    }
    std::cout << " from " << num_threads << " threads" << std::endl;
    std::vector<std::thread> threads;
    KeyGenerator gen(RANDOM, num_keys);
    double iops = 0;
    auto func = (command == "fillrandom") ? DoWrites : DoBatchWrites;
    for (int i = 0; i < num_threads; i++)
      threads.emplace_back(std::thread(
          [&] { Run(func, db, &gen, duration, batch_size, &iops); }));
    for (int i = 0; i < num_threads; i++)
      threads[i].join();
    Report(iops);

  } else if (command == "readseq") {
    std::cout << "Sequential reads" << std::endl;
    KeyGenerator gen(SEQUENTIAL, 0);
    double iops = 0;
    Run(DoReads, db, &gen, duration, batch_size, &iops);
    Report(iops);

  } else if (command == "readrandom") {
    std::cout << "Random reads from " << num_threads << " threads" << std::endl;
    std::vector<std::thread> threads;
    KeyGenerator gen(RANDOM, num_keys);
    double iops = 0;
    for (int i = 0; i < num_threads; i++)
      threads.emplace_back(std::thread(
          [&] { Run(DoReads, db, &gen, duration, batch_size, &iops); }));
    for (int i = 0; i < num_threads; i++)
      threads[i].join();
    Report(iops);

  } else if (command == "readwhilewriting") {
    std::cout << "Random reads and writes from " << num_threads
              << " threads each" << std::endl;
    std::vector<std::thread> write_threads;
    std::vector<std::thread> read_threads;
    KeyGenerator gen(RANDOM, num_keys);
    double write_iops = 0;
    double read_iops = 0;
    for (int i = 0; i < num_threads; i++) {
      read_threads.emplace_back(std::thread(
          [&] { Run(DoReads, db, &gen, duration, batch_size, &read_iops); }));
      write_threads.emplace_back(std::thread(
          [&] { Run(DoWrites, db, &gen, duration, batch_size, &write_iops); }));
    }
    for (int i = 0; i < num_threads; i++) {
      read_threads[i].join();
      write_threads[i].join();
    }
    std::cout << "Reads:  ";
    Report(read_iops);
    std::cout << "Writes: ";
    Report(write_iops);

  } else {
    std::cerr << usage_message;
    exit(EXIT_FAILURE);
  }

  delete db;

  return 0;
}
