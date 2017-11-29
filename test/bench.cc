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
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <utility>
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
    "  -v, --value <size>    Value size in bytes [default: 4000].\n"
    "  -t, --threads <num>   Number of threads [default: 1].\n"
    "  -b, --batch <size>    Batch size in operations [default: 128].\n"
    "  -d, --duration <sec>  Benchmark duration in seconds [default: 10].\n"
    "  -h, --help            Show this help message and exit.\n");

void Report(double iops, int value_size, bool nl = true) {
  std::cout << std::fixed << std::setprecision(0);
  std::cout << round(iops) << "\t";
  std::cout << std::fixed << std::setprecision(2);
  std::cout << iops * (kKeySize + value_size) / kMB;
  if (nl)
    std::cout << std::endl;
}

uint64_t NowMicros() {
  struct timeval tv;
  gettimeofday(&tv, nullptr);
  return static_cast<uint64_t>(tv.tv_sec) * 1000000 + tv.tv_usec;
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
  uint64_t max_seconds_;
  int64_t max_ops_;
  int64_t ops_;
  uint64_t start_at_;
  uint64_t now_;
};

// Repeatedly run the given function for max_seconds and
// add the number of operations per second to *iops.
void Run(std::function<void(crocks::Cluster*, Generator*, int)> func,
         crocks::Cluster* db, Generator* gen, int max_seconds, int batch_size,
         double* iops) {
  assert(max_seconds > 0);
  Duration duration(max_seconds, 0);
  while (!duration.Done(batch_size))
    func(db, gen, batch_size);
  {
    std::lock_guard<std::mutex> lock(mutex);
    *iops += duration.iops();
  }
}

void Latency(crocks::Cluster* db, Generator* gen, int max_seconds,
             int batch_size) {
  Duration duration(max_seconds, 0);
  std::map<int, int> map;
  while (!duration.Done(batch_size)) {
    for (int i = 0; i < batch_size; i++) {
      auto start = NowMicros();
      EnsureRpc(db->Put(gen->NextKey(), gen->NextValue()));
      map[NowMicros() - start]++;
    }
  }
  int all = 0;
  for (const auto& pair : map)
    all += pair.second;
  assert(all > 0);
  double last_perc = 0.0;
  std::map<int, int>::iterator it = map.begin();
  int min = it->first;
  int max = it->first;
  int ops = 0;
  uint64_t sum = 0;
  for (; it != map.end(); ++it) {
    sum += it->first * it->second;
    ops += it->second;
    double perc = (double)ops / all;
    if (last_perc < 0.50 && perc >= 0.50)
      std::cout << "p50:\t" << it->first << std::endl;
    else if (last_perc < 0.90 && perc >= 0.90)
      std::cout << "p90:\t" << it->first << std::endl;
    else if (last_perc < 0.95 && perc >= 0.95)
      std::cout << "p95:\t" << it->first << std::endl;
    else if (last_perc < 0.99 && perc >= 0.99)
      std::cout << "p99:\t" << it->first << std::endl;
    else if (last_perc < 0.999 && perc >= 0.999)
      std::cout << "p999:\t" << it->first << std::endl;
    else if (last_perc < 0.9999 && perc >= 0.9999)
      std::cout << "p9999:\t" << it->first << std::endl;
    else if (last_perc < 0.99999 && perc >= 0.99999)
      std::cout << "p99999:\t" << it->first << std::endl;
    last_perc = perc;
    max = it->first;
  }
  std::cout << "min:\t" << min << std::endl;
  std::cout << "max:\t" << max << std::endl;
  double mean = (double)sum / all;
  std::cout << "mean:\t" << mean << std::endl;
  // uint64_t dev = 0.0;
  // for (const auto& pair : map) {
  //   int square = pow(pair.first - mean, 2);
  //   dev += pair.second * square;
  // }
  // std::cout << std::fixed << std::setprecision(2);
  // std::cout << "dev:\t" << sqrt(dev / all) << std::endl;
}

void DoWrites(crocks::Cluster* db, Generator* gen, int batch_size) {
  for (int i = 0; i < batch_size; i++)
    EnsureRpc(db->Put(gen->NextKey(), gen->NextValue()));
}

void DoBatchWrites(crocks::Cluster* db, Generator* gen, int batch_size) {
  crocks::WriteBatch batch(db);
  for (int i = 0; i < batch_size; i++)
    batch.Put(gen->NextKey(), gen->NextValue());
  EnsureRpc(batch.Write());
}

void DoReads(crocks::Cluster* db, Generator* gen, int batch_size) {
  std::string value;
  for (int i = 0; i < batch_size; i++)
    EnsureRpc(db->Get(gen->NextKey(), &value));
}

void Fill(crocks::Cluster* db, int num_keys, int value_size) {
  Duration duration(0, num_keys);
  Generator gen(SEQUENTIAL, 0, value_size);
  // 1MB per batch
  int batch_size = kMB / (kKeySize + value_size);
  while (!duration.Done(batch_size))
    DoBatchWrites(db, &gen, batch_size);
}

int main(int argc, char** argv) {
  std::string etcd_address = crocks::GetEtcdEndpoint();
  int db_size = 1;
  int value_size = 4000;
  int num_threads = 1;
  int batch_size = 128;
  int duration = 10;
  const char* optstring = "e:s:v:t:b:d:h";
  static struct option longopts[] = {
      // clang-format off
      {"etcd",     required_argument, 0, 'e'},
      {"size",     required_argument, 0, 's'},
      {"value",    required_argument, 0, 'v'},
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
        db_size = std::stoi(optarg);
        break;
      case 'v':
        value_size = std::stoi(optarg);
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
  int num_keys = db_size * kGB / (kKeySize + value_size);

  crocks::Cluster* db = crocks::DBOpen(etcd_address);

  if (command == "fill") {
    std::cout << "Filling db with " << db_size << "GB" << std::endl;
    double duration = Measure(Fill, db, num_keys, value_size);
    std::cout << "IOPS\tMB/sec" << std::endl;
    Report(num_keys / duration, value_size);

  } else if (command == "fillseq") {
    Generator gen(SEQUENTIAL, 0, value_size);
    double iops = 0;
    Run(DoWrites, db, &gen, duration, batch_size, &iops);
    Report(iops, value_size);

  } else if (command == "fillrandom" || command == "fillbatch") {
    std::cout << num_threads << "\t";
    std::vector<std::thread> threads;
    Generator gen(RANDOM, num_keys, value_size);
    double iops = 0;
    auto func = (command == "fillrandom") ? DoWrites : DoBatchWrites;
    for (int i = 0; i < num_threads; i++)
      threads.emplace_back(std::thread(
          [&] { Run(func, db, &gen, duration, batch_size, &iops); }));
    for (int i = 0; i < num_threads; i++)
      threads[i].join();
    Report(iops, value_size);

  } else if (command == "readseq") {
    Generator gen(SEQUENTIAL, 0, value_size);
    double iops = 0;
    Run(DoReads, db, &gen, duration, batch_size, &iops);
    Report(iops, value_size);

  } else if (command == "readrandom") {
    std::cout << num_threads << "\t";
    std::vector<std::thread> threads;
    Generator gen(RANDOM, num_keys, value_size);
    double iops = 0;
    for (int i = 0; i < num_threads; i++)
      threads.emplace_back(std::thread(
          [&] { Run(DoReads, db, &gen, duration, batch_size, &iops); }));
    for (int i = 0; i < num_threads; i++)
      threads[i].join();
    Report(iops, value_size);

  } else if (command == "readwhilewriting") {
    std::cout << num_threads << "\t";
    std::vector<std::thread> write_threads;
    std::vector<std::thread> read_threads;
    Generator gen(RANDOM, num_keys, value_size);
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
    Report(read_iops, value_size, false);
    std::cout << "\t";
    Report(write_iops, value_size);

  } else if (command == "latency") {
    Generator gen(RANDOM, num_keys, value_size);
    Latency(db, &gen, duration, batch_size);

  } else {
    std::cerr << usage_message;
    exit(EXIT_FAILURE);
  }

  delete db;

  return 0;
}
