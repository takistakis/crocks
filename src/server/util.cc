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

#include "src/server/util.h"

#include <assert.h>
#include <stdlib.h>
#include <unistd.h>

#include <algorithm>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <rocksdb/advanced_options.h>
#include <rocksdb/compaction_job_stats.h>
#include <rocksdb/filter_policy.h>
#include <rocksdb/listener.h>
#include <rocksdb/slice.h>
#include <rocksdb/table.h>
#include <rocksdb/table_properties.h>
#include <rocksdb/write_batch.h>

#include <crocks/status.h>
#include "gen/crocks.pb.h"
#include "src/server/iterator.h"

namespace rocksdb {
class DB;
}

namespace crocks {

int RocksdbStatusCodeToInt(const rocksdb::Status::Code& code) {
  int value = static_cast<int>(code);
  assert(value >= 0 && value <= 13);
  return value;
}

void EnsureRocksdb(const std::string& what, const rocksdb::Status& status) {
  if (!status.ok()) {
    std::cerr << "RocksDB " << what << " failed with status " << status.code()
              << " (" << status.getState() << ")" << std::endl;
    exit(EXIT_FAILURE);
  }
}

void ApplyBatchUpdate(rocksdb::WriteBatch* batch,
                      rocksdb::ColumnFamilyHandle* cf,
                      const pb::BatchUpdate& batch_update) {
  switch (batch_update.op()) {
    case pb::BatchUpdate::PUT:
      batch->Put(cf, batch_update.key(), batch_update.value());
      break;
    case pb::BatchUpdate::DELETE:
      batch->Delete(cf, batch_update.key());
      break;
    case pb::BatchUpdate::SINGLE_DELETE:
      batch->SingleDelete(cf, batch_update.key());
      break;
    case pb::BatchUpdate::MERGE:
      batch->Merge(cf, batch_update.key(), batch_update.value());
      break;
    case pb::BatchUpdate::CLEAR:
      batch->Clear();
      break;
    default:
      assert(false);
  }
}

const int kIteratorBatchSize = 10;

void MakeNextBatch(MultiIterator* it, pb::IteratorResponse* response) {
  for (int i = 0; i < kIteratorBatchSize && it->Valid(); i++) {
    pb::KeyValue* kv = response->add_kvs();
    kv->set_key(it->key().ToString());
    kv->set_value(it->value().ToString());
    it->Next();
  }
  response->set_done(it->Valid() ? false : true);
  response->set_status(it->status().rocksdb_code());
}

void MakePrevBatch(MultiIterator* it, pb::IteratorResponse* response) {
  for (int i = 0; i < kIteratorBatchSize && it->Valid(); i++) {
    pb::KeyValue* kv = response->add_kvs();
    kv->set_key(it->key().ToString());
    kv->set_value(it->value().ToString());
    it->Prev();
  }
  response->set_done(it->Valid() ? false : true);
  response->set_status(it->status().rocksdb_code());
}

// Once we have a seek request, guess which way the client will
// iterate and send a bunch of key-value pairs and whatever
// else is needed. When the iterator becomes invalid, iteration
// stops and the done field of the response is set to true.
void ApplyIteratorRequest(MultiIterator* it, const pb::IteratorRequest& request,
                          pb::IteratorResponse* response) {
  switch (request.op()) {
    case pb::IteratorRequest::SEEK_TO_FIRST:
      it->SeekToFirst();
      MakeNextBatch(it, response);
      break;
    case pb::IteratorRequest::SEEK_TO_LAST:
      it->SeekToLast();
      MakePrevBatch(it, response);
      break;
    case pb::IteratorRequest::SEEK:
      it->Seek(request.target());
      MakeNextBatch(it, response);
      break;
    case pb::IteratorRequest::SEEK_FOR_PREV:
      it->SeekForPrev(request.target());
      MakePrevBatch(it, response);
      break;
    case pb::IteratorRequest::NEXT:
      MakeNextBatch(it, response);
      break;
    case pb::IteratorRequest::PREV:
      MakePrevBatch(it, response);
      break;
    default:
      assert(false);
  }
}

class Listener : public rocksdb::EventListener {
 public:
  explicit Listener() {}

  void OnFlushCompleted(rocksdb::DB* db,
                        const rocksdb::FlushJobInfo& info) override {
    if (info.cf_name == "default")
      return;
    std::cout << "Flush from shard " << info.cf_name << std::endl;
    raw += info.table_properties.raw_key_size +
           info.table_properties.raw_value_size;
    // Multiply by 2 because they were also written to the WAL
    written += 2 * info.table_properties.data_size;
  }

  void OnCompactionCompleted(rocksdb::DB* db,
                             const rocksdb::CompactionJobInfo& info) override {
    if (info.cf_name == "default")
      return;
    std::cout << "Compaction in shard " << info.cf_name << ": "
              << info.input_files.size() << " from L" << info.base_input_level
              << " -> " << info.output_files.size() << " in L"
              << info.output_level << std::endl;
    written += info.stats.total_output_bytes;
    std::cout << "Write amplification: " << (double)written / raw << std::endl;
  }

 private:
  uint64_t raw = 0;
  uint64_t written = 0;
};

uint64_t GetTotalSystemMemory() {
  long pages = sysconf(_SC_PHYS_PAGES);
  long page_size = sysconf(_SC_PAGE_SIZE);
  return pages * page_size;
}

rocksdb::ColumnFamilyOptions DefaultColumnFamilyOptions() {
  rocksdb::ColumnFamilyOptions cf_options;
  cf_options.OptimizeLevelStyleCompaction();
  cf_options.level_compaction_dynamic_level_bytes = true;
  cf_options.compression = rocksdb::kLZ4Compression;
  cf_options.bottommost_compression = rocksdb::kZSTD;
  return cf_options;
}

rocksdb::Options DefaultRocksdbOptions() {
  // https://github.com/facebook/rocksdb/wiki/Set-Up-Options
  rocksdb::BlockBasedTableOptions table_options;
  table_options.filter_policy.reset(rocksdb::NewBloomFilterPolicy(10, false));
  table_options.block_size = 16 * 1024;
  table_options.cache_index_and_filter_blocks = true;
  table_options.pin_l0_filter_and_index_blocks_in_cache = true;
  rocksdb::Options options;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  options.create_if_missing = true;
  options.IncreaseParallelism(
      std::max(std::thread::hardware_concurrency(), 2U));
  options.OptimizeLevelStyleCompaction();
  options.compaction_pri = rocksdb::kMinOverlappingRatio;
  // options.max_total_wal_size = 100 << 20;
  options.write_buffer_size = 64 << 20;
  // options.db_write_buffer_size = GetTotalSystemMemory() / 4;
  options.allow_ingest_behind = true;
  options.level0_slowdown_writes_trigger = 10;
  options.level0_stop_writes_trigger = 15;
  options.listeners.push_back(std::make_shared<Listener>());
  options.optimize_filters_for_hits = true;
  options.wal_bytes_per_sync = 512 << 10;
  options.bytes_per_sync = 512 << 10;
  return options;
}

}  // namespace crocks
