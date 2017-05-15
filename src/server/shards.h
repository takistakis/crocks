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

#ifndef CROCKS_SERVER_SHARDS_H
#define CROCKS_SERVER_SHARDS_H

#include <atomic>
#include <mutex>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <rocksdb/status.h>
#include <rocksdb/utilities/write_batch_with_index.h>

#include "src/server/util.h"

namespace crocks {

class Shard {
 public:
  Shard(rocksdb::DB* db, rocksdb::ColumnFamilyHandle* cf, int shard)
      : db_(db), cf_(cf), importing_(false) {}

  Shard(rocksdb::DB* db, int shard, const std::string& old_address)
      : db_(db), importing_(true), old_address_(old_address) {
    rocksdb::Status s = db_->CreateColumnFamily(rocksdb::ColumnFamilyOptions(),
                                                std::to_string(shard), &cf_);
    EnsureRocksdb("CreateColumnFamily", s);
    newer_ = new rocksdb::WriteBatchWithIndex();
  }

  ~Shard() {
    db_->DropColumnFamily(cf_);
    delete cf_;
  }

  rocksdb::ColumnFamilyHandle* cf() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return cf_;
  }

  bool importing() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return importing_;
  }

  std::string old_address() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return old_address_;
  }

  rocksdb::Status Get(const std::string& key, std::string* value);
  rocksdb::Status GetFromBatch(const std::string& key, std::string* value);
  rocksdb::Status Put(const std::string& key, const std::string& value);
  rocksdb::Status Delete(const std::string& key);

  void Ingest(const std::vector<std::string>& files);

 private:
  mutable std::mutex mutex_;
  rocksdb::DB* db_;
  rocksdb::ColumnFamilyHandle* cf_;
  rocksdb::WriteBatchWithIndex* newer_;
  std::atomic<bool> importing_;
  std::string old_address_;
};

class Shards {
 public:
  Shards(rocksdb::DB* db, std::vector<int> shards);
  ~Shards();

  Shard* at(int id) {
    std::lock_guard<std::mutex> lock(mutex_);
    return shards_.at(id);
  }

  bool empty() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return shards_.empty();
  }

  bool has(int id) const {
    std::lock_guard<std::mutex> lock(mutex_);
    return shards_.find(id) != shards_.end();
  }

  Shard* Add(int id, const std::string& old_address) {
    std::lock_guard<std::mutex> lock(mutex_);
    Shard* shard = new Shard(db_, id, old_address);
    shards_[id] = shard;
    return shard;
  }

  void Remove(int id) {
    std::lock_guard<std::mutex> lock(mutex_);
    delete shards_[id];
    shards_.erase(id);
  }

  std::vector<rocksdb::ColumnFamilyHandle*> ColumnFamilies() {
    std::vector<rocksdb::ColumnFamilyHandle*> column_families;
    for (const auto& pair : shards_) {
      Shard* shard = pair.second;
      column_families.push_back(shard->cf());
    }
    return column_families;
  }

 private:
  mutable std::mutex mutex_;
  rocksdb::DB* db_;
  std::unordered_map<int, Shard*> shards_;
};

}  // namespace crocks

#endif  // CROCKS_SERVER_SHARDS_H
