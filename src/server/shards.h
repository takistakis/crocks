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
#include <future>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include <rocksdb/status.h>

#include "src/common/lock.h"

namespace rocksdb {
class DB;
class ColumnFamilyHandle;
}  // namespace rocksdb

namespace crocks {

class Shard {
 public:
  Shard(rocksdb::DB* db, rocksdb::ColumnFamilyHandle* cf, int shard);
  Shard(rocksdb::DB* db, int shard, const std::string& old_address);
  ~Shard();

  rocksdb::ColumnFamilyHandle* cf() const {
    return cf_;
  }

  bool importing() const {
    return importing_.load();
  }

  void set_importing(bool value) {
    importing_.store(value);
  }

  std::string old_address() const {
    return old_address_;
  }

  // Get the key from the database and put it into *value. If it was
  // not found and there is a possibility that the former master
  // of the shard has the most recent value, *ask is set to true.
  rocksdb::Status Get(const std::string& key, std::string* value, bool* ask);
  rocksdb::Status Put(const std::string& key, const std::string& value);
  rocksdb::Status Delete(const std::string& key);

  void Ingest(const std::string& filename, const std::string& largest_key);

  // Increase the reference counter of the shard. Fails and returns
  // false if the shard is marked for removal. A referenced
  // shard is not allowed to be snapshotted and sent to a node.
  // Should only be used for requests to modify the shard.
  bool Ref();

  // Decrease the reference counter of the shard. Once called with migrating
  // set to true, it's not possible to increase the counter again.
  bool Unref(bool migrating = false);

  // Wait for the reference counter to reach 0
  void WaitRefs();

 private:
  rocksdb::DB* db_;
  rocksdb::ColumnFamilyHandle* cf_;
  std::atomic<bool> importing_;
  // If migrating_ is true can't get reference (can't put etc)
  bool migrating_;
  int refs_;
  std::promise<void> zero_refs_;
  std::mutex ref_mutex_;
  std::string old_address_;
  std::string largest_key_;
  mutable shared_mutex largest_key_mutex_;
};

class Shards {
 public:
  Shards(rocksdb::DB* db, const std::vector<int>& shards);
  Shards(rocksdb::DB* db,
         const std::vector<rocksdb::ColumnFamilyHandle*>& handles);

  std::shared_ptr<Shard> at(int id) {
    read_lock lock(mutex_);
    if (shards_.find(id) == shards_.end())
      return std::shared_ptr<Shard>();
    return shards_[id];
  }

  bool empty() const {
    read_lock lock(mutex_);
    return shards_.empty();
  }

  std::shared_ptr<Shard> Add(int id, const std::string& old_address);

  void Remove(int id);

  std::vector<rocksdb::ColumnFamilyHandle*> ColumnFamilies() const;

 private:
  mutable shared_mutex mutex_;
  rocksdb::DB* db_;
  std::unordered_map<int, std::shared_ptr<Shard>> shards_;
};

}  // namespace crocks

#endif  // CROCKS_SERVER_SHARDS_H
