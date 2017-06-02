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

#include <future>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include <rocksdb/status.h>

namespace rocksdb {
class DB;
class ColumnFamilyHandle;
class WriteBatch;
}

namespace crocks {

class Shard {
 public:
  Shard(rocksdb::DB* db, rocksdb::ColumnFamilyHandle* cf, int shard);
  Shard(rocksdb::DB* db, int shard, const std::string& old_address);
  ~Shard();

  rocksdb::ColumnFamilyHandle* cf() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return cf_;
  }

  bool importing() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return importing_;
  }

  void set_removing(bool value) {
    std::lock_guard<std::mutex> lock(mutex_);
    removing_ = value;
  }

  std::string old_address() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return old_address_;
  }

  // Get the key from the appropriate place (db, backup or both), and put it
  // into *value. If it was not found and there is a possibility that the
  // former master of the shard has the most recent value, *ask is true.
  rocksdb::Status Get(const std::string& key, std::string* value, bool* ask);
  rocksdb::Status Put(const std::string& key, const std::string& value);
  rocksdb::Status Delete(const std::string& key);

  void Ingest(const std::vector<std::string>& files);

  // Increase the reference counter of the shard. Fails and returns
  // false if the shard is marked for removal. A referenced
  // shard is not allowed to be snapshotted and sent to a node.
  // Should only be used for requests to modify the shard.
  bool Ref();

  // Decrease the reference counter of the shard
  void Unref();

  // Wait for the reference counter to reach 0
  void WaitRefs();

 private:
  mutable std::mutex mutex_;
  rocksdb::DB* db_;
  rocksdb::ColumnFamilyHandle* cf_;
  rocksdb::ColumnFamilyHandle* backup_;
  rocksdb::WriteBatch* newer_;
  // TODO: Use std::atomic<bool> without the lock
  bool importing_;
  bool removing_{false};
  int refs_{1};
  std::promise<void> zero_refs_;
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

  Shard* Add(int id, const std::string& old_address);

  void Remove(int id);

  std::vector<rocksdb::ColumnFamilyHandle*> ColumnFamilies() const;

  // Increase the reference counter of the given shard. Return
  // a pointer to the shard on success and nullptr if the shard
  // does not belong to this node, or is about to be removed.
  Shard* Ref(int id);

 private:
  mutable std::mutex mutex_;
  rocksdb::DB* db_;
  std::unordered_map<int, Shard*> shards_;
};

}  // namespace crocks

#endif  // CROCKS_SERVER_SHARDS_H
