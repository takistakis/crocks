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

// Responsible for exchanging cluster info with etcd

#ifndef CROCKS_COMMON_INFO_H
#define CROCKS_COMMON_INFO_H

#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "src/common/etcd_client.h"
#include "src/common/hash.h"
#include "src/common/info_wrapper.h"

const std::string kInfoKey = "info";

namespace crocks {

class Info {
 public:
  Info(const std::string& address);

  int num_shards() const {
    return info_.num_shards();
  }

  int num_nodes() const {
    return info_.num_nodes();
  }

  int id() const {
    return id_;
  }

  int IndexForShard(int id) {
    std::lock_guard<std::mutex> lock(mutex_);
    return map_[id];
  }

  int ShardForKey(const std::string& key) {
    return Hash(key) % info_.num_shards();
  }

  int IndexForKey(const std::string& key) {
    std::lock_guard<std::mutex> lock(mutex_);
    return map_[Hash(key) % info_.num_shards()];
  }

  // Return true if the given shard is intended for a different node
  bool WrongShard(int shard) {
    std::lock_guard<std::mutex> lock(mutex_);
    return map_[shard] != id_;
  }

  std::vector<std::string> Addresses() const {
    return info_.Addresses();
  };

  std::string Address(int id) const {
    return info_.Address(id);
  };

  std::vector<int> shards() const {
    return info_.shards(id_);
  };

  void Get();

  // Add a node with the given address and send the updated cluster
  // info to etcd, repeating the transaction until succeeded.
  void Add(const std::string& address);

  void Remove(int id);

  void Remove();

  // Change cluster state to RUNNING
  void Run();

  // Change cluster state to MIGRATING
  void Migrate();

  bool IsInit() const {
    return info_.IsInit();
  }

  bool IsRunning() const {
    return info_.IsRunning();
  }

  bool IsMigrating() const {
    return info_.IsMigrating();
  }

  bool IsMigrating(int shard) const {
    return info_.IsMigrating(shard);
  }

  bool NoMigrations() const {
    return info_.NoMigrations();
  }

  void* Watch();
  bool WatchNext(void* call);
  void WatchCancel(void* call);

  std::unordered_map<int, std::vector<int>> Tasks() const;

  void GiveShard(int shard);

  void MigrationOver(int shard);

  bool IsAvailable(int id) const;

  void SetAvailable(int id, bool available);

  void Print();

  void Lock() {
    etcd_.Lock();
  }

  void Unlock() {
    etcd_.Unlock();
  }

 private:
  void Parse(const std::string& str) {
    std::lock_guard<std::mutex> lock(mutex_);
    info_.Parse(str);
    map_ = info_.map();
  }

  mutable std::mutex mutex_;
  EtcdClient etcd_;
  InfoWrapper info_;
  std::vector<int> map_;
  std::string address_;
  int id_ = -1;
};

}  // namespace crocks

#endif  // CROCKS_COMMON_INFO_H
