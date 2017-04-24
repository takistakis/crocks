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

#include "gen/info.pb.h"
#include "src/common/etcd_client.h"
#include "src/common/hash.h"

const int kShardsPerNode = 20;
const std::string kInfoKey = "info";

namespace crocks {

class Info {
 public:
  Info(const std::string& address);

  int num_shards() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return info_.num_shards();
  }

  int num_nodes() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return info_.nodes_size();
  }

  int id() const {
    // XXX: This is set once at Add() which happens
    // before Watch(), so there's no need for a lock.
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

  // Return true if the given key is intended for a different node
  bool WrongShard(const std::string& key) {
    return IndexForKey(key) != id_;
  }

  std::vector<std::string> Addresses() const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<std::string> addresses;
    for (const auto& node : info_.nodes())
      addresses.push_back(node.address());
    return addresses;
  };

  void Get();

  // Add a node with the given address and send the updated cluster
  // info to etcd, repeating the transaction until succeeded.
  void Add(const std::string& address);

  // Remove the node with the given address and redistribute existing shards
  void Remove(const std::string& address);

  void Print();

  void* Watch();
  bool WatchNext(void* call);
  void WatchCancel(void* call);

 private:
  std::string Serialize() const;
  void Parse(const std::string& str);

  // Add a node with the given address and assign kShardsPerNode new shards
  void AddWithNewShards(const std::string& address);

  // Add a node with the given address and redistribute existing shards
  void AddRedistributingShards(const std::string& address);

  std::vector<int> Distribute(int s, int n);

  void UpdateMap();

  mutable std::mutex mutex_;
  EtcdClient etcd_;
  pb::ClusterInfo info_;
  std::unordered_map<int, int> map_;
  std::string address_;
  int id_ = -1;
};

}  // namespace crocks

#endif  // CROCKS_COMMON_INFO_H
