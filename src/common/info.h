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

#include <assert.h>

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
  Info(const std::string& address) : etcd_(address) {}

  int num_shards() const {
    return info_.num_shards();
  }

  int num_nodes() const {
    return info_.nodes_size();
  }

  int id() const {
    // XXX: This is set once at Add() which happens
    // before Watch(), so there's no need for a lock.
    return id_;
  }

  int IndexForShard(int id) {
    return map_[id];
  }

  int ShardForKey(const std::string& key) {
    return Hash(key) % num_shards();
  }

  int IndexForKey(const std::string& key) {
    return IndexForShard(ShardForKey(key));
  }

  std::vector<std::string> Addresses() const {
    std::vector<std::string> addresses;
    for (const auto& node : info_.nodes())
      addresses.push_back(node.address());
    return addresses;
  };

  std::string Serialize() const {
    std::string str;
    assert(info_.SerializeToString(&str));
    return str;
  }

  void Parse(const std::string& str) {
    assert(info_.ParseFromString(str));
    UpdateMap();
  }

  void Get();

  // Add a node with the given address and send the updated cluster
  // info to etcd, repeating the transaction until succeeded.
  void Add(const std::string& address);

  // Remove the node with the given address and redistribute existing shards
  void Remove(const std::string& address);

  // TODO: Watch
  void Watch(const std::string& key);

  void Print();

 private:
  // Add a node with the given address and assign kShardsPerNode new shards
  void AddWithNewShards(const std::string& address);

  // Add a node with the given address and redistribute existing shards
  void AddRedistributingShards(const std::string& address);

  std::vector<int> Distribute(int s, int n);

  void UpdateMap();

  EtcdClient etcd_;
  pb::ClusterInfo info_;
  std::unordered_map<int, int> map_;
  std::string address_;
  int id_ = -1;
};

}  // namespace crocks

#endif  // CROCKS_COMMON_INFO_H
