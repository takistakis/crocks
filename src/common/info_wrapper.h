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

// Thread-safe wrapper around pb::ClusterInfo
// with some useful added functionality.

#ifndef CROCKS_COMMON_INFO_WRAPPER_H
#define CROCKS_COMMON_INFO_WRAPPER_H

#include <assert.h>

#include <mutex>
#include <string>
#include <vector>

#include "gen/info.pb.h"

const int kShardsPerNode = 10;

namespace crocks {

class InfoWrapper {
 public:
  int num_shards() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return info_.num_shards();
  }

  int num_nodes() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return info_.nodes_size();
  }

  std::vector<int> shards(int id) const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<int> shards;
    assert(id >= 0 && id < info_.nodes_size());
    for (int shard : info_.nodes(id).shards())
      shards.push_back(shard);
    return shards;
  };

  std::vector<int> future(int id) const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<int> future;
    assert(id >= 0 && id < info_.nodes_size());
    for (int shard : info_.nodes(id).future())
      future.push_back(shard);
    return future;
  };

  std::string Serialize() const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::string str;
    bool ok = info_.SerializeToString(&str);
    assert(ok);
    return str;
  }

  void Parse(const std::string& str) {
    std::lock_guard<std::mutex> lock(mutex_);
    bool ok = info_.ParseFromString(str);
    assert(ok);
  }

  std::vector<std::string> Addresses() const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<std::string> addresses;
    for (const auto& node : info_.nodes())
      addresses.push_back(node.address());
    return addresses;
  };

  std::string Address(int id) const {
    std::lock_guard<std::mutex> lock(mutex_);
    return info_.nodes(id).address();
  };

  int IndexOf(const std::string& address) const {
    std::lock_guard<std::mutex> lock(mutex_);
    int i = 0;
    for (const auto& node : info_.nodes()) {
      if (node.address() == address)
        return i;
      i++;
    }
    return -1;
  }

  bool IsRemoved(int id) const {
    std::lock_guard<std::mutex> lock(mutex_);
    return info_.nodes(id).remove();
  }

  bool IsInit() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return info_.state() == pb::ClusterInfo::INIT;
  }

  bool IsRunning() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return info_.state() == pb::ClusterInfo::RUNNING;
  }

  bool IsMigrating() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return info_.state() == pb::ClusterInfo::MIGRATING;
  }

  bool NoMigrations() const {
    std::lock_guard<std::mutex> lock(mutex_);
    for (const auto& node : info_.nodes())
      if (node.future_size() > 0)
        return false;
    return true;
  }

  void SetRunning() {
    std::lock_guard<std::mutex> lock(mutex_);
    info_.set_state(pb::ClusterInfo::RUNNING);
  }

  void SetMigrating() {
    std::lock_guard<std::mutex> lock(mutex_);
    info_.set_state(pb::ClusterInfo::MIGRATING);
  }

  void AddNode(const std::string& address);

  void AddNodeWithNewShards(const std::string& address);

  void MarkRemoveNode(const std::string& address);

  void RemoveNode(int id);

  void RedistributeShards();

  void GiveShard(int id, int shard);

  void RemoveFuture(int id, int shard);

 private:
  pb::ClusterInfo info_;
  mutable std::mutex mutex_;
};

}  // namespace crocks

#endif  // CROCKS_COMMON_INFO_WRAPPER_H
