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

#include <string>
#include <unordered_map>
#include <vector>

#include "gen/info.pb.h"
#include "src/common/lock.h"

namespace crocks {

class InfoWrapper {
 public:
  int num_shards() const {
    read_lock lock(mutex_);
    return info_.shards_size();
  }

  int num_nodes() const {
    read_lock lock(mutex_);
    return info_.nodes_size();
  }

  std::vector<int> shards(int id) const {
    read_lock lock(mutex_);
    std::vector<int> shards;
    assert(id >= 0);
    int i = 0;
    for (const auto& shard : info_.shards()) {
      if (shard.master() == id)
        shards.push_back(i);
      i++;
    }
    return shards;
  };

  std::vector<int> future(int id) const {
    read_lock lock(mutex_);
    std::vector<int> future;
    assert(id >= 0);
    int i = 0;
    for (const auto& shard : info_.shards()) {
      if (shard.migrating() && shard.to() == id)
        future.push_back(i);
      i++;
    }
    return future;
  };

  std::vector<int> map() const {
    read_lock lock(mutex_);
    std::vector<int> map;
    for (const auto& shard : info_.shards())
      map.push_back(shard.master());
    return map;
  }

  std::string Serialize() const {
    read_lock lock(mutex_);
    std::string str;
    bool ok = info_.SerializeToString(&str);
    assert(ok);
    return str;
  }

  void Parse(const std::string& str) {
    write_lock lock(mutex_);
    bool ok = info_.ParseFromString(str);
    assert(ok);
  }

  std::vector<std::string> Addresses() const {
    read_lock lock(mutex_);
    std::vector<std::string> addresses;
    for (const auto& node : info_.nodes())
      addresses.push_back(node.address());
    return addresses;
  };

  std::string Address(int id) const {
    read_lock lock(mutex_);
    return info_.nodes(id).address();
  };

  int IndexOf(const std::string& address) const {
    read_lock lock(mutex_);
    for (const auto& node : info_.nodes())
      if (node.address() == address)
        return node.id();
    return -1;
  }

  bool IsRemoved(int id) const {
    read_lock lock(mutex_);
    return info_.nodes(id).remove();
  }

  bool IsInit() const {
    read_lock lock(mutex_);
    return info_.state() == pb::ClusterInfo::INIT;
  }

  bool IsRunning() const {
    read_lock lock(mutex_);
    return info_.state() == pb::ClusterInfo::RUNNING;
  }

  bool IsMigrating() const {
    read_lock lock(mutex_);
    return info_.state() == pb::ClusterInfo::MIGRATING;
  }

  bool IsMigrating(int shard) const {
    read_lock lock(mutex_);
    return info_.shards(shard).migrating();
  }

  bool NoMigrations() const {
    read_lock lock(mutex_);
    for (const auto& shard : info_.shards())
      if (shard.migrating())
        return false;
    return true;
  }

  bool IsAvailable(int id) const {
    read_lock lock(mutex_);
    return info_.nodes(id).available();
  }

  bool IsHealthy() const;

  void SetRunning() {
    write_lock lock(mutex_);
    info_.set_state(pb::ClusterInfo::RUNNING);
  }

  void SetMigrating() {
    write_lock lock(mutex_);
    info_.set_state(pb::ClusterInfo::MIGRATING);
  }

  int AddNode(const std::string& address);

  int AddNodeWithNewShards(const std::string& address, int num_shards);

  void MarkRemoveNode(int id);

  void RemoveNode(int id);

  void RedistributeShards();

  std::pair<int, int> Task(int id) const;

  void GiveShard(int id, int shard);

  void MigrationOver(int shard_id);

  void SetAvailable(int id, bool available);

 private:
  pb::ClusterInfo info_;
  mutable shared_mutex mutex_;
};

}  // namespace crocks

#endif  // CROCKS_COMMON_INFO_WRAPPER_H
