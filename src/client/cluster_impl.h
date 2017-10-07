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

#ifndef CROCKS_CLIENT_CLUSTER_IMPL_H
#define CROCKS_CLIENT_CLUSTER_IMPL_H

#include <functional>
#include <string>
#include <unordered_map>

#include <crocks/options.h>
#include <crocks/status.h>
#include "src/common/info.h"

namespace crocks {

class Node;

class ClusterImpl {
 public:
  ClusterImpl(const Options&, const std::string& address);
  ~ClusterImpl();

  Status Get(const std::string& key, std::string* value);
  Status Put(const std::string& key, const std::string& value);
  Status Delete(const std::string& key);
  Status SingleDelete(const std::string& key);
  Status Merge(const std::string& key, const std::string& value);

  void WaitUntilHealthy();

  int IndexForShard(int shard, bool update = false);
  int ShardForKey(const std::string& key);
  int IndexForKey(const std::string& key);
  Node* NodeForKey(const std::string& key);
  Node* NodeByIndex(int idx);

  int num_nodes() const {
    return info_.num_nodes();
  }

  int num_shards() const {
    return info_.num_shards();
  }

  std::unordered_map<int, Node*> nodes() const {
    return nodes_;
  }

  void Lock() {
    info_.Lock();
  }

  void Unlock() {
    info_.Unlock();
  }

 private:
  Status Operation(const std::function<Status(Node*)>&, const std::string& key);
  void Update();

  const Options options_;
  Info info_;
  std::unordered_map<int, Node*> nodes_;
};

}  // namespace crocks

#endif  // CROCKS_CLIENT_CLUSTER_IMPL_H
