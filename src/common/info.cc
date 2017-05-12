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

#include "src/common/info.h"

#include <assert.h>

#include <algorithm>
#include <iostream>
#include <sstream>
#include <vector>

namespace crocks {

// Helper function to print a list of shards in a compact way
template <class T>
std::string ListToString(const T& list) {
  // T must be an iterable that contains integers. Works with std::vector<int>
  // and RepeatedField<int32> as defined in google::protobuf. The returned
  // string is a comma separated list of ranges, represented as from-to
  // inclusive. For example ListToString([1,2,3,5,7,8,9]) returns "1-3,5,7-9".
  std::ostringstream stream;
  bool in_range = false;
  int last = list[0];
  for (int n : list) {
    if (n == list[0]) {
      // Start with the first integer
      stream << n;
    } else if (!in_range) {
      if (n == last + 1) {
        // Not in a range and the current integer is larger
        // than the last by one. Open a new range and continue.
        in_range = true;
      } else {
        stream << "," << n;
      }
    } else {
      if (n != last + 1) {
        // The range is broken. Finish it and append the current integer.
        stream << "-" << last << "," << n;
        in_range = false;
      }
    }
    last = n;
  }
  // Finish the last open range
  if (in_range)
    stream << "-" << last;
  return stream.str();
}

Info::Info(const std::string& address) : etcd_(address) {}

void Info::UpdateIndex() {
  std::lock_guard<std::mutex> lock(mutex_);
  int i = 0;
  for (const auto& node : info_.nodes()) {
    if (node.address() == address_) {
      if (id_ != i) {
        std::cout << id_ << ": Index changed to " << i << std::endl;
        id_ = i;
      }
      return;
    }
    i++;
  }
  // If address was not found assign -1 to indicate removed
  id_ = -1;
}

void Info::Get() {
  std::string info;
  etcd_.Get(kInfoKey, &info);
  Parse(info);
}

void Info::Add(const std::string& address) {
  bool succeeded;
  do {
    std::string old_info;
    if (etcd_.Get(kInfoKey, &old_info)) {
      Parse(old_info);
      if (info_.state() == pb::ClusterInfo::INIT) {
        AddWithNewShards(address);
      } else if (info_.state() == pb::ClusterInfo::RUNNING) {
        pb::NodeInfo* node = info_.add_nodes();
        node->set_address(address);
      } else if (info_.state() == pb::ClusterInfo::MIGRATING) {
        std::cout << "Migrating. Try again later." << std::endl;
        exit(EXIT_FAILURE);
      }
      succeeded = etcd_.TxnPutIfValueEquals(kInfoKey, Serialize(), old_info);
    } else {
      AddWithNewShards(address);
      succeeded = etcd_.TxnPutIfKeyMissing(kInfoKey, Serialize());
    }
  } while (!succeeded);
  // Set the index and address of the new node
  id_ = num_nodes() - 1;
  address_ = address;
}

void Info::Remove(const std::string& address) {
  bool succeeded;
  do {
    Get();
    assert(IsRunning());
    std::string old_info = Serialize();
    RemoveNode(address);
    succeeded = etcd_.TxnPutIfValueEquals(kInfoKey, Serialize(), old_info);
  } while (!succeeded);
}

void Info::Run() {
  if (IsRunning() || !NoMigrations())
    return;
  bool succeeded;
  do {
    std::string old_info;
    if (!etcd_.Get(kInfoKey, &old_info))
      return;
    Parse(old_info);
    info_.set_state(pb::ClusterInfo::RUNNING);
    succeeded = etcd_.TxnPutIfValueEquals(kInfoKey, Serialize(), old_info);
  } while (!succeeded);
}

void Info::Migrate() {
  bool succeeded;
  do {
    std::string old_info;
    if (!etcd_.Get(kInfoKey, &old_info))
      return;
    Parse(old_info);
    RedistributeShards();
    info_.set_state(pb::ClusterInfo::MIGRATING);
    succeeded = etcd_.TxnPutIfValueEquals(kInfoKey, Serialize(), old_info);
  } while (!succeeded);
}

void Info::Print() {
  switch (info_.state()) {
    case pb::ClusterInfo::INIT:
      std::cout << "state: INIT" << std::endl;
      break;
    case pb::ClusterInfo::RUNNING:
      std::cout << "state: RUNNING" << std::endl;
      break;
    case pb::ClusterInfo::MIGRATING:
      std::cout << "state: MIGRATING" << std::endl;
      break;
    case pb::ClusterInfo::SHUTDOWN:
      std::cout << "state: SHUTDOWN" << std::endl;
      break;
    default:
      assert(false);
  }
  std::cout << "nodes: " << num_nodes() << std::endl;
  std::cout << "shards: " << num_shards() << std::endl;
  int i = 0;
  for (const auto& node : info_.nodes()) {
    std::cout << "node " << i << ":" << std::endl;
    std::cout << "  address: " << node.address() << std::endl;
    if (node.shards_size() > 0)
      std::cout << "  shards: " << ListToString(node.shards()) << std::endl;
    else
      std::cout << "  shards: none" << std::endl;
    if (node.future_size() > 0)
      std::cout << "  future: " << ListToString(node.future()) << std::endl;
    if (node.remove())
      std::cout << "  remove: true" << std::endl;

    i++;
  }
}

void Info::RedistributeShards() {
  std::cout << "Redistributing shards" << std::endl;

  std::vector<bool> skip;
  for (int i = 0; i < num_nodes(); i++) {
    auto node = info_.nodes(i);
    skip.push_back(node.remove() ? true : false);
  }

  std::vector<int> targets = Distribute(num_shards(), num_nodes(), skip);

  // Calculate diffs
  // Positive diff: the node has diff shards to give
  // Negative diff: the node has -diff shards to get
  std::vector<int> diffs;
  for (int i = 0; i < num_nodes(); i++) {
    auto node = info_.nodes(i);
    diffs.push_back(node.shards_size() + node.future_size() - targets[i]);
  }

  for (int i = 0; i < num_nodes() - 1; i++) {
    pb::NodeInfo* left = info_.mutable_nodes(i);
    for (int j = i + 1; j < num_nodes(); j++) {
      pb::NodeInfo* right = info_.mutable_nodes(j);
      while (diffs[i] > 0 && diffs[j] < 0) {
        int shard = left->shards(diffs[i] - 1);
        diffs[i]--;
        right->add_future(shard);
        diffs[j]++;
      }
      while (diffs[j] > 0 && diffs[i] < 0) {
        int shard = right->shards(diffs[j] - 1);
        diffs[j]--;
        left->add_future(shard);
        diffs[i]++;
      }
    }
  }

  for (int i = 0; i < num_nodes(); i++) {
    auto node = info_.mutable_nodes(i);
    std::sort(node->mutable_future()->begin(), node->mutable_future()->end());
  }

  for (int i = 0; i < num_nodes(); i++) {
    auto node = info_.nodes(i);
    if (node.future_size() > 0)
      assert(node.shards_size() + node.future_size() == targets[i]);
  }
}

// private
std::string Info::Serialize() const {
  std::string str;
  bool ok = info_.SerializeToString(&str);
  assert(ok);
  return str;
}

void Info::Parse(const std::string& str) {
  bool ok = info_.ParseFromString(str);
  assert(ok);
  UpdateMap();
}

void Info::AddWithNewShards(const std::string& address) {
  pb::NodeInfo* node = info_.add_nodes();
  node->set_address(address);
  for (int i = num_shards(); i < num_shards() + kShardsPerNode; i++)
    node->add_shards(i);
  info_.set_num_shards(num_shards() + kShardsPerNode);
  UpdateMap();
}

void Info::RemoveNode(const std::string& address) {
  // Remove the node from the list
  pb::NodeInfo* node;
  bool found = false;
  for (int i = 0; i < num_nodes(); i++) {
    if (info_.nodes(i).address() == address) {
      node = info_.mutable_nodes(i);
      found = true;
      break;
    }
  }
  assert(found);
  node->set_remove(true);
}

void Info::Remove() {
  bool succeeded;
  do {
    Get();
    std::string old_info = Serialize();
    info_.mutable_nodes()->SwapElements(id_, num_nodes() - 1);
    info_.mutable_nodes()->RemoveLast();
    succeeded = etcd_.TxnPutIfValueEquals(kInfoKey, Serialize(), old_info);
  } while (!succeeded);
}

std::vector<int> Info::Distribute(int s, int n, const std::vector<bool>& skip) {
  // Romoved nodes get 0 shards. For the rest, each pair must have the same
  // number of shards, or a difference of one shard. Let s be the number of
  // shards and n the number of nodes. Each node will be assigned either n/s
  // or n/s+1 shards and all shards will have to add up to s. We achieve
  // that by giving the first n%s nodes n/s+1 shards and the rest just n/s.
  int N = n;
  for (bool b : skip)
    if (b)
      n--;
  std::vector<int> vec;
  int j = 0;
  for (int i = 0; i < N; i++) {
    if (skip[i]) {
      vec.push_back(0);
      continue;
    }
    if (j < s % n)
      vec.push_back(s / n + 1);
    else
      vec.push_back(s / n);
    j++;
  }
  return vec;
}

void Info::UpdateMap() {
  int i = 0;
  for (const auto& node : info_.nodes()) {
    for (int shard : node.shards())
      map_[shard] = i;
    i++;
  }
}

void Info::UpdateTasks() {
  tasks_.clear();
  for (int shard : info_.nodes(id_).future()) {
    int current_master = map_[shard];
    tasks_[current_master].push_back(shard);
  }
}

void Info::GiveShard(int shard) {
  bool succeeded;
  do {
    Get();
    std::string old_info = Serialize();
    DoGiveShard(shard);
    succeeded = etcd_.TxnPutIfValueEquals(kInfoKey, Serialize(), old_info);
  } while (!succeeded);
}

void Info::DoGiveShard(int shard) {
  // Remove shard from my shards
  auto node = info_.mutable_nodes(id_);
  for (int i = 0; i < node->shards_size(); i++) {
    if (node->shards(i) == shard) {
      node->mutable_shards()->SwapElements(i, node->shards_size() - 1);
      node->mutable_shards()->RemoveLast();
      std::sort(node->mutable_shards()->begin(), node->mutable_shards()->end());
      break;
    }
  }

  // Put shard into new master's future shards
  for (int i = 0; i < info_.nodes_size(); i++) {
    auto node = info_.mutable_nodes(i);
    for (int j = 0; j < node->future_size(); j++) {
      if (node->future(j) == shard) {
        node->mutable_future()->SwapElements(j, node->future_size() - 1);
        node->mutable_future()->RemoveLast();
        node->add_shards(shard);
        std::sort(node->mutable_shards()->begin(),
                  node->mutable_shards()->end());
        return;
      }
    }
  }
}

bool Info::NoMigrations() {
  for (const auto& node : info_.nodes())
    if (node.future_size() > 0)
      return false;
  return true;
}

void* Info::Watch() {
  std::string info;
  void* call = etcd_.Watch(kInfoKey, &info);
  Parse(info);
  return call;
}

bool Info::WatchNext(void* call) {
  std::string info;
  bool canceled = etcd_.WatchNext(call, &info);
  if (!canceled) {
    std::lock_guard<std::mutex> lock(mutex_);
    Parse(info);
  }
  return canceled;
}

void Info::WatchCancel(void* call) {
  etcd_.WatchCancel(call);
}

}  // namespace crocks
