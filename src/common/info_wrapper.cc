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

#include "src/common/info_wrapper.h"

#include <algorithm>

namespace crocks {

std::vector<int> Distribution(int s, int n, const std::vector<bool>& skip) {
  // Romoved nodes get 0 shards. For the rest, each pair must have the same
  // number of shards, or a difference of one shard. Let s be the number of
  // shards and n the number of nodes. Each node will be assigned either s/n
  // or s/n+1 shards and all shards will have to add up to s. We achieve
  // that by giving the first s%n nodes s/n+1 shards and the rest just s/n.
  int N = n;
  for (bool b : skip)
    if (b)
      n--;
  std::vector<int> targets;
  int j = 0;
  for (int i = 0; i < N; i++) {
    if (skip[i]) {
      targets.push_back(0);
      continue;
    }
    if (j < s % n)
      targets.push_back(s / n + 1);
    else
      targets.push_back(s / n);
    j++;
  }
  return targets;
}

void InfoWrapper::AddNode(const std::string& address) {
  std::lock_guard<std::mutex> lock(mutex_);
  pb::NodeInfo* node = info_.add_nodes();
  node->set_address(address);
}

void InfoWrapper::AddNodeWithNewShards(const std::string& address) {
  std::lock_guard<std::mutex> lock(mutex_);
  pb::NodeInfo* node = info_.add_nodes();
  node->set_address(address);
  for (int i = info_.num_shards(); i < info_.num_shards() + kShardsPerNode; i++)
    node->add_shards(i);
  info_.set_num_shards(info_.num_shards() + kShardsPerNode);
}

void InfoWrapper::MarkRemoveNode(const std::string& address) {
  std::lock_guard<std::mutex> lock(mutex_);
  pb::NodeInfo* node;
  for (int i = 0; i < info_.nodes_size(); i++) {
    if (info_.nodes(i).address() == address) {
      node = info_.mutable_nodes(i);
      node->set_remove(true);
      return;
    }
  }
  assert(false);
}

void InfoWrapper::RemoveNode(int id) {
  std::lock_guard<std::mutex> lock(mutex_);
  info_.mutable_nodes()->SwapElements(id, info_.nodes_size() - 1);
  info_.mutable_nodes()->RemoveLast();
}

void InfoWrapper::RedistributeShards() {
  std::lock_guard<std::mutex> lock(mutex_);
  std::vector<bool> skip;
  int num_nodes = info_.nodes_size();
  int num_shards = info_.num_shards();
  for (int i = 0; i < num_nodes; i++) {
    auto node = info_.nodes(i);
    skip.push_back(node.remove() ? true : false);
  }

  std::vector<int> targets = Distribution(num_shards, num_nodes, skip);

  // Calculate diffs
  // Positive diff: the node has diff shards to give
  // Negative diff: the node has -diff shards to get
  std::vector<int> diffs;
  for (int i = 0; i < num_nodes; i++) {
    auto node = info_.nodes(i);
    diffs.push_back(node.shards_size() + node.future_size() - targets[i]);
  }
  for (int i = 0; i < num_nodes - 1; i++) {
    pb::NodeInfo* left = info_.mutable_nodes(i);
    for (int j = i + 1; j < num_nodes; j++) {
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
  for (int i = 0; i < num_nodes; i++) {
    auto node = info_.mutable_nodes(i);
    std::sort(node->mutable_future()->begin(), node->mutable_future()->end());
  }
  for (int i = 0; i < num_nodes; i++) {
    auto node = info_.nodes(i);
    if (node.future_size() > 0)
      assert(node.shards_size() + node.future_size() == targets[i]);
  }
}

void InfoWrapper::GiveShard(int id, int shard) {
  std::lock_guard<std::mutex> lock(mutex_);
  bool found = false;
  pb::NodeInfo* node = info_.mutable_nodes(id);
  for (int i = 0; i < node->shards_size(); i++) {
    if (node->shards(i) == shard) {
      node->mutable_shards()->SwapElements(i, node->shards_size() - 1);
      node->mutable_shards()->RemoveLast();
      std::sort(node->mutable_shards()->begin(), node->mutable_shards()->end());
      found = true;
      break;
    }
  }
  assert(found);
  for (int i = 0; i < info_.nodes_size(); i++) {
    pb::NodeInfo* node = info_.mutable_nodes(i);
    for (int j = 0; j < node->future_size(); j++) {
      if (node->future(j) == shard) {
        node->mutable_future()->SwapElements(j, node->future_size() - 1);
        node->mutable_future()->RemoveLast();
        node->add_shards(shard);
        std::sort(node->mutable_future()->begin(),
                  node->mutable_future()->end());
        std::sort(node->mutable_shards()->begin(),
                  node->mutable_shards()->end());
        return;
      }
    }
  }
  assert(false);
}

}  // namespace crocks
