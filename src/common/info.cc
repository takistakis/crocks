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
      AddWithNewShards(address);
      succeeded = etcd_.TxnPutIfValueEquals(kInfoKey, Serialize(), old_info);
    } else {
      AddWithNewShards(address);
      succeeded = etcd_.TxnPutIfKeyMissing(kInfoKey, Serialize());
    }
  } while (!succeeded);
  // Set the index of the new node
  id_ = num_nodes() - 1;
}

void Info::Remove(const std::string& address) {
  // Remove the node from the list
  for (int i = 0; i < num_nodes(); i++) {
    if (info_.nodes(i).address() == address) {
      info_.mutable_nodes()->SwapElements(i, num_nodes() - 1);
      break;
    }
  }
  pb::NodeInfo* old_node = info_.mutable_nodes()->ReleaseLast();

  // For each node left, remove shards from the end of the removed
  // node and assign it to them, until the new target is reached.
  std::vector<int> targets = Distribute(num_shards(), num_nodes());
  for (int i = 0; i < num_nodes(); i++) {
    pb::NodeInfo* node = info_.mutable_nodes(i);
    int new_shards = targets[i] - node->shards_size();
    for (int j = 0; j < new_shards; j++) {
      int last = old_node->shards(old_node->shards_size() - 1);
      old_node->mutable_shards()->RemoveLast();
      node->add_shards(last);
    }
    std::sort(node->mutable_shards()->begin(), node->mutable_shards()->end());
  }

  // Every shard of the old node must have been redistributed
  assert(old_node->shards_size() == 0);
  delete old_node;
}

void Info::Print() {
  std::cout << "nodes: " << num_nodes() << std::endl;
  std::cout << "shards: " << num_shards() << std::endl;
  int i = 0;
  for (const auto& node : info_.nodes()) {
    std::cout << "node " << i << ":" << std::endl;
    std::cout << "  address: " << node.address() << std::endl;
    std::cout << "  shards: ";
    std::cout << ListToString(node.shards()) << std::endl;
    i++;
  }
}

// private
void Info::AddWithNewShards(const std::string& address) {
  pb::NodeInfo* node = info_.add_nodes();
  node->set_address(address);
  for (int i = num_shards(); i < num_shards() + kShardsPerNode; i++)
    node->add_shards(i);
  info_.set_num_shards(num_shards() + kShardsPerNode);
  UpdateMap();
}

void Info::AddRedistributingShards(const std::string& address) {
  pb::NodeInfo* new_node = info_.add_nodes();
  new_node->set_address(address);

  // For each existing node, remove shards from the end and assign
  // them to the new node, until the new target is reached.
  std::vector<int> targets = Distribute(num_shards(), num_nodes());
  for (int i = 0; i < num_nodes() - 1; i++) {
    pb::NodeInfo* node = info_.mutable_nodes(i);
    for (int j = node->shards_size() - 1; j >= targets[i]; j--) {
      int last = node->shards(j);
      node->mutable_shards()->RemoveLast();
      new_node->add_shards(last);
    }
  }

  // Sorting the shards isn't necessary but it keeps the database tidy
  std::sort(new_node->mutable_shards()->begin(),
            new_node->mutable_shards()->end());

  // Shards assigned to the new node must be as many as predicted
  assert(new_node->shards_size() == targets.back());
  UpdateMap();
}

std::vector<int> Info::Distribute(int s, int n) {
  // Each pair of nodes must have the same number of shards, or a
  // difference of one shard. Let s be the number of shards and n the
  // number of nodes. Each node will be assigned either n/s or n/s+1
  // shards and all shards will have to add up to s. We achieve that
  // by giving the first n%s nodes n/s+1 shards and the rest just n/s.
  std::vector<int> vec;
  for (int i = 0; i < s % n; i++)
    vec.push_back(s / n + 1);
  for (int i = s % n; i < n; i++)
    vec.push_back(s / n);
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

}  // namespace crocks
