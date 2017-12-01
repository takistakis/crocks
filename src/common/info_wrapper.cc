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

#include <utility>

namespace crocks {

int InfoWrapper::AddNode(const std::string& address) {
  write_lock lock(mutex_);
  int id = info_.nodes_size();
  pb::NodeInfo* node = info_.add_nodes();
  node->set_address(address);
  node->set_id(id);
  node->set_available(true);
  return id;
}

int InfoWrapper::AddNodeWithNewShards(const std::string& address,
                                      int num_shards) {
  write_lock lock(mutex_);
  int id = info_.nodes_size();
  pb::NodeInfo* node = info_.add_nodes();
  node->set_address(address);
  node->set_id(id);
  node->set_num_shards(num_shards);
  node->set_available(true);
  for (int i = 0; i < num_shards; i++) {
    pb::ShardInfo* shard = info_.add_shards();
    shard->set_master(id);
  }
  return id;
}

void InfoWrapper::MarkRemoveNode(int id) {
  write_lock lock(mutex_);
  pb::NodeInfo* node = info_.mutable_nodes(id);
  if (node->address().empty())
    return;
  node->set_remove(true);
}

void InfoWrapper::RemoveNode(int id) {
  write_lock lock(mutex_);
  info_.mutable_nodes(id)->Clear();
}

void InfoWrapper::RedistributeShards() {
  write_lock lock(mutex_);
  int num_nodes = 0;
  for (const auto& node : info_.nodes())
    if (!node.remove() && !node.address().empty())
      num_nodes++;
  int num_shards = info_.shards_size();

  // Calculate diffs
  // Positive diff: the node has diff shards to give
  // Negative diff: the node has -diff shards to get
  std::unordered_map<int, int> diffs;
  // Removed nodes get 0 shards. For the rest, each pair must have the same
  // number of shards, or a difference of one shard. Let s be the number of
  // shards and n the number of nodes. Each node will be assigned either s/n
  // or s/n+1 shards and all shards will have to add up to s. We achieve
  // that by giving the first s%n nodes s/n+1 shards and the rest just s/n.
  int i = 0;
  for (const auto& node : info_.nodes()) {
    if (node.address().empty())
      continue;
    if (node.remove()) {
      diffs[node.id()] = node.num_shards();
      continue;
    }
    int target = num_shards / num_nodes;
    if (i < num_shards % num_nodes)
      target++;
    diffs[node.id()] = node.num_shards() - target;
    i++;
  }

  i = 0;
  for (const auto& node : info_.nodes()) {
    int id = node.id();
    while (diffs[id] < 0) {
      pb::ShardInfo* shard = info_.mutable_shards(i);
      if (diffs[shard->master()] > 0) {
        shard->set_migrating(true);
        shard->set_from(shard->master());
        shard->set_to(id);
        diffs[id]++;
        diffs[shard->master()]--;
      }
      i++;
    }
  }
  for (auto pair : diffs)
    assert(pair.second == 0);
}

std::unordered_map<int, std::vector<int>> InfoWrapper::Tasks(int id) const {
  read_lock lock(mutex_);
  std::unordered_map<int, std::vector<int>> tasks;
  assert(id >= 0);
  int i = 0;
  for (const auto& shard : info_.shards()) {
    if (shard.migrating() && shard.to() == id)
      tasks[shard.from()].push_back(i);
    i++;
  }
  return tasks;
}

void InfoWrapper::GiveShard(int id, int shard_id) {
  write_lock lock(mutex_);
  pb::ShardInfo* shard = info_.mutable_shards(shard_id);
  if (shard->master() != id)
    // We have already given the shard. The call must have been
    // interrupted after giving it and now we're resuming.
    return;
  assert(shard->master() == id);
  assert(shard->migrating());
  assert(shard->from() == id);
  assert(shard->to() != id);
  shard->set_master(shard->to());
  pb::NodeInfo* from = info_.mutable_nodes(shard->from());
  pb::NodeInfo* to = info_.mutable_nodes(shard->to());
  from->set_num_shards(from->num_shards() - 1);
  to->set_num_shards(to->num_shards() + 1);
}

void InfoWrapper::MigrationOver(int shard_id) {
  write_lock lock(mutex_);
  pb::ShardInfo* shard = info_.mutable_shards(shard_id);
  shard->set_migrating(false);
  shard->clear_from();
  shard->clear_to();
  for (const auto& shard : info_.shards())
    if (shard.migrating())
      return;
  assert(info_.state() == pb::ClusterInfo::MIGRATING);
  info_.set_state(pb::ClusterInfo::RUNNING);
}

void InfoWrapper::SetAvailable(int id, bool available) {
  write_lock lock(mutex_);
  pb::NodeInfo* node = info_.mutable_nodes(id);
  node->set_available(available);
}

}  // namespace crocks
