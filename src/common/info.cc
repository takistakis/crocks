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
#include <stdlib.h>

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
      if (info_.IsInit()) {
        info_.AddNodeWithNewShards(address);
      } else if (info_.IsRunning()) {
        info_.AddNode(address);
      } else if (info_.IsMigrating()) {
        std::cout << "Migrating. Try again later." << std::endl;
        exit(EXIT_FAILURE);
      }
      succeeded =
          etcd_.TxnPutIfValueEquals(kInfoKey, info_.Serialize(), old_info);
    } else {
      info_.AddNodeWithNewShards(address);
      succeeded = etcd_.TxnPutIfKeyMissing(kInfoKey, info_.Serialize());
    }
  } while (!succeeded);
  UpdateMap();
  // Set the index and address of the new node
  id_ = num_nodes() - 1;
  address_ = address;
}

void Info::Remove(const std::string& address) {
  bool succeeded;
  do {
    Get();
    assert(IsRunning());
    std::string old_info = info_.Serialize();
    info_.MarkRemoveNode(address);
    succeeded =
        etcd_.TxnPutIfValueEquals(kInfoKey, info_.Serialize(), old_info);
  } while (!succeeded);
}

void Info::Remove() {
  bool succeeded;
  do {
    Get();
    std::string old_info = info_.Serialize();
    info_.RemoveNode(id_);
    succeeded =
        etcd_.TxnPutIfValueEquals(kInfoKey, info_.Serialize(), old_info);
  } while (!succeeded);
  // There's no need to update the map
  UpdateIndex();
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
    info_.SetRunning();
    succeeded =
        etcd_.TxnPutIfValueEquals(kInfoKey, info_.Serialize(), old_info);
  } while (!succeeded);
}

void Info::Migrate() {
  bool succeeded;
  do {
    std::string old_info;
    if (!etcd_.Get(kInfoKey, &old_info))
      return;
    Parse(old_info);
    info_.RedistributeShards();
    if (info_.NoMigrations()) {
      std::cout << "There was nothing to migrate" << std::endl;
      return;
    }
    info_.SetMigrating();
    succeeded =
        etcd_.TxnPutIfValueEquals(kInfoKey, info_.Serialize(), old_info);
  } while (!succeeded);
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
  if (!canceled)
    Parse(info);
  return canceled;
}

void Info::WatchCancel(void* call) {
  etcd_.WatchCancel(call);
}

std::unordered_map<std::string, std::vector<int>> Info::Tasks() {
  std::lock_guard<std::mutex> lock(mutex_);
  std::unordered_map<std::string, std::vector<int>> tasks;
  if (id_ < 0 || id_ >= info_.num_nodes())
    return tasks;
  for (int shard : info_.future(id_))
    tasks[info_.Address(map_[shard])].push_back(shard);
  return tasks;
}

void Info::GiveShard(int shard) {
  bool succeeded;
  do {
    Get();
    std::string old_info = info_.Serialize();
    info_.GiveShard(id_, shard);
    succeeded =
        etcd_.TxnPutIfValueEquals(kInfoKey, info_.Serialize(), old_info);
  } while (!succeeded);
  UpdateMap();
}

void Info::RemoveFuture(int shard) {
  bool succeeded;
  do {
    Get();
    std::string old_info = info_.Serialize();
    info_.RemoveFuture(id_, shard);
    succeeded =
        etcd_.TxnPutIfValueEquals(kInfoKey, info_.Serialize(), old_info);
  } while (!succeeded);
}

void Info::Print() {
  if (info_.IsInit())
    std::cout << "state: INIT" << std::endl;
  else if (info_.IsRunning())
    std::cout << "state: RUNNING" << std::endl;
  else if (info_.IsMigrating())
    std::cout << "state: MIGRATING" << std::endl;
  else
    assert(false);
  std::cout << "nodes: " << num_nodes() << std::endl;
  std::cout << "shards: " << num_shards() << std::endl;
  for (int i = 0; i < num_nodes(); i++) {
    std::cout << "node " << i << ":" << std::endl;
    std::cout << "  address: " << info_.Address(i) << std::endl;
    auto shards = info_.shards(i);
    if (shards.size() > 0)
      std::cout << "  shards: " << ListToString(shards) << " (" << shards.size()
                << ")" << std::endl;
    auto future = info_.future(i);
    if (future.size() > 0)
      std::cout << "  future: " << ListToString(future) << " (" << future.size()
                << ")" << std::endl;
    if (info_.IsRemoved(i))
      std::cout << "  remove: true" << std::endl;
  }
}

void Info::UpdateMap() {
  for (int i = 0; i < num_nodes(); i++) {
    auto shards = info_.shards(i);
    for (int shard : shards)
      map_[shard] = i;
  }
}

void Info::UpdateIndex() {
  if (id_ == -1)
    return;
  int new_id = info_.IndexOf(address_);
  if (id_ != new_id) {
    if (new_id != -1)
      std::cerr << id_ << ": Index changed to " << new_id << std::endl;
    id_ = new_id;
  }
}

}  // namespace crocks
