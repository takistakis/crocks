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

#include "src/client/cluster_impl.h"

#include <assert.h>

#include <utility>

#include <grpc++/grpc++.h>

#include <crocks/cluster.h>
#include "src/client/node.h"

namespace crocks {

Cluster::Cluster(const std::string& address)
    : impl_(new ClusterImpl(address)) {}

Cluster::~Cluster() {
  delete impl_;
}

Status Cluster::Get(const std::string& key, std::string* value) {
  return impl_->Get(key, value);
}

Status Cluster::Put(const std::string& key, const std::string& value) {
  return impl_->Put(key, value);
}

Status Cluster::Delete(const std::string& key) {
  return impl_->Delete(key);
}

Status Cluster::SingleDelete(const std::string& key) {
  return impl_->SingleDelete(key);
}

Status Cluster::Merge(const std::string& key, const std::string& value) {
  return impl_->Merge(key, value);
}

Cluster* DBOpen(const std::string& address) {
  return new Cluster(address);
}

// Cluster implementation
ClusterImpl::ClusterImpl(const std::string& address) : info_(address) {
  info_.Get();
  info_.Run();
  int id = 0;
  for (const auto& address : info_.Addresses()) {
    if (!address.empty())
      nodes_[id] = new Node(address);
    id++;
  }
}

ClusterImpl::~ClusterImpl() {
  for (const auto& pair : nodes_)
    delete pair.second;
}

Status ClusterImpl::Get(const std::string& key, std::string* value) {
  Node* node = NodeForKey(key);
  Status status = node->Get(key, value);
  while (status.IsUnavailable()) {
    Update();
    Node* new_node = NodeForKey(key);
    if (new_node != node) {
      status = new_node->Get(key, value);
      node = new_node;
    } else {
      info_.SetAvailable(IndexForKey(key), false);
      break;
    }
  }
  while (status.grpc_code() == grpc::StatusCode::INVALID_ARGUMENT) {
    Update();
    status = NodeForKey(key)->Get(key, value);
  }
  return status;
}

Status ClusterImpl::Put(const std::string& key, const std::string& value) {
  Node* node = NodeForKey(key);
  Status status = node->Put(key, value);
  while (status.IsUnavailable()) {
    Update();
    Node* new_node = NodeForKey(key);
    if (new_node != node) {
      status = new_node->Put(key, value);
      node = new_node;
    } else {
      info_.SetAvailable(IndexForKey(key), false);
      break;
    }
  }
  while (status.grpc_code() == grpc::StatusCode::INVALID_ARGUMENT) {
    Update();
    status = NodeForKey(key)->Put(key, value);
  }
  return status;
}

Status ClusterImpl::Delete(const std::string& key) {
  Node* node = NodeForKey(key);
  Status status = node->Delete(key);
  while (status.IsUnavailable()) {
    Update();
    Node* new_node = NodeForKey(key);
    if (new_node != node) {
      status = new_node->Delete(key);
      node = new_node;
    } else {
      info_.SetAvailable(IndexForKey(key), false);
      break;
    }
  }
  while (status.grpc_code() == grpc::StatusCode::INVALID_ARGUMENT) {
    Update();
    status = NodeForKey(key)->Delete(key);
  }
  return status;
}

Status ClusterImpl::SingleDelete(const std::string& key) {
  Node* node = NodeForKey(key);
  Status status = node->SingleDelete(key);
  while (status.IsUnavailable()) {
    Update();
    Node* new_node = NodeForKey(key);
    if (new_node != node) {
      status = new_node->SingleDelete(key);
      node = new_node;
    } else {
      info_.SetAvailable(IndexForKey(key), false);
      break;
    }
  }
  while (status.grpc_code() == grpc::StatusCode::INVALID_ARGUMENT) {
    Update();
    status = NodeForKey(key)->SingleDelete(key);
  }
  return status;
}

Status ClusterImpl::Merge(const std::string& key, const std::string& value) {
  Node* node = NodeForKey(key);
  Status status = node->Merge(key, value);
  while (status.IsUnavailable()) {
    Update();
    Node* new_node = NodeForKey(key);
    if (new_node != node) {
      status = new_node->Merge(key, value);
      node = new_node;
    } else {
      info_.SetAvailable(IndexForKey(key), false);
      break;
    }
  }
  while (status.grpc_code() == grpc::StatusCode::INVALID_ARGUMENT) {
    Update();
    status = NodeForKey(key)->Merge(key, value);
  }
  return status;
}

int ClusterImpl::IndexForShard(int shard) {
  return info_.IndexForShard(shard);
}

int ClusterImpl::ShardForKey(const std::string& key) {
  return info_.ShardForKey(key);
}

int ClusterImpl::IndexForKey(const std::string& key) {
  return info_.IndexForKey(key);
}

Node* ClusterImpl::NodeForKey(const std::string& key) {
  int idx = info_.IndexForKey(key);
  return nodes_[idx];
}

Node* ClusterImpl::NodeByIndex(int idx) {
  return nodes_[idx];
}

std::string ClusterImpl::AddressForShard(int shard, bool update) {
  if (update)
    Update();
  int idx = info_.IndexForShard(shard);
  return nodes_[idx]->address();
}

void ClusterImpl::Update() {
  info_.Get();
  int id = 0;
  for (const auto& address : info_.Addresses()) {
    if (address.empty()) {
      delete nodes_[id];
      nodes_[id] = nullptr;
    } else if (nodes_[id] == nullptr) {
      nodes_[id] = new Node(address);
    } else {
      assert(nodes_[id]->address() == address);
    }
    id++;
  }
}

}  // namespace crocks
