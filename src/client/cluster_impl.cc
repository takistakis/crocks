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

int Cluster::IndexForShard(int shard) {
  return impl_->IndexForShard(shard);
}

int Cluster::ShardForKey(const std::string& key) {
  return impl_->ShardForKey(key);
}

int Cluster::IndexForKey(const std::string& key) {
  return impl_->IndexForKey(key);
}

Node* Cluster::NodeForKey(const std::string& key) {
  return impl_->NodeForKey(key);
}

Node* Cluster::NodeByIndex(int idx) {
  return impl_->NodeByIndex(idx);
}

std::string Cluster::AddressForShard(int shard, bool update) {
  return impl_->AddressForShard(shard, update);
}

int Cluster::num_nodes() const {
  return impl_->num_nodes();
}

int Cluster::num_shards() const {
  return impl_->num_shards();
}

void Cluster::Lock() {
  return impl_->Lock();
}

void Cluster::Unlock() {
  return impl_->Unlock();
}

Cluster* DBOpen(const std::string& address) {
  return new Cluster(address);
}

// Cluster implementation
Cluster::ClusterImpl::ClusterImpl(const std::string& address) : info_(address) {
  info_.Get();
  info_.Run();
  for (const std::string& address : info_.Addresses())
    nodes_.push_back(new Node(address));
}

Cluster::ClusterImpl::~ClusterImpl() {
  for (Node* node : nodes_)
    delete node;
}

Status Cluster::ClusterImpl::Get(const std::string& key, std::string* value) {
  Status status = NodeForKey(key)->Get(key, value);
  while (status.grpc_code() == grpc::StatusCode::INVALID_ARGUMENT) {
    Update();
    status = NodeForKey(key)->Get(key, value);
  }
  return status;
}

Status Cluster::ClusterImpl::Put(const std::string& key,
                                 const std::string& value) {
  Status status = NodeForKey(key)->Put(key, value);
  while (status.grpc_code() == grpc::StatusCode::INVALID_ARGUMENT) {
    Update();
    status = NodeForKey(key)->Put(key, value);
  }
  return status;
}

Status Cluster::ClusterImpl::Delete(const std::string& key) {
  Status status = NodeForKey(key)->Delete(key);
  while (status.grpc_code() == grpc::StatusCode::INVALID_ARGUMENT) {
    Update();
    status = NodeForKey(key)->Delete(key);
  }
  return status;
}

Status Cluster::ClusterImpl::SingleDelete(const std::string& key) {
  Status status = NodeForKey(key)->SingleDelete(key);
  while (status.grpc_code() == grpc::StatusCode::INVALID_ARGUMENT) {
    Update();
    status = NodeForKey(key)->SingleDelete(key);
  }
  return status;
}

Status Cluster::ClusterImpl::Merge(const std::string& key,
                                   const std::string& value) {
  Status status = NodeForKey(key)->Merge(key, value);
  while (status.grpc_code() == grpc::StatusCode::INVALID_ARGUMENT) {
    Update();
    status = NodeForKey(key)->Merge(key, value);
  }
  return status;
}

int Cluster::ClusterImpl::IndexForShard(int shard) {
  return info_.IndexForShard(shard);
}

int Cluster::ClusterImpl::ShardForKey(const std::string& key) {
  return info_.ShardForKey(key);
}

int Cluster::ClusterImpl::IndexForKey(const std::string& key) {
  return info_.IndexForKey(key);
}

Node* Cluster::ClusterImpl::NodeForKey(const std::string& key) {
  int idx = info_.IndexForKey(key);
  return nodes_[idx];
}

Node* Cluster::ClusterImpl::NodeByIndex(int idx) {
  return nodes_[idx];
}

std::string Cluster::ClusterImpl::AddressForShard(int shard, bool update) {
  if (update)
    Update();
  int idx = info_.IndexForShard(shard);
  return nodes_[idx]->address();
}

void Cluster::ClusterImpl::Update() {
  info_.Get();
  std::vector<Node*> new_nodes;
  for (const std::string& address : info_.Addresses()) {
    // For each node address remove the Node* with that address
    // from the original vector and push it on the new one.
    Node* node = nullptr;
    for (auto it = nodes_.begin(); it != nodes_.end(); it++) {
      Node* cur = *it;
      if (cur->address() == address) {
        nodes_.erase(it);
        node = cur;
        break;
      }
    }
    // If there was no such node, create a new one
    if (node == nullptr)
      node = new Node(address);
    new_nodes.push_back(node);
  }
  // Delete the rest nodes (i.e. the ones that were removed)
  for (Node* node : nodes_)
    delete node;
  // Finally replace the old vector
  nodes_ = std::move(new_nodes);
}

}  // namespace crocks
