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

#include <grpc++/grpc++.h>

#include "src/client/hash.h"
#include "src/client/node.h"

namespace crocks {

Cluster::Cluster(std::vector<std::string> addresses)
    : impl_(new ClusterImpl(addresses)) {}

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

int Cluster::IndexForKey(const std::string& key) {
  return impl_->IndexForKey(key);
}

Node* Cluster::NodeForKey(const std::string& key) {
  return impl_->NodeForKey(key);
}

Node* Cluster::NodeByIndex(int idx) {
  return impl_->NodeByIndex(idx);
}

int Cluster::num_nodes() const {
  return impl_->num_nodes();
}

Cluster* DBOpen(std::vector<std::string> addresses) {
  return new Cluster(addresses);
}

// Cluster implementation
Cluster::ClusterImpl::ClusterImpl(std::vector<std::string> addresses)
    : size_(addresses.size()) {
  for (const std::string& address : addresses) {
    Node* node = new Node(
        grpc::CreateChannel(address, grpc::InsecureChannelCredentials()));
    nodes_.push_back(node);
  }
}

Cluster::ClusterImpl::~ClusterImpl() {
  for (Node* node : nodes_)
    delete node;
}

Status Cluster::ClusterImpl::Get(const std::string& key, std::string* value) {
  return NodeForKey(key)->Get(key, value);
}

Status Cluster::ClusterImpl::Put(const std::string& key,
                                 const std::string& value) {
  return NodeForKey(key)->Put(key, value);
}

Status Cluster::ClusterImpl::Delete(const std::string& key) {
  return NodeForKey(key)->Delete(key);
}

Status Cluster::ClusterImpl::SingleDelete(const std::string& key) {
  return NodeForKey(key)->SingleDelete(key);
}

Status Cluster::ClusterImpl::Merge(const std::string& key,
                                   const std::string& value) {
  return NodeForKey(key)->Merge(key, value);
}

int Cluster::ClusterImpl::IndexForKey(const std::string& key) {
  return Hash(key) % size_;
}

Node* Cluster::ClusterImpl::NodeForKey(const std::string& key) {
  int idx = Hash(key) % size_;
  return nodes_[idx];
}

Node* Cluster::ClusterImpl::NodeByIndex(int idx) {
  return nodes_[idx];
}

}  // namespace crocks
