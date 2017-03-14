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

#ifndef CROCKS_CLIENT_ITERATOR_IMPL_H
#define CROCKS_CLIENT_ITERATOR_IMPL_H

#include <assert.h>

#include <string>
#include <vector>

#include <grpc++/grpc++.h>

#include <crocks/iterator.h>
#include <crocks/status.h>
#include "src/client/heap.h"
#include "src/client/node_iterator.h"

namespace crocks {

class Cluster;

class Iterator::IteratorImpl {
 public:
  IteratorImpl(Cluster* db);
  ~IteratorImpl();

  bool Valid() const {
    return current_ != nullptr;
  }

  void SeekToFirst();
  void SeekToLast();
  void Seek(const std::string& target);
  void SeekForPrev(const std::string& target);
  void Next();
  void Prev();

  std::string key() const {
    assert(Valid());
    return current_->key();
  }

  std::string value() const {
    assert(Valid());
    return current_->value();
  }

  Status status() const {
    Status status;
    for (auto& iter : iters_) {
      status = iter->status();
      if (!status.ok())
        break;
    }
    return status;
  }

 private:
  void ClearHeaps();

  Cluster* db_;
  grpc::CompletionQueue cq_;

  // iters_ keeps at all times a NodeIterator* for each node in the cluster
  std::vector<NodeIterator*> iters_;
  // Iterators in iters_ that are Valid(), are also in the active heap
  MinHeap min_heap_;
  MaxHeap max_heap_;
  NodeIterator* current_ = nullptr;
  bool forward_;
};

}  // namespace crocks

#endif  // CROCKS_CLIENT_ITERATOR_IMPL_H
