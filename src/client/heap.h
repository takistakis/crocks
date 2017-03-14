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

#ifndef CROCKS_CLIENT_HEAP_H
#define CROCKS_CLIENT_HEAP_H

#include <assert.h>

#include <queue>
#include <string>
#include <vector>

#include "src/client/node_iterator.h"

namespace crocks {

// Used for the max heap (reverse iteration)
struct NodeIteratorLess {
  bool operator()(const NodeIterator* lhs, const NodeIterator* rhs) const {
    return lhs->key() < rhs->key();
  }
};

// Used for the min heap (forward iteration)
struct NodeIteratorGreater {
  bool operator()(const NodeIterator* lhs, const NodeIterator* rhs) const {
    return lhs->key() > rhs->key();
  }
};

// Heap behaves as a min priority queue but it also allows:
//
// - Adding elements in O(1), losing the heap properties.
// - Regaining the heap properties in O(n).
// - Clearing the underlying container.
//
// Iterating in the underlying container (not in order) is
// also possible but currently not used and not implemented.
template <typename Compare>
class Heap {
 public:
  NodeIterator* top() {
    assert(is_heap_);
    if (empty())
      return nullptr;
    else
      return container_.front();
  }

  void clear() {
    is_heap_ = true;
    container_.clear();
  }

  bool empty() {
    return container_.empty();
  }

  void make_heap() {
    assert(!is_heap_);
    std::make_heap(container_.begin(), container_.end(), cmp_);
    is_heap_ = true;
  }

  void push_back(NodeIterator* iter) {
    container_.push_back(iter);
    is_heap_ = false;
  }

  void push(NodeIterator* iter) {
    assert(is_heap_);
    container_.push_back(iter);
    std::push_heap(container_.begin(), container_.end(), cmp_);
  }

  void pop() {
    assert(is_heap_);
    std::pop_heap(container_.begin(), container_.end(), cmp_);
    container_.pop_back();
  }

 private:
  std::vector<NodeIterator*> container_;
  Compare cmp_;
  bool is_heap_ = true;
};

typedef Heap<NodeIteratorGreater> MinHeap;
typedef Heap<NodeIteratorLess> MaxHeap;

}  // namespace crocks

#endif  // CROCKS_CLIENT_HEAP_H
