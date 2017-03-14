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

#include "src/client/iterator_impl.h"

#include <crocks/cluster.h>

namespace crocks {

Iterator::Iterator(Cluster* db) : impl_(new IteratorImpl(db)) {}

Iterator::~Iterator() {
  delete impl_;
}

bool Iterator::Valid() const {
  return impl_->Valid();
}

void Iterator::SeekToFirst() {
  impl_->SeekToFirst();
}

void Iterator::SeekToLast() {
  impl_->SeekToLast();
}

void Iterator::Seek(const std::string& target) {
  impl_->Seek(target);
}

void Iterator::SeekForPrev(const std::string& target) {
  impl_->SeekForPrev(target);
}

void Iterator::Next() {
  impl_->Next();
}

void Iterator::Prev() {
  impl_->Prev();
}

std::string Iterator::key() const {
  return impl_->key();
}

std::string Iterator::value() const {
  return impl_->value();
}

Status Iterator::status() const {
  return impl_->status();
}

// Iterator implementation
Iterator::IteratorImpl::IteratorImpl(Cluster* db) : db_(db) {
  for (int idx = 0; idx < db_->num_nodes(); idx++)
    iters_.push_back(new NodeIterator(db_->NodeByIndex(idx), &cq_));
}

Iterator::IteratorImpl::~IteratorImpl() {
  for (auto iter : iters_)
    iter->RequestFinish();

  // TODO: This can be done in a background thread instead of blocking
  // here. I don't know when and how can the thread be joined though.
  for (auto iter : iters_) {
    iter->WaitForResponses();
    delete iter;
  }
}

void Iterator::IteratorImpl::SeekToFirst() {
  ClearHeaps();
  forward_ = true;
  for (auto iter : iters_) {
    // XXX: When a seek request is made, some iterators may have pending
    // requests for key-value batches that were not needed, not waited for and
    // might not have arrived. Waiting here, handles both this case and the
    // first seek after instantiation when the we don't have a response even for
    // taking a stream (ClientAsyncReaderWriter) from the Node. The latter is
    // mandatory and could be done in the constructor. For the former, consider
    // gRPC stream cancellation, or somehow requesting with a different tag.
    iter->WaitForResponses();
    iter->RequestSeekToFirst();
  }

  for (auto iter : iters_) {
    iter->WaitForResponses();
    iter->PushBatch();
    if (iter->Valid())
      min_heap_.push_back(iter);
  }

  if (!min_heap_.empty()) {
    min_heap_.make_heap();
    current_ = min_heap_.top();
  }
}

void Iterator::IteratorImpl::SeekToLast() {
  ClearHeaps();
  forward_ = false;
  for (auto iter : iters_) {
    iter->WaitForResponses();
    iter->RequestSeekToLast();
  }

  for (auto iter : iters_) {
    iter->WaitForResponses();
    iter->PushBatch();
    if (iter->Valid())
      max_heap_.push_back(iter);
  }

  if (!max_heap_.empty()) {
    max_heap_.make_heap();
    current_ = max_heap_.top();
  }
}

void Iterator::IteratorImpl::Seek(const std::string& target) {
  ClearHeaps();
  forward_ = true;
  for (auto iter : iters_) {
    iter->WaitForResponses();
    iter->RequestSeek(target);
  }

  for (auto iter : iters_) {
    iter->WaitForResponses();
    iter->PushBatch();
    if (iter->Valid())
      min_heap_.push_back(iter);
  }

  if (!min_heap_.empty()) {
    min_heap_.make_heap();
    current_ = min_heap_.top();
  }
}

void Iterator::IteratorImpl::SeekForPrev(const std::string& target) {
  ClearHeaps();
  forward_ = false;
  for (auto iter : iters_) {
    iter->WaitForResponses();
    iter->RequestSeekForPrev(target);
  }

  for (auto iter : iters_) {
    iter->WaitForResponses();
    iter->PushBatch();
    if (iter->Valid())
      max_heap_.push_back(iter);
  }

  if (!max_heap_.empty()) {
    max_heap_.make_heap();
    current_ = max_heap_.top();
  }
}

void Iterator::IteratorImpl::Next() {
  assert(Valid());
  // Unexpected direction. The buffers are currently in
  // descending order. Do a normal seek for the current
  // key, to make the servers send the next key-values.
  if (!forward_)
    Seek(key());
  // Pop the node iterator with the smallest key, and push it again after
  // calling Next() on it, to assure that the heap properties are maintained.
  // TODO: This requires at least logN comparisons for each of the two
  // operations even if it belongs to the top. We could achieve just logN for
  // the whole thing and only 1 or 2 comparisons if no rearrangement is actually
  // needed, with a custom priority queue implementation. Some things could
  // be copied from the rocksdb heap implementation in rocksdb/util/heap.h.
  assert(current_ == min_heap_.top());
  min_heap_.pop();
  current_->Next();
  if (current_->Valid())
    min_heap_.push(current_);
  current_ = min_heap_.top();
}

void Iterator::IteratorImpl::Prev() {
  assert(Valid());
  if (forward_)
    SeekForPrev(key());
  assert(current_ == max_heap_.top());
  max_heap_.pop();
  current_->Prev();
  if (current_->Valid())
    max_heap_.push(current_);
  current_ = max_heap_.top();
}

void Iterator::IteratorImpl::ClearHeaps() {
  min_heap_.clear();
  max_heap_.clear();
  current_ = nullptr;
}

}  // namespace crocks
