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

#include "src/client/node_iterator.h"

namespace crocks {

// Key-value pairs left in the queue that trigger a new request
const int kToGo = 5;

NodeIterator::NodeIterator(Node* node, grpc::CompletionQueue* cq)
    : cq_(cq),
      stream_(node->AsyncIteratorStream(&context_, cq, this)),
      pending_requests_(1) {}

void NodeIterator::PushBatch() {
  assert(pending_requests_ == 0);
  for (const pb::KeyValue& kv : response_.kvs())
    Push(kv);
  done_ = response_.done();
}

void NodeIterator::Push(pb::KeyValue kv) {
  if (queue_.empty()) {
    valid_ = true;
  } else {
    // KeyValues must be inserted in order
    if (forward_)
      assert(kv.key() > key());
    else
      assert(kv.key() < key());
  }
  queue_.push(kv);
}

void NodeIterator::Next() {
  // Assert forward direction. The one responsible for
  // unexpected direction changes is the merging iterator.
  assert(forward_);
  assert(Valid());
  assert(!queue_.empty());
  // Move forward by removing an item from the queue. The queue cannot be
  // empty, because as soon as it becomes empty, we wait for the next batch.
  queue_.pop();
  if (queue_.size() == kToGo) {
    // The buffer is starting to have fewer items. Request early
    // a new batch. Assert that there are no pending requests,
    // because the request for the current batch has been answered
    // (obviously) and the request for the next will be made now.
    assert(pending_requests_ == 0);
    if (!done_)
      RequestNext();
  } else if (queue_.empty()) {
    // There are no items left in the queue
    if (done_) {
      // The server has reached the end. Mark as invalid and return.
      valid_ = false;
    } else {
      // The server has not reached the end and the next batch
      // has been requested (at kToGo) but has not yet come. We
      // have no choice but to block waiting for the response.
      WaitForResponses();
      PushBatch();
    }
  }
}

void NodeIterator::Prev() {
  assert(!forward_);
  assert(Valid());
  assert(!queue_.empty());
  queue_.pop();
  if (queue_.size() == kToGo) {
    assert(pending_requests_ == 0);
    if (!done_)
      RequestPrev();
  } else if (queue_.empty()) {
    if (done_) {
      valid_ = false;
    } else {
      WaitForResponses();
      PushBatch();
    }
  }
}

void NodeIterator::RequestSeekToFirst() {
  assert(pending_requests_ == 0);
  ClearQueue();
  forward_ = true;
  valid_ = false;
  request_.set_op(pb::IteratorRequest::SEEK_TO_FIRST);
  stream_->Write(request_, this);
  stream_->Read(&response_, this);
  pending_requests_ += 2;
}

void NodeIterator::RequestSeekToLast() {
  assert(pending_requests_ == 0);
  ClearQueue();
  forward_ = false;
  valid_ = false;
  request_.set_op(pb::IteratorRequest::SEEK_TO_LAST);
  stream_->Write(request_, this);
  stream_->Read(&response_, this);
  pending_requests_ += 2;
}

void NodeIterator::RequestSeek(const std::string& target) {
  assert(pending_requests_ == 0);
  ClearQueue();
  forward_ = true;
  valid_ = false;
  request_.set_op(pb::IteratorRequest::SEEK);
  request_.set_target(target);
  stream_->Write(request_, this);
  stream_->Read(&response_, this);
  pending_requests_ += 2;
}

void NodeIterator::RequestSeekForPrev(const std::string& target) {
  assert(pending_requests_ == 0);
  ClearQueue();
  forward_ = false;
  valid_ = false;
  request_.set_op(pb::IteratorRequest::SEEK_FOR_PREV);
  request_.set_target(target);
  stream_->Write(request_, this);
  stream_->Read(&response_, this);
  pending_requests_ += 2;
}

void NodeIterator::RequestNext() {
  assert(pending_requests_ == 0);
  request_.set_op(pb::IteratorRequest::NEXT);
  stream_->Write(request_, this);
  stream_->Read(&response_, this);
  pending_requests_ += 2;
}

void NodeIterator::RequestPrev() {
  assert(pending_requests_ == 0);
  request_.set_op(pb::IteratorRequest::PREV);
  stream_->Write(request_, this);
  stream_->Read(&response_, this);
  pending_requests_ += 2;
}

void NodeIterator::RequestFinish() {
  stream_->WritesDone(this);
  stream_->Finish(&status_, this);
  pending_requests_ += 2;
}

void NodeIterator::WaitForResponses() {
  while (pending_requests_ > 0) {
    void* got_tag;
    bool ok = false;
    bool got_event = cq_->Next(&got_tag, &ok);
    assert(got_event);
    assert(ok);
    static_cast<NodeIterator*>(got_tag)->DecrementPendingRequests();
  }
}

void NodeIterator::DecrementPendingRequests() {
  pending_requests_--;
}

void NodeIterator::ClearQueue() {
  // XXX: Swap the queue with an empty queue as a workaround for the
  // fact that queue doesn't have a clear() member function. Switching
  // to the much more versatile deque would be more straightforward.
  std::queue<pb::KeyValue> empty;
  std::swap(queue_, empty);
}

}  // namespace crocks
