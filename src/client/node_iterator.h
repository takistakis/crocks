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

#ifndef CROCKS_CLIENT_NODE_ITERATOR_H
#define CROCKS_CLIENT_NODE_ITERATOR_H

#include <assert.h>

#include <memory>
#include <queue>
#include <string>

#include <grpc++/grpc++.h>

#include <crocks/status.h>
#include "gen/crocks.pb.h"
#include "src/client/node.h"  // IWYU pragma: keep

namespace crocks {

// TODO: A state machine and a Proceed() member function that checks the
// state and acts accordingly might be a good idea. We have to make sure
// that the order between a write and a read response is guaranteed.

class NodeIterator {
 public:
  NodeIterator(Node* node, grpc::CompletionQueue* cq);

  void PushBatch();
  void Push(pb::KeyValue kv);
  void Next();
  void Prev();
  void RequestSeekToFirst();
  void RequestSeekToLast();
  void RequestSeek(const std::string& target);
  void RequestSeekForPrev(const std::string& target);
  void RequestNext();
  void RequestPrev();
  void RequestFinish();
  void WaitForResponses();
  void DecrementPendingRequests();

  bool Valid() const {
    return valid_;
  }

  std::string key() const {
    assert(Valid());
    return queue_.front().key();
  }

  std::string value() const {
    assert(Valid());
    return queue_.front().value();
  }

  Status status() const {
    return Status(status_, response_.status());
  }

 private:
  void ClearQueue();

  std::queue<pb::KeyValue> queue_;
  bool forward_;
  bool valid_ = false;
  bool done_ = false;

  pb::IteratorRequest request_;
  pb::IteratorResponse response_;
  grpc::ClientContext context_;
  grpc::Status status_;
  grpc::CompletionQueue* cq_;
  std::unique_ptr<
      grpc::ClientAsyncReaderWriter<pb::IteratorRequest, pb::IteratorResponse>>
      stream_ = nullptr;
  int pending_requests_;
};

}  // namespace crocks

#endif  // CROCKS_CLIENT_NODE_ITERATOR_H
