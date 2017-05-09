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

#include "src/client/write_batch_impl.h"

#include <assert.h>
#include <stdlib.h>

#include <chrono>

#include <crocks/cluster.h>
#include <crocks/status.h>
#include <crocks/write_batch.h>
#include "src/client/node.h"

namespace crocks {

// WriteBatchImpl wrapper
WriteBatch::WriteBatch(Cluster* db) : impl_(new WriteBatchImpl(db)) {}

WriteBatch::~WriteBatch() {
  delete impl_;
}

void WriteBatch::Put(const std::string& key, const std::string& value) {
  impl_->Put(key, value);
}

void WriteBatch::Delete(const std::string& key) {
  impl_->Delete(key);
}

void WriteBatch::SingleDelete(const std::string& key) {
  impl_->SingleDelete(key);
}

void WriteBatch::Merge(const std::string& key, const std::string& value) {
  impl_->Merge(key, value);
}

void WriteBatch::Clear() {
  impl_->Clear();
}

Status WriteBatch::Write() {
  return impl_->Write();
}

Status WriteBatch::WriteWithLock() {
  return impl_->WriteWithLock();
}

// Write batch implementation
WriteBatch::WriteBatchImpl::WriteBatchImpl(Cluster* db)
    : db_(db),
      // Fill call_ vector with db_->num_nodes() nullptrs
      calls_(db_->num_nodes()) {}

WriteBatch::WriteBatchImpl::~WriteBatchImpl() {
  // Some items may be empty but deleting nullptr is ok
  for (auto call : calls_)
    delete call;
};

void WriteBatch::WriteBatchImpl::Put(const std::string& key,
                                     const std::string& value) {
  int idx = db_->IndexForKey(key);
  AsyncBatchCall* call = EnsureBatchCall(idx);
  pb::BatchUpdate* batch_update = call->request.add_updates();
  batch_update->set_op(pb::BatchUpdate::PUT);
  batch_update->set_key(key);
  batch_update->set_value(value);
  // XXX: ByteSize() seems to be deprecated in favor of ByteSizeLong() which
  // returns size_t instead of int.
  // XXX: We keep track of the total bytes, because call->request.ByteSize()
  // recalculates the size and it's too expensive to do that every time
  // StreamIfExceededThreshold() is called (i.e. at every single batch
  // operation). In crocks.pb.cc apart from the size of each individual
  // BatchUpdate there is this line: total_size += 1 * this->updates_size(); so
  // we need to add at least 1 for each batch_update. However the true size
  // seems to be bigger by 1 or 2 bytes. We add 3 to be sure and in
  // StreamIfExceededThreshold() we assert that our estimation is no bigger than
  // the actual size.
  call->byte_size += batch_update->ByteSize() + 3;
  StreamIfExceededThreshold(idx);
}

void WriteBatch::WriteBatchImpl::Delete(const std::string& key) {
  int idx = db_->IndexForKey(key);
  AsyncBatchCall* call = EnsureBatchCall(idx);
  pb::BatchUpdate* batch_update = call->request.add_updates();
  batch_update->set_op(pb::BatchUpdate::DELETE);
  batch_update->set_key(key);
  call->byte_size += batch_update->ByteSize() + 3;
  StreamIfExceededThreshold(idx);
}

void WriteBatch::WriteBatchImpl::SingleDelete(const std::string& key) {
  int idx = db_->IndexForKey(key);
  AsyncBatchCall* call = EnsureBatchCall(idx);
  pb::BatchUpdate* batch_update = call->request.add_updates();
  batch_update->set_op(pb::BatchUpdate::SINGLE_DELETE);
  batch_update->set_key(key);
  call->byte_size += batch_update->ByteSize() + 3;
  StreamIfExceededThreshold(idx);
}

void WriteBatch::WriteBatchImpl::Merge(const std::string& key,
                                       const std::string& value) {
  int idx = db_->IndexForKey(key);
  AsyncBatchCall* call = EnsureBatchCall(idx);
  pb::BatchUpdate* batch_update = call->request.add_updates();
  batch_update->set_op(pb::BatchUpdate::MERGE);
  batch_update->set_key(key);
  batch_update->set_value(value);
  call->byte_size += batch_update->ByteSize() + 3;
  StreamIfExceededThreshold(idx);
}

void WriteBatch::WriteBatchImpl::Clear() {
  // We clear the current buffers and add a CLEAR operation to each one of them,
  // so that the servers clear their own batches. Since the operations are
  // cleared, there is no way the threshold is exceeded and we don't need to
  // call StreamIfExceededThreshold().
  for (auto call : calls_) {
    if (call == nullptr)
      continue;
    call->request.Clear();
    pb::BatchUpdate* batch_update = call->request.add_updates();
    batch_update->set_op(pb::BatchUpdate::CLEAR);
    call->byte_size += batch_update->ByteSize() + 3;
  }
}

Status WriteBatch::WriteBatchImpl::Write() {
  DoWrite();
  return GetStatus();
}

Status WriteBatch::WriteBatchImpl::WriteWithLock() {
  db_->Lock();
  DoWrite();
  db_->Unlock();
  return GetStatus();
}

// private
AsyncBatchCall* WriteBatch::WriteBatchImpl::EnsureBatchCall(int idx) {
  AsyncBatchCall* call = calls_[idx];
  if (call == nullptr) {
    call = new AsyncBatchCall;
    calls_[idx] = call;
  }
  return call;
}

// Get a tag from the completion queue and decrement the number of
// pending_requests of the related call.
void WriteBatch::WriteBatchImpl::QueueNext() {
  void* got_tag;
  bool ok = false;
  bool got_event = cq_.Next(&got_tag, &ok);
  assert(got_event);
  assert(ok);
  static_cast<AsyncBatchCall*>(got_tag)->pending_requests--;
}

// Similar to QueueNext() but does not block and returns true if it processed a
// call or false if the queue was empty.
bool WriteBatch::WriteBatchImpl::QueueAsyncNext() {
  void* got_tag;
  bool ok = false;
  // The deadline is normally something like
  // std::chrono::system_clock::now() + std::chrono::milliseconds(10)
  // Here we only pass now() so it will timeout immediately.
  switch (cq_.AsyncNext(&got_tag, &ok, std::chrono::system_clock::now())) {
    case grpc::CompletionQueue::GOT_EVENT:
      assert(ok);
      static_cast<AsyncBatchCall*>(got_tag)->pending_requests--;
      return true;
    case grpc::CompletionQueue::TIMEOUT:
      return false;
    // FIXME: When can a queue be shutting down and what should we do about it?
    case grpc::CompletionQueue::SHUTDOWN:
      exit(EXIT_FAILURE);
  }
  // Unreachable
  return false;
}

void WriteBatch::WriteBatchImpl::StreamIfExceededThreshold(int idx) {
  AsyncBatchCall* call = calls_[idx];
  if (call->byte_size <= kByteSizeThreshold1) {
    // Below the low threshold. Do nothing.
    return;
  } else if (call->byte_size <= kByteSizeThreshold2) {
    // Above the low threshold. If there are pending requests even after
    // checking the queue, continue and try again later, else send a buffer.
    while (call->pending_requests > 0 && QueueAsyncNext())
      ;
    if (call->pending_requests == 0)
      Stream(idx);
  } else {
    // Above the high threshold. We have no choice but to block.
    while (call->pending_requests > 0)
      QueueNext();
    Stream(idx);
  }
}

void WriteBatch::WriteBatchImpl::Stream(int idx) {
  AsyncBatchCall* call = calls_[idx];
  if (call->request.updates_size() == 0)
    return;

  assert(call->request.ByteSize() <= call->byte_size);
  if (call->writer == nullptr) {
    assert(call->pending_requests == 0);
    call->writer = db_->NodeByIndex(idx)->AsyncBatchWriter(
        &call->context, &call->response, &cq_, call);
    call->writer->Write(call->request, call);
    call->pending_requests = 2;
  } else {
    // From http://www.grpc.io/grpc/cpp/classgrpc_1_1_client_async_writer.html:
    // "Only one write may be outstanding at any given time. This means that
    // after calling Write, one must wait to receive tag from the completion
    // queue BEFORE calling Write again." So before we call Write(), we have to
    // repeatedly read from the completion queue, until there are no more
    // pending requests for the given call.
    while (call->pending_requests > 0)
      QueueNext();
    call->writer->Write(call->request, call);
    call->request.Clear();
    call->byte_size = 0;
    call->pending_requests = 1;
  }
}

void WriteBatch::WriteBatchImpl::DoWrite() {
  // Fan-out the remaining buffers and the finish-RPC requests
  for (int idx = 0; idx < db_->num_nodes(); idx++) {
    AsyncBatchCall* call = calls_[idx];
    if (call == nullptr)
      continue;
    Stream(idx);
    call->writer->WritesDone(call);
    call->writer->Finish(&call->status, call);
    call->pending_requests += 2;
  }

  // Make sure every server has responded
  for (auto call : calls_) {
    if (call == nullptr)
      continue;
    while (call->pending_requests > 0)
      QueueNext();
  }
}

Status WriteBatch::WriteBatchImpl::GetStatus() {
  // Check the statuses and return the first that's not OK
  for (int idx = 0; idx < db_->num_nodes(); idx++) {
    AsyncBatchCall* call = calls_[idx];
    if (call == nullptr)
      continue;
    call->writer = nullptr;
    if (call->status.ok()) {
      if (call->response.status() != rocksdb::StatusCode::OK)
        return Status(call->response.status());
    } else {
      return Status(call->status);
    }
  }
  return Status();
}

}  // namespace crocks
