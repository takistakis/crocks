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
#include <iostream>
#include <utility>

#include <crocks/cluster.h>
#include <crocks/status.h>
#include <crocks/write_batch.h>
#include "src/client/cluster_impl.h"
#include "src/client/node.h"

namespace crocks {

// WriteBatchImpl wrapper
WriteBatch::WriteBatch(Cluster* db)
    : impl_(new WriteBatchImpl(db, 64 * 1024, 1024 * 1024)) {}

WriteBatch::WriteBatch(Cluster* db, int threshold_low, int threshold_high)
    : impl_(new WriteBatchImpl(db, threshold_low, threshold_high)) {}

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

void Buffer::AddPut(const std::string& key, const std::string& value) {
  pb::BatchUpdate* batch_update = buffer_.add_updates();
  batch_update->set_op(pb::BatchUpdate::PUT);
  batch_update->set_key(key);
  batch_update->set_value(value);
  // XXX: ByteSize() seems to be deprecated in favor of ByteSizeLong()
  // which returns size_t instead of int.
  // XXX: We keep track of the total bytes, because buffer_.ByteSize()
  // recalculates the size and it's too expensive to do that every
  // time StreamIfExceededThreshold() is called (i.e. at every single
  // batch operation). In crocks.pb.cc apart from the size of each
  // individual BatchUpdate there is this line: total_size += 1 *
  // this->updates_size(); so we need to add at least 1 for each
  // batch_update. However the true size seems to be bigger by 1 or
  // 2 bytes. We add 3 to be sure and in StreamIfExceededThreshold()
  // we assert that our estimation is no bigger than the actual size.
  byte_size_ += batch_update->ByteSize() + 3;
}

void Buffer::AddDelete(const std::string& key) {
  pb::BatchUpdate* batch_update = buffer_.add_updates();
  batch_update->set_op(pb::BatchUpdate::DELETE);
  batch_update->set_key(key);
  byte_size_ += batch_update->ByteSize() + 3;
}

void Buffer::AddSingleDelete(const std::string& key) {
  pb::BatchUpdate* batch_update = buffer_.add_updates();
  batch_update->set_op(pb::BatchUpdate::SINGLE_DELETE);
  batch_update->set_key(key);
  byte_size_ += batch_update->ByteSize() + 3;
}

void Buffer::AddMerge(const std::string& key, const std::string& value) {
  pb::BatchUpdate* batch_update = buffer_.add_updates();
  batch_update->set_op(pb::BatchUpdate::MERGE);
  batch_update->set_key(key);
  batch_update->set_value(value);
  byte_size_ += batch_update->ByteSize() + 3;
}

void Buffer::AddClear() {
  pb::BatchUpdate* batch_update = buffer_.add_updates();
  batch_update->set_op(pb::BatchUpdate::CLEAR);
  byte_size_ += batch_update->ByteSize() + 3;
}

void Buffer::Clear() {
  buffer_.Clear();
  byte_size_ = 0;
}

void Buffer::Stream() {
  call_->stream->Write(buffer_, call_);
  call_->pending_requests++;
}

void Buffer::RestreamFirstBuffer() {
  assert(first_);
  assert(read_requested_);
  assert(first_buffer_.updates_size() > 0);
  call_->stream->Write(first_buffer_, call_);
  call_->pending_requests++;
}

void Buffer::RequestRead() {
  assert(first_);
  call_->stream->Read(&response_, call_);
  // The first time this is called we have to copy the buffer
  if (!read_requested_) {
    read_requested_ = true;
    first_buffer_ = buffer_;
  }
  call_->pending_requests++;
}

// Write batch implementation
WriteBatch::WriteBatchImpl::WriteBatchImpl(Cluster* db, int threshold_low,
                                           int threshold_high)
    : db_(db->get()),
      // Fill buffer_ vector with db_->num_shards() nullptrs
      buffers_(db_->num_shards()),
      threshold_low_(threshold_low),
      threshold_high_(threshold_high) {}

WriteBatch::WriteBatchImpl::~WriteBatchImpl() {
  // Some items may be empty but deleting nullptr is ok
  for (auto pair : calls_)
    delete pair.second;
  for (auto buffer : buffers_)
    delete buffer;
};

void WriteBatch::WriteBatchImpl::Put(const std::string& key,
                                     const std::string& value) {
  int shard = db_->ShardForKey(key);
  Buffer* buffer = EnsureBuffer(shard);
  buffer->AddPut(key, value);
  StreamIfExceededThreshold(shard);
}

void WriteBatch::WriteBatchImpl::Delete(const std::string& key) {
  int shard = db_->ShardForKey(key);
  Buffer* buffer = EnsureBuffer(shard);
  buffer->AddDelete(key);
  StreamIfExceededThreshold(shard);
}

void WriteBatch::WriteBatchImpl::SingleDelete(const std::string& key) {
  int shard = db_->ShardForKey(key);
  Buffer* buffer = EnsureBuffer(shard);
  buffer->AddSingleDelete(key);
  StreamIfExceededThreshold(shard);
}

void WriteBatch::WriteBatchImpl::Merge(const std::string& key,
                                       const std::string& value) {
  int shard = db_->ShardForKey(key);
  Buffer* buffer = EnsureBuffer(shard);
  buffer->AddMerge(key, value);
  StreamIfExceededThreshold(shard);
}

void WriteBatch::WriteBatchImpl::Clear() {
  // We clear the current buffers and add a CLEAR operation to each one of them,
  // so that the servers clear their own batches. Since the operations are
  // cleared, there is no way the threshold is exceeded and we don't need to
  // call StreamIfExceededThreshold().
  for (auto buffer : buffers_) {
    buffer->Clear();
    buffer->AddClear();
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
AsyncBatchCall* WriteBatch::WriteBatchImpl::EnsureBatchCall(int id) {
  AsyncBatchCall* call = calls_[id];
  if (call == nullptr) {
    call = new AsyncBatchCall;
    Node* node = db_->NodeByIndex(id);
    call->stream = node->AsyncBatchStream(&call->context, &cq_, call);
    call->pending_requests = 1;
    calls_[id] = call;
  }
  return call;
}

Buffer* WriteBatch::WriteBatchImpl::EnsureBuffer(int shard) {
  Buffer* buffer = buffers_[shard];
  if (buffer == nullptr) {
    buffer = new Buffer;
    buffer->set_shard(shard);
    buffers_[shard] = buffer;
  }
  return buffer;
}

// Get a tag from the completion queue and decrement the number of
// pending_requests of the related call.
void WriteBatch::WriteBatchImpl::QueueNext() {
  void* got_tag;
  bool ok = false;
  bool got_event = cq_.Next(&got_tag, &ok);
  assert(got_event);
  AsyncBatchCall* call;
  call = static_cast<AsyncBatchCall*>(got_tag);
  if (ok) {
    call->pending_requests--;
  } else {
    call->shutdown = true;
    call->pending_requests = 0;
  }
}

// Similar to QueueNext() but does not block and returns true if it processed a
// call or false if the queue was empty.
bool WriteBatch::WriteBatchImpl::QueueAsyncNext() {
  void* got_tag;
  bool ok = false;
  AsyncBatchCall* call;
  // The deadline is normally something like
  // std::chrono::system_clock::now() + std::chrono::milliseconds(10)
  // Here we only pass now() so it will timeout immediately.
  switch (cq_.AsyncNext(&got_tag, &ok, std::chrono::system_clock::now())) {
    case grpc::CompletionQueue::GOT_EVENT:
      call = static_cast<AsyncBatchCall*>(got_tag);
      if (ok) {
        call->pending_requests--;
      } else {
        call->shutdown = true;
        call->pending_requests = 0;
      }
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

void WriteBatch::WriteBatchImpl::StreamIfExceededThreshold(int shard) {
  Buffer* buffer = buffers_[shard];
  AsyncBatchCall* call = buffer->call();
  if (call == nullptr) {
    int node_id = db_->IndexForShard(shard);
    call = EnsureBatchCall(node_id);
    buffer->set_call(call);
  }
  if (buffer->ByteSize() <= threshold_low_) {
    // Below the low threshold. Do nothing.
    return;
  } else if (buffer->ByteSize() <= threshold_high_) {
    // Above the low threshold. If there are pending requests even after
    // checking the queue, continue and try again later, else send a buffer.
    while (call->pending_requests > 0 && QueueAsyncNext())
      ;
    if (call->pending_requests == 0)
      Stream(buffer);
  } else {
    // Above the high threshold. We have no choice but to block.
    Stream(buffer);
  }
}

void WriteBatch::WriteBatchImpl::Stream(Buffer* buffer) {
  assert(buffer->updates_size() > 0);
  assert(buffer->RealByteSize() <= buffer->ByteSize());
  AsyncBatchCall* call = buffer->call();
  // From http://www.grpc.io/grpc/cpp/classgrpc_1_1_client_async_writer.html:
  // "Only one write may be outstanding at any given time. This means that
  // after calling Write, one must wait to receive tag from the completion
  // queue BEFORE calling Write again." So before we call Write(), we have to
  // repeatedly read from the completion queue, until there are no more
  // pending requests for the given call.
  while (call->pending_requests > 0)
    QueueNext();
  if (buffer->first()) {
    if (buffer->read_requested()) {
      if (buffer->ok() && !call->shutdown) {
        // OK, set not first and stream normally
        buffer->set_first(false);
        buffer->Stream();
        buffer->Clear();
      } else {
        // Not OK, update and stream the first buffer again
        int node_id = db_->IndexForShard(buffer->shard(), true);
        AsyncBatchCall* call = EnsureBatchCall(node_id);
        buffer->set_call(call);
        while (call->pending_requests > 0)
          QueueNext();
        buffer->RestreamFirstBuffer();
        buffer->RequestRead();
      }
    } else {
      // Stream the first buffer and request a response
      buffer->Stream();
      buffer->RequestRead();
      buffer->Clear();
    }
  } else {
    // Stream normally
    buffer->Stream();
    buffer->Clear();
  }
}

void WriteBatch::WriteBatchImpl::DoWrite() {
  // Fan-out the remaining buffers and the finish-RPC requests
  for (Buffer* buffer : buffers_) {
    if (buffer == nullptr)
      continue;
    assert(buffer->call() != nullptr);
    if (buffer->updates_size() > 0)
      Stream(buffer);
    // assert(!buffer->first());
  }

  for (auto pair : calls_) {
    AsyncBatchCall* call = pair.second;
    assert(call != nullptr);
    while (call->pending_requests > 0)
      QueueNext();
    call->stream->WritesDone(call);
    call->stream->Read(&call->response, call);
    call->stream->Finish(&call->status, call);
    call->pending_requests += 3;
  }

  // Make sure every server has responded
  for (auto pair : calls_) {
    AsyncBatchCall* call = pair.second;
    assert(call != nullptr);
    while (call->pending_requests > 0)
      QueueNext();
  }
}

Status WriteBatch::WriteBatchImpl::GetStatus() {
  // Check the statuses and return the first that's not OK
  for (auto pair : calls_) {
    AsyncBatchCall* call = pair.second;
    assert(call != nullptr);
    call->stream = nullptr;
    if (call->status.ok()) {
      if (call->response.status() != rocksdb::StatusCode::OK)
        return Status(call->response.status());
    } else {
      // FIXME: Possible when a call was started, no
      // shards were referenced, and the node shut down.
      if (call->status.error_code() == grpc::StatusCode::UNAVAILABLE) {
        std::cerr << "Ignoring UNAVAILABLE gRPC status" << std::endl;
        continue;
      }
      return Status(call->status);
    }
  }
  return Status();
}

}  // namespace crocks
