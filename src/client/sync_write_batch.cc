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

#include "src/client/sync_write_batch.h"

#include "src/client/node.h"

namespace crocks {

SyncWriteBatch::SyncWriteBatch(Node* db)
    : db_(db), byte_size_(0), writing_(false) {}

void SyncWriteBatch::Put(const std::string& key, const std::string& value) {
  pb::BatchUpdate* batch_update = request_.add_updates();
  batch_update->set_op(pb::BatchUpdate::PUT);
  batch_update->set_key(key);
  batch_update->set_value(value);
  byte_size_ += batch_update->ByteSize() + 3;
  StreamIfExceededThreshold();
}

void SyncWriteBatch::Delete(const std::string& key) {
  pb::BatchUpdate* batch_update = request_.add_updates();
  batch_update->set_op(pb::BatchUpdate::DELETE);
  batch_update->set_key(key);
  byte_size_ += batch_update->ByteSize() + 3;
  StreamIfExceededThreshold();
}

void SyncWriteBatch::SingleDelete(const std::string& key) {
  pb::BatchUpdate* batch_update = request_.add_updates();
  batch_update->set_op(pb::BatchUpdate::SINGLE_DELETE);
  batch_update->set_key(key);
  byte_size_ += batch_update->ByteSize() + 3;
  StreamIfExceededThreshold();
}

void SyncWriteBatch::Merge(const std::string& key, const std::string& value) {
  pb::BatchUpdate* batch_update = request_.add_updates();
  batch_update->set_op(pb::BatchUpdate::MERGE);
  batch_update->set_key(key);
  batch_update->set_value(value);
  byte_size_ += batch_update->ByteSize() + 3;
  StreamIfExceededThreshold();
}

void SyncWriteBatch::Clear() {
  request_.Clear();
  pb::BatchUpdate* batch_update = request_.add_updates();
  batch_update->set_op(pb::BatchUpdate::CLEAR);
  byte_size_ += batch_update->ByteSize() + 3;
}

Status SyncWriteBatch::Write() {
  Stream();
  writer_->WritesDone();
  grpc::Status status = writer_->Finish();

  writing_ = false;
  return Status(status, response_.status());
}

// XXX: Deprecated in favor of SyncWriteBatch::StreamIfExceededThreshold().
// Do not use. Doesn't even work. Kept here as a reference for the operations on
// repeated protobuf fields.
void SyncWriteBatch::StreamIfExceededMax() {
  // We have already added a new BatchUpdate to the buffer. If the new buffer
  // exceeds the maximum byte size, we remove the last update, stream, clear the
  // buffer and re-add the BatchUpdate to the now empty buffer.
  if (byte_size_ <= kMaxByteSize)
    return;
  // This should be true for the method to work but it isn't.
  assert(byte_size_ == request_.ByteSize());
  // .mutable_updates() because .updates() is const and does not allow any
  // modification. Returns RepeatedPtrField<crocks::BatchUpdate>* See:
  // https://developers.google.com/protocol-buffers/docs/reference/cpp/google.protobuf.repeated_field
  // for the available methods.
  auto updates(request_.mutable_updates());
  pb::BatchUpdate* batch_update = updates->ReleaseLast();
  // Do we really need EnsureWriter()?
  // Create a new BatchWriter in the SyncWriteBatch constructor?
  // XXX: The relevant method at the server side starts running when
  // writer_ = db_->BatchWriter() is called and returns upon writer_->Finish().
  // Check if this imposes any scalability problems and consider making it
  // return immediately and somehow associate the batch with an id.
  Stream();
  // NOTE: In RocksDB a committed batch retains all updates after being written
  // and needs to be cleared in order to be reused. However we wouldn't want to
  // keep around buffers that have been streamed, so is seems logical to clear
  // buffers when sending a stream.
  updates->Clear();
  updates->AddAllocated(batch_update);
  byte_size_ = batch_update->ByteSize() + 3;
}

void SyncWriteBatch::StreamIfExceededThreshold() {
  if (byte_size_ <= kByteSizeThreshold)
    return;
  Stream();
  request_.Clear();
  byte_size_ = 0;
}

void SyncWriteBatch::Stream() {
  if (!writing_) {
    writer_ = db_->BatchWriter(&context_, &response_);
    writing_ = true;
  }
  if (!writer_->Write(request_)) {
    std::cerr << "Broken stream" << std::endl;
    exit(EXIT_FAILURE);
  }
}

}  // namespace crocks
