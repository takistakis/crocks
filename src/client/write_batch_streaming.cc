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

#include "src/client/write_batch_streaming.h"

#include <stdlib.h>

#include <iostream>

#include <crocks/status.h>
#include "src/client/node.h"

namespace crocks {

// WriteBatchStreaming
WriteBatchStreaming::WriteBatchStreaming(Node* db) : writing_(false), db_(db) {}

void WriteBatchStreaming::Put(const std::string& key,
                              const std::string& value) {
  pb::BatchUpdate batch_update;
  batch_update.set_op(pb::BatchUpdate::PUT);
  batch_update.set_key(key);
  batch_update.set_value(value);
  Stream(batch_update);
}

void WriteBatchStreaming::Delete(const std::string& key) {
  pb::BatchUpdate batch_update;
  batch_update.set_op(pb::BatchUpdate::DELETE);
  batch_update.set_key(key);
  Stream(batch_update);
}

void WriteBatchStreaming::SingleDelete(const std::string& key) {
  pb::BatchUpdate batch_update;
  batch_update.set_op(pb::BatchUpdate::SINGLE_DELETE);
  batch_update.set_key(key);
  Stream(batch_update);
}

void WriteBatchStreaming::Merge(const std::string& key,
                                const std::string& value) {
  pb::BatchUpdate batch_update;
  batch_update.set_op(pb::BatchUpdate::MERGE);
  batch_update.set_key(key);
  batch_update.set_value(value);
  Stream(batch_update);
}

void WriteBatchStreaming::Clear() {
  pb::BatchUpdate batch_update;
  batch_update.set_op(pb::BatchUpdate::CLEAR);
  Stream(batch_update);
}

Status WriteBatchStreaming::Write() {
  writer_->WritesDone();
  grpc::Status status = writer_->Finish();

  writing_ = false;
  return Status(status, response_.status());
}

void WriteBatchStreaming::Stream(pb::BatchUpdate batch_update) {
  if (!writing_) {
    writer_ = db_->BatchStreamingWriter(&context_, &response_);
    writing_ = true;
  }
  // WriteOptions().set_buffer_hint() buffers messages and sends them in
  // batches.
  if (!writer_->Write(batch_update, grpc::WriteOptions().set_buffer_hint())) {
    std::cerr << "Broken stream" << std::endl;
    exit(EXIT_FAILURE);
  }
}

}  // namespace crocks
