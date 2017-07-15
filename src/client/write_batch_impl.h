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

#ifndef CROCKS_CLIENT_WRITE_BATCH_IMPL_H
#define CROCKS_CLIENT_WRITE_BATCH_IMPL_H

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include <grpc++/grpc++.h>

#include <crocks/status.h>
#include <crocks/write_batch.h>
#include "gen/crocks.pb.h"

namespace crocks {

class Cluster;
class ClusterImpl;

// TODO: Make thresholds configurable
const int kByteSizeThreshold1 = 64 * 1024;    // 64KB
const int kByteSizeThreshold2 = 1024 * 1024;  // 1MB

struct AsyncBatchCall {
  pb::Response response;
  grpc::ClientContext context;
  grpc::Status status;
  int pending_requests = 0;
  std::unique_ptr<grpc::ClientAsyncReaderWriter<pb::BatchBuffer, pb::Response>>
      stream = nullptr;
};

class Buffer {
 public:
  void AddPut(const std::string& key, const std::string& value);
  void AddDelete(const std::string& key);
  void AddSingleDelete(const std::string& key);
  void AddMerge(const std::string& key, const std::string& value);
  void AddClear();

  void Clear();

  void Stream();

  // Resend the first buffer. The call must have been updated
  // to the new master of the shard before calling this.
  void RestreamFirstBuffer();

  void RequestRead();

  int updates_size() const {
    return buffer_.updates_size();
  }

  int ByteSize() const {
    return byte_size_;
  }

  int RealByteSize() const {
    return buffer_.ByteSize();
  }

  const pb::BatchBuffer& get() const {
    return buffer_;
  }

  int shard() const {
    return shard_;
  }

  void set_shard(int shard) {
    shard_ = shard;
  }

  bool first() const {
    return first_;
  }

  void set_first(bool first) {
    first_ = first;
  }

  bool read_requested() const {
    return read_requested_;
  }

  void set_call(AsyncBatchCall* call) {
    call_ = call;
  }

  AsyncBatchCall* call() const {
    return call_;
  }

  bool ok() const {
    return response_.status() == 0;
  }

 private:
  pb::BatchBuffer buffer_;
  // We might need to send the first buffer again to another node
  // if the master of the shard changed so we keep a copy of it.
  pb::BatchBuffer first_buffer_;
  pb::Response response_;
  int byte_size_ = 0;
  int shard_;
  bool first_ = true;
  bool read_requested_ = false;
  AsyncBatchCall* call_ = nullptr;
};

class WriteBatch::WriteBatchImpl {
 public:
  WriteBatchImpl(Cluster* db);
  ~WriteBatchImpl();

  void Put(const std::string& key, const std::string& value);
  void Delete(const std::string& key);
  void SingleDelete(const std::string& key);
  void Merge(const std::string& key, const std::string& value);
  void Clear();
  Status Write();
  Status WriteWithLock();

 private:
  AsyncBatchCall* EnsureBatchCall(const std::string& address);
  Buffer* EnsureBuffer(int shard);
  void QueueNext(bool call = false);
  bool QueueAsyncNext();
  void StreamIfExceededThreshold(int shard);
  void Stream(Buffer* buffer);
  void DoWrite();
  Status GetStatus();

  ClusterImpl* db_;
  grpc::CompletionQueue cq_;
  std::unordered_map<std::string, AsyncBatchCall*> calls_;
  std::vector<Buffer*> buffers_;
};

}  // namespace crocks

#endif  // CROCKS_CLIENT_WRITE_BATCH_IMPL_H
