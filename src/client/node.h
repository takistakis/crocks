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

#ifndef CROCKS_CLIENT_NODE_H
#define CROCKS_CLIENT_NODE_H

#include <memory>
#include <string>

#include <crocks/status.h>
#include "gen/crocks.grpc.pb.h"
#include "src/client/write_batch_buffered.h"

namespace crocks {

// 4MB is the default message limit for gRPC. However it is
// suggested to break up streaming messages in 16KB - 64KB messages.
// See: https://github.com/grpc/grpc.github.io/issues/371.
// Here we break them in 512KB messages and we'll see.
const int kMaxByteSize = 4 * 1024 * 1024;  // 4MB
// TODO: kByteSizeThreshold should probably be configurable and per batch
const int kByteSizeThreshold = 512 * 1024;  // 512KB

class Node {
 public:
  Node(std::shared_ptr<grpc::Channel> channel);

  Status Get(const std::string& key, std::string* value);
  Status Put(const std::string& key, const std::string& value);
  Status Delete(const std::string& key);
  Status SingleDelete(const std::string& key);
  Status Merge(const std::string& key, const std::string& value);

  // For write_batch_buffered
  Status Write(const WriteBatchBuffered& batch);

  // For write_batch_streaming
  std::unique_ptr<grpc::ClientWriter<pb::BatchUpdate>> BatchStreamingWriter(
      grpc::ClientContext* context, pb::Response* response);

  // For sync_write_batch
  std::unique_ptr<grpc::ClientWriter<pb::BatchBuffer>> BatchWriter(
      grpc::ClientContext* context, pb::Response* response);

  // For async write_batch
  std::unique_ptr<grpc::ClientAsyncWriter<pb::BatchBuffer>> AsyncBatchWriter(
      grpc::ClientContext* context, pb::Response* response,
      grpc::CompletionQueue* cq, void* tag);

 private:
  std::unique_ptr<pb::RPC::Stub> stub_;
};

Node* DBOpen(const std::string& address);

}  // namespace crocks

#endif  // CROCKS_CLIENT_NODE_H
