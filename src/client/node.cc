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

#include "src/client/node.h"

#include <grpc++/grpc++.h>

namespace crocks {

Node::Node(std::shared_ptr<grpc::Channel> channel)
    : stub_(pb::RPC::NewStub(channel)) {}

Status Node::Get(const std::string& key, std::string* value) {
  grpc::ClientContext context;
  pb::Key request;
  pb::Response response;

  request.set_key(key);
  grpc::Status status = stub_->Get(&context, request, &response);
  // If status is not OK, value is an empty string
  *value = response.value();
  return Status(status, response.status());
}

Status Node::Put(const std::string& key, const std::string& value) {
  grpc::ClientContext context;
  pb::KeyValue request;
  pb::Response response;

  request.set_key(key);
  request.set_value(value);
  grpc::Status status = stub_->Put(&context, request, &response);
  return Status(status, response.status());
}

Status Node::Delete(const std::string& key) {
  grpc::ClientContext context;
  pb::Key request;
  pb::Response response;

  request.set_key(key);
  grpc::Status status = stub_->Delete(&context, request, &response);
  return Status(status, response.status());
}

Status Node::SingleDelete(const std::string& key) {
  grpc::ClientContext context;
  pb::Key request;
  pb::Response response;

  request.set_key(key);
  grpc::Status status = stub_->SingleDelete(&context, request, &response);
  return Status(status, response.status());
}

Status Node::Merge(const std::string& key, const std::string& value) {
  grpc::ClientContext context;
  pb::KeyValue request;
  pb::Response response;

  request.set_key(key);
  request.set_value(value);
  grpc::Status status = stub_->Merge(&context, request, &response);
  return Status(status, response.status());
}

// For write_batch_buffered
Status Node::Write(const WriteBatchBuffered& batch) {
  grpc::ClientContext context;
  pb::BatchBuffer request = batch.request();
  pb::Response response;

  grpc::Status status = stub_->BatchBuffered(&context, request, &response);
  return Status(status, response.status());
}

// For write_batch_streaming
std::unique_ptr<grpc::ClientWriter<pb::BatchUpdate>> Node::BatchStreamingWriter(
    grpc::ClientContext* context, pb::Response* response) {
  return stub_->BatchStreaming(context, response);
}

// For sync_write_batch
std::unique_ptr<grpc::ClientWriter<pb::BatchBuffer>> Node::BatchWriter(
    grpc::ClientContext* context, pb::Response* response) {
  return stub_->Batch(context, response);
}

// For async write_batch
std::unique_ptr<grpc::ClientAsyncWriter<pb::BatchBuffer>>
Node::AsyncBatchWriter(grpc::ClientContext* context, pb::Response* response,
                       grpc::CompletionQueue* cq, void* tag) {
  return stub_->AsyncBatch(context, response, cq, tag);
}

Node* DBOpen(const std::string& address) {
  return new Node(
      grpc::CreateChannel(address, grpc::InsecureChannelCredentials()));
}

}  // namespace crocks
