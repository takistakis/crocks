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

#include "src/common/util.h"

namespace crocks {

Node::Node(const std::string& address)
    : stub_(pb::RPC::NewStub(
          grpc::CreateChannel(address, grpc::InsecureChannelCredentials()))),
      address_(address) {}

Status Node::Ping() {
  pb::Empty request;
  pb::Empty response;
  grpc::ClientContext context;
  grpc::Status status = stub_->Ping(&context, request, &response);
  return Status(status);
}

Status Node::Get(const std::string& key, std::string* value) {
  pb::Key request;
  pb::Response response;
  request.set_key(key);
  grpc::Status status = Ensure(
      [&](grpc::ClientContext* ctx) {
        return stub_->Get(ctx, request, &response);
      },
      "Node::Get");
  // If status is not OK, value is an empty string
  *value = response.value();
  return Status(status, response.status());
}

Status Node::Put(const std::string& key, const std::string& value) {
  pb::KeyValue request;
  pb::Response response;
  request.set_key(key);
  request.set_value(value);
  grpc::Status status = Ensure(
      [&](grpc::ClientContext* ctx) {
        return stub_->Put(ctx, request, &response);
      },
      "Node::Put");
  return Status(status, response.status());
}

Status Node::Delete(const std::string& key) {
  pb::Key request;
  pb::Response response;
  request.set_key(key);
  grpc::Status status = Ensure(
      [&](grpc::ClientContext* ctx) {
        return stub_->Delete(ctx, request, &response);
      },
      "Node::Delete");
  return Status(status, response.status());
}

Status Node::SingleDelete(const std::string& key) {
  pb::Key request;
  pb::Response response;
  request.set_key(key);
  grpc::Status status = Ensure(
      [&](grpc::ClientContext* ctx) {
        return stub_->SingleDelete(ctx, request, &response);
      },
      "Node::SingleDelete");
  return Status(status, response.status());
}

Status Node::Merge(const std::string& key, const std::string& value) {
  pb::KeyValue request;
  pb::Response response;
  request.set_key(key);
  request.set_value(value);
  grpc::Status status = Ensure(
      [&](grpc::ClientContext* ctx) {
        return stub_->Merge(ctx, request, &response);
      },
      "Node::Merge");
  return Status(status, response.status());
}

// For write_batch
std::unique_ptr<grpc::ClientAsyncReaderWriter<pb::BatchBuffer, pb::Response>>
Node::AsyncBatchStream(grpc::ClientContext* context, grpc::CompletionQueue* cq,
                       void* tag) {
  return stub_->AsyncBatch(context, cq, tag);
}

// For iterator
std::unique_ptr<
    grpc::ClientAsyncReaderWriter<pb::IteratorRequest, pb::IteratorResponse>>
Node::AsyncIteratorStream(grpc::ClientContext* context,
                          grpc::CompletionQueue* cq, void* tag) {
  return stub_->AsyncIterator(context, cq, tag);
}

}  // namespace crocks
