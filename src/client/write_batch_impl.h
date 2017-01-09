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
#include <vector>

#include <grpc++/grpc++.h>

#include <crocks/status.h>
#include <crocks/write_batch.h>
#include "gen/crocks.pb.h"

namespace crocks {

class Cluster;

struct AsyncBatchCall {
  pb::BatchBuffer request;
  pb::Response response;
  grpc::ClientContext context;
  grpc::Status status;
  int byte_size = 0;
  int pending_requests = 0;
  std::unique_ptr<grpc::ClientAsyncWriter<pb::BatchBuffer>> writer = nullptr;
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

 private:
  AsyncBatchCall* EnsureBatchCall(int idx);
  void QueueNext();
  void StreamIfExceededThreshold(int idx);
  void Stream(int idx);

  Cluster* db_;
  grpc::CompletionQueue cq_;
  std::vector<AsyncBatchCall*> calls_;
};

}  // namespace crocks

#endif  // CROCKS_CLIENT_WRITE_BATCH_IMPL_H
