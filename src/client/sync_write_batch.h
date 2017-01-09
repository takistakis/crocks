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

#ifndef CROCKS_CLIENT_SYNC_WRITE_BATCH_H
#define CROCKS_CLIENT_SYNC_WRITE_BATCH_H

#include <memory>
#include <string>

#include <crocks/status.h>
#include "gen/crocks.grpc.pb.h"

namespace crocks {

class Node;

// Streaming and buffered
class SyncWriteBatch {
 public:
  SyncWriteBatch(Node* db);

  void Put(const std::string& key, const std::string& value);
  void Delete(const std::string& key);
  void SingleDelete(const std::string& key);
  void Merge(const std::string& key, const std::string& value);
  void Clear();
  Status Write();

 private:
  void StreamIfExceededMax();
  void StreamIfExceededThreshold();
  void Stream();

  Node* db_;
  pb::BatchBuffer request_;
  pb::Response response_;
  grpc::ClientContext context_;
  int byte_size_;
  bool writing_;
  std::unique_ptr<grpc::ClientWriter<pb::BatchBuffer>> writer_;
};

}  // namespace crocks

#endif  // CROCKS_CLIENT_SYNC_WRITE_BATCH_H
