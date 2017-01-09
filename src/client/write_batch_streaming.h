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

#ifndef CROCKS_CLIENT_WRITE_BATCH_STREAMING_H
#define CROCKS_CLIENT_WRITE_BATCH_STREAMING_H

#include <memory>
#include <string>

#include <grpc++/grpc++.h>

#include <crocks/status.h>
#include "gen/crocks.pb.h"

namespace crocks {

class Node;

class WriteBatchStreaming {
 public:
  WriteBatchStreaming(Node* db);

  void Put(const std::string& key, const std::string& value);
  void Delete(const std::string& key);
  void SingleDelete(const std::string& key);
  void Merge(const std::string& key, const std::string& value);
  void Clear();
  Status Write();

 private:
  void Stream(pb::BatchUpdate batch_update);

  bool writing_;
  Node* db_;
  grpc::ClientContext context_;
  pb::Response response_;
  std::unique_ptr<grpc::ClientWriter<pb::BatchUpdate>> writer_;
};

}  // namespace crocks

#endif  // CROCKS_CLIENT_WRITE_BATCH_STREAMING_H
