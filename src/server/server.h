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

#ifndef CROCKS_SERVER_SERVER_H
#define CROCKS_SERVER_SERVER_H

#include <string>

#include <grpc++/grpc++.h>
#include <rocksdb/db.h>
#include <rocksdb/options.h>

#include "gen/crocks.grpc.pb.h"
#include "gen/crocks.pb.h"

namespace crocks {

class Service final : public pb::RPC::Service {
 public:
  Service(const std::string& dbpath);
  Service(rocksdb::Options options, const std::string& dbpath);
  ~Service();

  grpc::Status Get(grpc::ServerContext* context, const pb::Key* request,
                   pb::Response* response) override;

  grpc::Status Put(grpc::ServerContext* context, const pb::KeyValue* request,
                   pb::Response* response) override;

  grpc::Status Delete(grpc::ServerContext* context, const pb::Key* request,
                      pb::Response* response) override;

  grpc::Status SingleDelete(grpc::ServerContext* context,
                            const pb::Key* request,
                            pb::Response* response) override;

  grpc::Status Merge(grpc::ServerContext* context, const pb::KeyValue* request,
                     pb::Response* response) override;

  grpc::Status Batch(grpc::ServerContext* context,
                     grpc::ServerReader<pb::BatchBuffer>* reader,
                     pb::Response* response) override;

 private:
  rocksdb::Options options_;
  rocksdb::DB* db_;
  std::string dbpath_;
};

}  // namespace crocks

#endif  // CROCKS_SERVER_SERVER_H
