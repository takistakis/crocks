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

#ifndef CROCKS_SERVER_ASYNC_SERVER_H
#define CROCKS_SERVER_ASYNC_SERVER_H

#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <rocksdb/options.h>

#include "gen/crocks.grpc.pb.h"
#include "src/common/info.h"

namespace grpc {
class Server;
class ServerCompletionQueue;
class Status;
}  // namespace grpc

namespace rocksdb {
class ColumnFamilyHandle;
class DB;
}  // namespace rocksdb

namespace crocks {

class Shards;
class ShardImporter;

class AsyncServer final {
 public:
  AsyncServer(const std::string& etcd_address, const std::string& dbpath,
              const std::string& options_path, int num_threads);
  ~AsyncServer();

  // Start listening for incoming client connections, announce server to
  // etcd, open RocksDB database, and start watching etcd for changes
  void Init(const std::string& listening_address, const std::string& hostname);

  void Run();

 private:
  void ServeThread(int i);
  void WatchThread();
  void MigrationOver(ShardImporter& importer, int shard_id);
  void HandleError(const grpc::Status& status, int node_id);

  std::string dbpath_;
  pb::RPC::AsyncService service_;
  std::unique_ptr<grpc::Server> server_;
  std::vector<std::unique_ptr<grpc::ServerCompletionQueue>> cqs_;
  std::unique_ptr<grpc::ServerCompletionQueue> migrate_cq_;
  rocksdb::DB* db_;
  rocksdb::Options options_;
  rocksdb::ColumnFamilyHandle* default_cf_ = nullptr;
  Info info_;
  Shards* shards_;
  void* call_ = nullptr;
  std::thread watcher_;
  int num_threads_;
};

}  // namespace crocks

#endif  // CROCKS_SERVER_ASYNC_SERVER_H
