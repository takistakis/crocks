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

#include "src/server/server.h"

#include <iostream>
#include <thread>
#include <utility>
#include <vector>

#include <rocksdb/status.h>
#include <rocksdb/write_batch.h>

#include "src/server/iterator.h"
#include "src/server/util.h"

namespace crocks {

const grpc::Status invalid_status(grpc::StatusCode::INVALID_ARGUMENT,
                                  "Not responsible for this shard");

Service::Service(const std::string& address, const std::string& dbpath)
    : info_(address), options_(DefaultRocksdbOptions()), dbpath_(dbpath) {
  rocksdb::Status s = rocksdb::DB::Open(options_, dbpath_, &db_);
  EnsureRocksdb("Open", s);
};

Service::~Service() {
  rocksdb::DestroyDB(dbpath_, options_);
  for (const auto& pair : cfs_)
    delete pair.second;
  delete db_;
  info_.WatchCancel(call_);
  watcher_.join();
};

void Service::Init(const std::string& address) {
  info_.Add(address);
  AddColumnFamilies(info_.shards(), db_, &cfs_);
  call_ = info_.Watch();
  // Create a thread that watches the "info" key and repeatedly
  // reads for updates. Gets cleaned up by the destructor.
  watcher_ = std::thread(SyncWatchThread, &info_, call_);
}

grpc::Status Service::Get(grpc::ServerContext* context, const pb::Key* request,
                          pb::Response* response) {
  int shard = info_.ShardForKey(request->key());
  if (info_.WrongShard(shard))
    return invalid_status;
  std::string value;
  rocksdb::Status s =
      db_->Get(rocksdb::ReadOptions(), cfs_[shard], request->key(), &value);
  response->set_status(RocksdbStatusCodeToInt(s.code()));
  response->set_value(value);
  return grpc::Status::OK;
}

grpc::Status Service::Put(grpc::ServerContext* context,
                          const pb::KeyValue* request, pb::Response* response) {
  int shard = info_.ShardForKey(request->key());
  if (info_.WrongShard(shard))
    return invalid_status;
  rocksdb::Status s = db_->Put(rocksdb::WriteOptions(), cfs_[shard],
                               request->key(), request->value());
  response->set_status(RocksdbStatusCodeToInt(s.code()));
  return grpc::Status::OK;
}

grpc::Status Service::Delete(grpc::ServerContext* context,
                             const pb::Key* request, pb::Response* response) {
  int shard = info_.ShardForKey(request->key());
  if (info_.WrongShard(shard))
    return invalid_status;
  rocksdb::Status s =
      db_->Delete(rocksdb::WriteOptions(), cfs_[shard], request->key());
  response->set_status(RocksdbStatusCodeToInt(s.code()));
  return grpc::Status::OK;
}

grpc::Status Service::SingleDelete(grpc::ServerContext* context,
                                   const pb::Key* request,
                                   pb::Response* response) {
  int shard = info_.ShardForKey(request->key());
  if (info_.WrongShard(shard))
    return invalid_status;
  rocksdb::Status s =
      db_->SingleDelete(rocksdb::WriteOptions(), cfs_[shard], request->key());
  response->set_status(RocksdbStatusCodeToInt(s.code()));
  return grpc::Status::OK;
}

grpc::Status Service::Merge(grpc::ServerContext* context,
                            const pb::KeyValue* request,
                            pb::Response* response) {
  int shard = info_.ShardForKey(request->key());
  if (info_.WrongShard(shard))
    return invalid_status;
  rocksdb::Status s = db_->Merge(rocksdb::WriteOptions(), cfs_[shard],
                                 request->key(), request->value());
  response->set_status(RocksdbStatusCodeToInt(s.code()));
  return grpc::Status::OK;
}

grpc::Status Service::Batch(grpc::ServerContext* context,
                            grpc::ServerReader<pb::BatchBuffer>* reader,
                            pb::Response* response) {
  pb::BatchBuffer batch_buffer;
  rocksdb::WriteBatch batch;

  while (reader->Read(&batch_buffer)) {
    for (const pb::BatchUpdate& batch_update : batch_buffer.updates()) {
      int shard = info_.ShardForKey(batch_update.key());
      if (info_.WrongShard(shard))
        return invalid_status;
      ApplyBatchUpdate(&batch, cfs_[shard], batch_update);
    }
  }

  if (context->IsCancelled()) {
    std::cerr << "Batch RPC finished unexpectedly" << std::endl;
  } else {
    rocksdb::Status s = db_->Write(rocksdb::WriteOptions(), &batch);
    response->set_status(RocksdbStatusCodeToInt(s.code()));
  }

  return grpc::Status::OK;
}

grpc::Status Service::Iterator(
    grpc::ServerContext* context,
    grpc::ServerReaderWriter<pb::IteratorResponse, pb::IteratorRequest>*
        stream) {
  pb::IteratorRequest request;
  pb::IteratorResponse response;
  // https://github.com/facebook/rocksdb/wiki/Basic-Operations#iteration
  std::vector<rocksdb::ColumnFamilyHandle*> column_families;
  for (const auto& pair : cfs_)
    column_families.push_back(pair.second);
  MultiIterator* it = new MultiIterator(db_, column_families);
  while (stream->Read(&request)) {
    response.Clear();
    ApplyIteratorRequest(it, request, &response);
    stream->Write(response);
  }
  delete it;

  return grpc::Status::OK;
}

grpc::Status Service::Migrate(
    grpc::ServerContext* context,
    grpc::ServerReaderWriter<pb::MigrateResponse, pb::MigrateRequest>* stream) {
  std::string msg = "The synchronous server does not support migrations";
  std::cerr << msg << std::endl;
  grpc::Status unimplemented(grpc::StatusCode::UNIMPLEMENTED, msg);
  return unimplemented;
}

void SyncWatchThread(Info* info, void* call) {
  for (;;)
    if (info->WatchNext(call))
      return;
}

}  // namespace crocks
