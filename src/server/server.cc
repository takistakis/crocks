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

#include <rocksdb/status.h>
#include <rocksdb/write_batch.h>

#include "src/server/util.h"

namespace crocks {

Service::Service(const std::string& dbpath)
    : options_(DefaultRocksdbOptions()), dbpath_(dbpath) {
  rocksdb::Status s = rocksdb::DB::Open(options_, dbpath_, &db_);
  EnsureRocksdb("Open", s);
};

Service::Service(rocksdb::Options options, const std::string& dbpath)
    : options_(options), dbpath_(dbpath) {
  rocksdb::Status s = rocksdb::DB::Open(options_, dbpath_, &db_);
  EnsureRocksdb("Open", s);
};

Service::~Service() {
  rocksdb::DestroyDB(dbpath_, options_);
  delete db_;
};

grpc::Status Service::Get(grpc::ServerContext* context, const pb::Key* request,
                          pb::Response* response) {
  std::string value;
  rocksdb::Status s = db_->Get(rocksdb::ReadOptions(), request->key(), &value);
  response->set_status(RocksdbStatusCodeToInt(s.code()));
  response->set_value(value);
  return grpc::Status::OK;
}

grpc::Status Service::Put(grpc::ServerContext* context,
                          const pb::KeyValue* request, pb::Response* response) {
  rocksdb::Status s =
      db_->Put(rocksdb::WriteOptions(), request->key(), request->value());
  response->set_status(RocksdbStatusCodeToInt(s.code()));
  return grpc::Status::OK;
}

grpc::Status Service::Delete(grpc::ServerContext* context,
                             const pb::Key* request, pb::Response* response) {
  rocksdb::Status s = db_->Delete(rocksdb::WriteOptions(), request->key());
  response->set_status(RocksdbStatusCodeToInt(s.code()));
  return grpc::Status::OK;
}

grpc::Status Service::SingleDelete(grpc::ServerContext* context,
                                   const pb::Key* request,
                                   pb::Response* response) {
  rocksdb::Status s =
      db_->SingleDelete(rocksdb::WriteOptions(), request->key());
  response->set_status(RocksdbStatusCodeToInt(s.code()));
  return grpc::Status::OK;
}

grpc::Status Service::Merge(grpc::ServerContext* context,
                            const pb::KeyValue* request,
                            pb::Response* response) {
  rocksdb::Status s =
      db_->Merge(rocksdb::WriteOptions(), request->key(), request->value());
  response->set_status(RocksdbStatusCodeToInt(s.code()));
  return grpc::Status::OK;
}

grpc::Status Service::Batch(grpc::ServerContext* context,
                            grpc::ServerReader<pb::BatchBuffer>* reader,
                            pb::Response* response) {
  pb::BatchBuffer batch_buffer;
  rocksdb::WriteBatch batch;

  while (reader->Read(&batch_buffer))
    for (const pb::BatchUpdate& batch_update : batch_buffer.updates())
      ApplyBatchUpdate(&batch, batch_update);

  if (context->IsCancelled()) {
    std::cerr << "Batch RPC finished unexpectedly" << std::endl;
  } else {
    rocksdb::Status s = db_->Write(rocksdb::WriteOptions(), &batch);
    response->set_status(RocksdbStatusCodeToInt(s.code()));
  }

  return grpc::Status::OK;
}

}  // namespace crocks
