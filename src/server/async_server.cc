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

#include "src/server/async_server.h"

#include <assert.h>
#include <stdlib.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <iostream>
#include <utility>
#include <vector>

#include <grpc++/grpc++.h>
#include <rocksdb/db.h>
#include <rocksdb/status.h>
#include <rocksdb/write_batch.h>

#include "gen/crocks.pb.h"
#include "src/server/iterator.h"
#include "src/server/migrate_util.h"
#include "src/server/util.h"

std::atomic<bool> shutdown(false);

namespace crocks {

const grpc::Status invalid_status(grpc::StatusCode::INVALID_ARGUMENT,
                                  "Not responsible for this shard");

// Simple POD struct used as an argument wrapper for calls
struct CallData {
  pb::RPC::AsyncService* service;
  grpc::ServerCompletionQueue* cq;
  rocksdb::DB* db;
  std::unordered_map<int, rocksdb::ColumnFamilyHandle*>* cfs;
  Info* info;
};

// Base class used to cast the void* tags we get from
// the completion queue and call Proceed() on them.
class Call {
 public:
  virtual void Proceed(bool ok) = 0;
  virtual void Delete() = 0;
};

class GetCall final : public Call {
 public:
  explicit GetCall(CallData* data)
      : data_(data), responder_(&ctx_), status_(REQUEST) {
    data->service->RequestGet(&ctx_, &request_, &responder_, data_->cq,
                              data_->cq, this);
  }

  void Proceed(bool ok) {
    rocksdb::Status s;
    std::string value;
    int shard;

    switch (status_) {
      case REQUEST:
        new GetCall(data_);
        if (!ok) {
          std::cerr << "Get in REQUEST was not ok. Finishing." << std::endl;
          responder_.FinishWithError(grpc::Status::CANCELLED, this);
          status_ = FINISH;
          break;
        }
        shard = data_->info->ShardForKey(request_.key());
        if (data_->info->WrongShard(shard)) {
          responder_.FinishWithError(invalid_status, this);
        } else {
          s = Get(shard, request_.key(), &value);
          response_.set_status(RocksdbStatusCodeToInt(s.code()));
          response_.set_value(value);
          responder_.Finish(response_, grpc::Status::OK, this);
        }
        status_ = FINISH;
        break;

      case FINISH:
        if (!ok)
          std::cerr << "Get RPC finished unexpectedly" << std::endl;
        delete this;
        break;
    }
  }

  rocksdb::Status Get(int shard, const std::string& key, std::string* value) {
    rocksdb::Status s;
    rocksdb::ColumnFamilyHandle* cf = data_->cfs->at(shard);
    if (data_->info->IsImporting(shard))
      std::cerr << data_->info->id() << ": Get request for shard " << shard
                << " while importing. Undefined behavior." << std::endl;
    else
      s = data_->db->Get(rocksdb::ReadOptions(), cf, key, value);
    return s;
  }

  void Delete() {
    delete this;
  }

 private:
  CallData* data_;
  grpc::ServerContext ctx_;
  grpc::ServerAsyncResponseWriter<pb::Response> responder_;
  pb::Key request_;
  pb::Response response_;
  enum CallStatus { REQUEST, FINISH };
  CallStatus status_;
};

class PutCall final : public Call {
 public:
  explicit PutCall(CallData* data)
      : data_(data), responder_(&ctx_), status_(REQUEST) {
    data_->service->RequestPut(&ctx_, &request_, &responder_, data_->cq,
                               data_->cq, this);
  }

  void Proceed(bool ok) {
    rocksdb::Status s;
    int shard;

    switch (status_) {
      case REQUEST:
        new PutCall(data_);
        if (!ok) {
          std::cerr << "Put in REQUEST was not ok. Finishing." << std::endl;
          responder_.FinishWithError(grpc::Status::CANCELLED, this);
          status_ = FINISH;
          break;
        }
        shard = data_->info->ShardForKey(request_.key());
        if (data_->info->WrongShard(shard)) {
          responder_.FinishWithError(invalid_status, this);
        } else {
          Put(shard, request_.key(), request_.value());
          response_.set_status(RocksdbStatusCodeToInt(s.code()));
          responder_.Finish(response_, grpc::Status::OK, this);
        }
        status_ = FINISH;
        break;

      case FINISH:
        if (!ok)
          std::cerr << "Put RPC finished unexpectedly" << std::endl;
        delete this;
        break;
    }
  }

  rocksdb::Status Put(int shard, const std::string& key,
                      const std::string& value) {
    rocksdb::Status s;
    rocksdb::ColumnFamilyHandle* cf = data_->cfs->at(shard);
    if (data_->info->IsImporting(shard))
      std::cerr << data_->info->id() << ": Put request for shard " << shard
                << " while importing. Undefined behavior." << std::endl;
    else
      s = data_->db->Put(rocksdb::WriteOptions(), cf, key, value);
    return s;
  }

  void Delete() {
    delete this;
  }

 private:
  CallData* data_;
  grpc::ServerContext ctx_;
  grpc::ServerAsyncResponseWriter<pb::Response> responder_;
  pb::KeyValue request_;
  pb::Response response_;
  enum CallStatus { REQUEST, FINISH };
  CallStatus status_;
};

class DeleteCall final : public Call {
 public:
  explicit DeleteCall(CallData* data)
      : data_(data), responder_(&ctx_), status_(REQUEST) {
    data_->service->RequestDelete(&ctx_, &request_, &responder_, data_->cq,
                                  data_->cq, this);
  }

  void Proceed(bool ok) {
    rocksdb::Status s;
    int shard;

    switch (status_) {
      case REQUEST:
        new DeleteCall(data_);
        if (!ok) {
          std::cerr << "Delete in REQUEST was not ok. Finishing." << std::endl;
          responder_.FinishWithError(grpc::Status::CANCELLED, this);
          status_ = FINISH;
          break;
        }
        shard = data_->info->ShardForKey(request_.key());
        if (data_->info->WrongShard(shard)) {
          responder_.FinishWithError(invalid_status, this);
        } else {
          Delete(shard, request_.key());
          response_.set_status(RocksdbStatusCodeToInt(s.code()));
          responder_.Finish(response_, grpc::Status::OK, this);
        }
        status_ = FINISH;
        break;

      case FINISH:
        if (!ok)
          std::cerr << "Delete RPC finished unexpectedly" << std::endl;
        delete this;
        break;
    }
  }

  rocksdb::Status Delete(int shard, const std::string& key) {
    rocksdb::Status s;
    rocksdb::ColumnFamilyHandle* cf = data_->cfs->at(shard);
    if (data_->info->IsImporting(shard))
      std::cerr << data_->info->id() << ": Delete request for shard " << shard
                << " while importing. Undefined behavior." << std::endl;
    else
      s = data_->db->Delete(rocksdb::WriteOptions(), cf, key);
    return s;
  }

  void Delete() {
    delete this;
  }

 private:
  CallData* data_;
  grpc::ServerContext ctx_;
  grpc::ServerAsyncResponseWriter<pb::Response> responder_;
  pb::Key request_;
  pb::Response response_;
  enum CallStatus { REQUEST, FINISH };
  CallStatus status_;
};

// TODO: Add SingleDelete() and Merge()

class BatchCall final : public Call {
 public:
  explicit BatchCall(CallData* data)
      : data_(data), reader_(&ctx_), status_(REQUEST) {
    data_->service->RequestBatch(&ctx_, &reader_, data_->cq, data_->cq, this);
  }

  void Proceed(bool ok) {
    rocksdb::Status s;
    int shard;

    switch (status_) {
      case REQUEST:
        new BatchCall(data_);
        if (!ok) {
          std::cerr << "Batch in REQUEST was not ok. Finishing." << std::endl;
          reader_.FinishWithError(grpc::Status::CANCELLED, this);
          status_ = FINISH;
          break;
        }
        reader_.Read(&request_, this);
        status_ = READ;
        assert(request_.updates_size() == 0);
        break;

      case READ:
        // This read must be done even if ok is false
        reader_.Read(&request_, this);
        if (ok) {
          for (const pb::BatchUpdate& batch_update : request_.updates()) {
            shard = data_->info->ShardForKey(batch_update.key());
            // TODO: Check if this works
            if (data_->info->WrongShard(shard)) {
              reader_.FinishWithError(invalid_status, this);
              status_ = FINISH;
            }
            rocksdb::ColumnFamilyHandle* cf = data_->cfs->at(shard);
            if (data_->info->IsImporting(shard))
              std::cerr << data_->info->id() << ": Batch request for shard "
                        << shard << " while importing. Undefined behavior."
                        << std::endl;
            else
              ApplyBatchUpdate(&batch_, cf, batch_update);
          }
        } else {
          status_ = DONE;
        }
        break;

      case DONE:
        assert(!ok);
        s = data_->db->Write(rocksdb::WriteOptions(), &batch_);
        response_.set_status(RocksdbStatusCodeToInt(s.code()));
        reader_.Finish(response_, grpc::Status::OK, this);
        status_ = FINISH;
        break;

      case FINISH:
        if (!ok)
          std::cerr << "Batch RPC finished unexpectedly" << std::endl;
        delete this;
        break;
    }
  }

  void Delete() {
    delete this;
  }

 private:
  CallData* data_;
  grpc::ServerContext ctx_;
  grpc::ServerAsyncReader<pb::Response, pb::BatchBuffer> reader_;
  pb::BatchBuffer request_;
  pb::Response response_;
  enum CallStatus { REQUEST, READ, DONE, FINISH };
  CallStatus status_;
  rocksdb::WriteBatch batch_;
};

class IteratorCall final : public Call {
 public:
  explicit IteratorCall(CallData* data)
      : data_(data), stream_(&ctx_), status_(REQUEST) {
    data_->service->RequestIterator(&ctx_, &stream_, data_->cq, data_->cq,
                                    this);
  }

  void Proceed(bool ok) {
    switch (status_) {
      case REQUEST:
        new IteratorCall(data_);
        if (!ok) {
          std::cerr << "Iterator in REQUEST was not ok. Finishing."
                    << std::endl;
          stream_.Finish(grpc::Status::CANCELLED, this);
          status_ = FINISH;
          break;
        }
        it_ = new MultiIterator(data_->db, *(data_->cfs));
        stream_.Read(&request_, this);
        status_ = READ;
        break;

      case READ:
        if (ok) {
          response_.Clear();
          ApplyIteratorRequest(it_, request_, &response_);
          stream_.Write(response_, this);
          status_ = WRITE;
        } else {
          stream_.Finish(grpc::Status::OK, this);
          status_ = FINISH;
        }
        break;

      case WRITE:
        if (ok) {
          stream_.Read(&request_, this);
          status_ = READ;
        } else {
          stream_.Finish(grpc::Status::OK, this);
          status_ = FINISH;
        }
        break;

      case FINISH:
        if (!ok)
          std::cerr << "Iterator RPC finished unexpectedly" << std::endl;
        delete it_;
        delete this;
        break;
    }
  }

  void Delete() {
    delete this;
  }

 private:
  CallData* data_;
  grpc::ServerContext ctx_;
  grpc::ServerAsyncReaderWriter<pb::IteratorResponse, pb::IteratorRequest>
      stream_;
  pb::IteratorRequest request_;
  pb::IteratorResponse response_;
  enum CallStatus { REQUEST, READ, WRITE, FINISH };
  CallStatus status_;
  MultiIterator* it_ = nullptr;
};

class MigrateCall final : public Call {
 public:
  explicit MigrateCall(CallData* data)
      : data_(data), writer_(&ctx_), status_(REQUEST) {
    data_->service->RequestMigrate(&ctx_, &request_, &writer_, data_->cq,
                                   data_->cq, this);
  }

  void Proceed(bool ok) {
    rocksdb::Status s;
    pb::MigrateResponse response;
    bool retval;

    switch (status_) {
      case REQUEST:
        new MigrateCall(data_);
        if (!ok) {
          std::cerr << "Migrate in REQUEST was not ok. Finishing." << std::endl;
          writer_.Finish(grpc::Status::CANCELLED, this);
          status_ = FINISH;
          break;
        }

        std::cerr << data_->info->id() << ": Migrating shard "
                  << request_.shard() << std::endl;
        data_->info->GiveShard(request_.shard());
        migrator_ = std::unique_ptr<ShardMigrator>(
            new ShardMigrator(data_->db, request_.shard()));
        cf_ = data_->cfs->at(request_.shard());
        migrator_->DumpShard(cf_);
        retval = migrator_->ReadChunk(&response);
        assert(retval);
        writer_.Write(response, this);
        status_ = WRITE;
        break;

      case WRITE:
        if (migrator_->ReadChunk(&response)) {
          writer_.Write(response, this);
          status_ = WRITE;
        } else {
          writer_.Finish(grpc::Status::OK, this);
          status_ = FINISH;
        }
        break;

      case FINISH:
        if (!ok)
          std::cerr << "Migrate RPC finished unexpectedly" << std::endl;

        data_->db->DropColumnFamily(cf_);
        delete cf_;
        data_->cfs->erase(request_.shard());

        if (data_->info->Shards().size() == 0) {
          if (data_->cfs->size() == 0) {
            data_->info->Remove();
            // std::cerr << "Done. Press ^C to exit." << std::endl;
            shutdown.store(true);
          }
        }

        delete this;
        break;
    }
  }

  void Delete() {
    delete this;
  }

 private:
  CallData* data_;
  grpc::ServerContext ctx_;
  grpc::ServerAsyncWriter<pb::MigrateResponse> writer_;
  pb::MigrateRequest request_;
  pb::Response response_;
  enum CallStatus { REQUEST, WRITE, FINISH };
  CallStatus status_;
  std::unique_ptr<ShardMigrator> migrator_;
  rocksdb::ColumnFamilyHandle* cf_;
};

AsyncServer::AsyncServer(const std::string& etcd_address,
                         const std::string& dbpath)
    : dbpath_(dbpath), options_(DefaultRocksdbOptions()), info_(etcd_address) {
  rocksdb::Status s = rocksdb::DB::Open(options_, dbpath_, &db_);
  EnsureRocksdb("Open", s);
}

AsyncServer::~AsyncServer() {
  std::cerr << "Shutting down..." << std::endl;
  server_->Shutdown(std::chrono::system_clock::now());
  cq_->Shutdown();
  void* tag;
  bool ok;
  while (cq_->Next(&tag, &ok))
    static_cast<Call*>(tag)->Delete();
  info_.WatchCancel(call_);
  watcher_.join();
  for (const auto& pair : cfs_)
    delete pair.second;
  delete db_;
}

void AsyncServer::Init(const std::string& listening_address,
                       const std::string& hostname) {
  grpc::ServerBuilder builder;
  int selected_port;
  builder.AddListeningPort(listening_address, grpc::InsecureServerCredentials(),
                           &selected_port);
  builder.RegisterService(&service_);
  cq_ = builder.AddCompletionQueue();
  server_ = builder.BuildAndStart();
  if (selected_port == 0) {
    std::cerr << "Could not bind to a port" << std::endl;
    exit(EXIT_FAILURE);
  }
  std::string port = std::to_string(selected_port);
  std::string node_address = hostname + ":" + port;
  info_.Add(node_address);
  AddColumnFamilies(info_.Shards(), db_, &cfs_);
  call_ = info_.Watch();
  // Create a thread that watches the "info" key and repeatedly
  // reads for updates. Gets cleaned up by the destructor.
  watcher_ = std::thread(WatchThread, &info_, db_, &cfs_, call_);
  std::cerr << "Asynchronous server listening on port " << port << std::endl;
}

void AsyncServer::Run() {
  CallData data{&service_, cq_.get(), db_, &cfs_, &info_};
  new GetCall(&data);
  new PutCall(&data);
  new DeleteCall(&data);
  new BatchCall(&data);
  new IteratorCall(&data);
  new MigrateCall(&data);
  void* tag;
  bool ok;
  for (;;) {
    // For the meaning of the return value of Next, and ok see:
    // https://groups.google.com/d/msg/grpc-io/qtZya6AuGAQ/Umepla-GAAAJ
    // http://www.grpc.io/grpc/cpp/classgrpc_1_1_completion_queue.html
    if (!cq_->Next(&tag, &ok)) {
      std::cerr << "Shutting down..." << std::endl;
      break;
    }
    static_cast<Call*>(tag)->Proceed(ok);
    if (shutdown.load())
      break;
  }
}

}  // namespace crocks
