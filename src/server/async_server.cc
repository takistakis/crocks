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
#include <mutex>
#include <utility>
#include <vector>

#include <grpc++/grpc++.h>
#include <rocksdb/db.h>
#include <rocksdb/status.h>
#include <rocksdb/write_batch.h>

#include <crocks/status.h>
#include "gen/crocks.pb.h"
#include "src/server/iterator.h"
#include "src/server/migrate_util.h"
#include "src/server/shards.h"
#include "src/server/util.h"

std::atomic<bool> shutdown(false);

std::mutex mutex_;

namespace crocks {

const grpc::Status invalid_status(grpc::StatusCode::INVALID_ARGUMENT,
                                  "Not responsible for this shard");

// Simple POD struct used as an argument wrapper for calls
struct CallData {
  pb::RPC::AsyncService* service;
  grpc::ServerCompletionQueue* cq;
  rocksdb::DB* db;
  Info* info;
  Shards* shards;
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

    if (ctx_.IsCancelled() && status_ != FINISH) {
      std::cerr << "Get request cancelled. Finishing." << std::endl;
      responder_.FinishWithError(grpc::Status::CANCELLED, this);
      status_ = FINISH;
      return;
    }

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
        if (request_.force()) {
          s = ForceGet(shard, request_.key(), &value);
          response_.set_status(RocksdbStatusCodeToInt(s.code()));
          response_.set_value(value);
          responder_.Finish(response_, grpc::Status::OK, this);
        } else if (data_->info->WrongShard(shard)) {
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
        delete this;
        break;
    }
  }

  rocksdb::Status Get(int shard_id, const std::string& key,
                      std::string* value) {
    rocksdb::Status s;
    Shard* shard = data_->shards->at(shard_id);

    {
      std::lock_guard<std::mutex> lock(mutex_);
      if (!shard->importing())
        return shard->Get(key, value);
      // I'm the master of the shard but not have ingested all of it yet
      s = shard->GetFromBatch(key, value);
      if (s.ok())
        return s;
      assert(s.IsNotFound());
    }

    std::cerr << data_->info->id() << ": Importing " << shard_id
              << ". Key not in the batch. Asking the former master."
              << std::endl;

    // TODO: This should happen in the background
    s = RequestForceGet(shard->old_address(), key, value);

    // If he replied with invalid argument, we must have ingested by now
    if (s.IsInvalidArgument()) {
      std::lock_guard<std::mutex> lock(mutex_);
      assert(!shard->importing());
      std::cerr << data_->info->id()
                << ": While waiting for RequestForceGet to return, importing "
                << shard_id << " finished." << std::endl;
      s = shard->Get(key, value);
    }
    return s;
  }

  rocksdb::Status ForceGet(int shard_id, const std::string& key,
                           std::string* value) {
    int id = data_->info->id();
    std::cerr << id << ": ForceGet request for shard " << shard_id << std::endl;
    std::lock_guard<std::mutex> lock(mutex_);

    if (!data_->shards->has(shard_id)) {
      std::cerr << id << ": Shard " << shard_id << " already deleted."
                << std::endl;
      return rocksdb::Status::InvalidArgument("Shard already deleted");
    }

    Shard* shard = data_->shards->at(shard_id);
    assert(!shard->importing());
    return shard->Get(key, value);
  }

  rocksdb::Status RequestForceGet(const std::string& address,
                                  const std::string& key, std::string* value) {
    std::unique_ptr<pb::RPC::Stub> stub(pb::RPC::NewStub(
        grpc::CreateChannel(address, grpc::InsecureChannelCredentials())));
    grpc::ClientContext context;
    pb::Key request;
    pb::Response response;
    request.set_key(key);
    request.set_force(true);

    int id = data_->info->id();
    grpc::Status status = stub->Get(&context, request, &response);

    // If gRPC failed, the server must have shut down and if
    // RocksDB status is INVALID_ARGUMENT, he has deleted
    // the shard. Either way, we return INVALID_ARGUMENT.
    if (!status.ok()) {
      std::cerr << data_->info->id() << ": He had shut down" << std::endl;
      return rocksdb::Status::InvalidArgument("Shard already deleted");
    }

    if (response.status() == 4) {
      std::cerr << id << ": He had deleted the shard" << std::endl;
      return rocksdb::Status::InvalidArgument("Shard already deleted");
    }

    *value = response.value();

    if (response.status() == 0) {
      std::cerr << id << ": Got " << *value << std::endl;
      return rocksdb::Status::OK();

    } else if (response.status() == 1) {
      std::cerr << id << ": Not found" << std::endl;
      return rocksdb::Status::NotFound("Not found");

    } else {
      std::cerr << id << ": Another status" << std::endl;
      return rocksdb::Status::OK();
    }
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

    if (ctx_.IsCancelled() && status_ != FINISH) {
      std::cerr << "Put request cancelled. Finishing." << std::endl;
      responder_.FinishWithError(grpc::Status::CANCELLED, this);
      status_ = FINISH;
      return;
    }

    switch (status_) {
      case REQUEST:
        new PutCall(data_);
        if (!ok) {
          std::cerr << "Put in REQUEST was not ok. Finishing." << std::endl;
          responder_.FinishWithError(grpc::Status::CANCELLED, this);
          status_ = FINISH;
          break;
        }
        {
          std::lock_guard<std::mutex> lock(mutex_);
          int shard = data_->info->ShardForKey(request_.key());
          if (data_->info->WrongShard(shard)) {
            responder_.FinishWithError(invalid_status, this);
          } else {
            s = data_->shards->at(shard)->Put(request_.key(), request_.value());
            response_.set_status(RocksdbStatusCodeToInt(s.code()));
            responder_.Finish(response_, grpc::Status::OK, this);
          }
        }
        status_ = FINISH;
        break;

      case FINISH:
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

    if (ctx_.IsCancelled() && status_ != FINISH) {
      std::cerr << "Delete request cancelled. Finishing." << std::endl;
      responder_.FinishWithError(grpc::Status::CANCELLED, this);
      status_ = FINISH;
      return;
    }

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
          s = data_->shards->at(shard)->Delete(request_.key());
          response_.set_status(RocksdbStatusCodeToInt(s.code()));
          responder_.Finish(response_, grpc::Status::OK, this);
        }
        status_ = FINISH;
        break;

      case FINISH:
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

    if (ctx_.IsCancelled() && status_ != FINISH) {
      std::cerr << "Batch request cancelled. Finishing." << std::endl;
      reader_.FinishWithError(grpc::Status::CANCELLED, this);
      status_ = FINISH;
      return;
    }

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
            int shard_id = data_->info->ShardForKey(batch_update.key());
            // TODO: Check if this works
            if (data_->info->WrongShard(shard_id)) {
              reader_.FinishWithError(invalid_status, this);
              status_ = FINISH;
            }
            Shard* shard = data_->shards->at(shard_id);
            rocksdb::ColumnFamilyHandle* cf = shard->cf();
            if (shard->importing())
              std::cerr << "Not implemented" << std::endl;
            // ApplyBatchUpdate(data_->newer->at(shard_id)->GetWriteBatch(), cf,
            //                  batch_update);
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
    if (ctx_.IsCancelled() && status_ != FINISH) {
      std::cerr << "Iterator request cancelled. Finishing." << std::endl;
      stream_.Finish(grpc::Status::CANCELLED, this);
      status_ = FINISH;
      return;
    }

    std::vector<rocksdb::ColumnFamilyHandle*> column_families;

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
        it_ = new MultiIterator(data_->db, data_->shards->ColumnFamilies());
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

    if (ctx_.IsCancelled() && status_ != FINISH) {
      std::cerr << "Migrate request cancelled. Finishing." << std::endl;
      writer_.Finish(grpc::Status::CANCELLED, this);
      status_ = FINISH;
      return;
    }

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
        migrator_->DumpShard(data_->shards->at(request_.shard())->cf());
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
        data_->shards->Remove(request_.shard());
        if (data_->shards->empty()) {
          data_->info->Remove();
          shutdown.store(true);
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
  delete shards_;
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
  shards_ = new Shards(db_, info_.shards());
  call_ = info_.Watch();
  // Create a thread that watches the "info" key and repeatedly
  // reads for updates. Gets cleaned up by the destructor.
  watcher_ = std::thread(&AsyncServer::WatchThread, this);
  std::cerr << "Asynchronous server listening on port " << port << std::endl;
}

void AsyncServer::Run() {
  CallData data{&service_, cq_.get(), db_, &info_, shards_};
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

void AsyncServer::WatchThread() {
  while (!info_.WatchNext(call_)) {
    if (info_.IsMigrating() && info_.NoMigrations())
      info_.Run();

    // If removed, the server cancels the watch so we keep
    // continuing and calling WatchNext until it returns true.
    if (info_.id() == -1)
      continue;

    if (info_.future().empty())
      continue;

    for (const auto& task : info_.Tasks()) {
      std::string address = task.first;
      for (int shard_id : task.second) {
        Shard* shard;
        {
          std::lock_guard<std::mutex> lock(mutex_);
          shard = shards_->Add(shard_id, address);
        }

        pb::MigrateRequest request;
        pb::MigrateResponse response;
        grpc::ClientContext context;
        std::unique_ptr<pb::RPC::Stub> stub(pb::RPC::NewStub(
            grpc::CreateChannel(address, grpc::InsecureChannelCredentials())));

        // Send a request for the shard
        request.set_shard(shard_id);
        std::unique_ptr<grpc::ClientReaderInterface<pb::MigrateResponse>>
            reader = stub->Migrate(&context, request);

        // Once the old master gets the request, he is is supposed to
        // pass ownership to us by informing etcd. We wait for that, so
        // that we can start serving requests for that shard immediately.
        while (info_.IndexForShard(shard_id) != info_.id()) {
          bool ret = info_.WatchNext(call_);
          assert(!ret);
        }

        ShardImporter importer(db_, shard_id);
        while (reader->Read(&response))
          importer.WriteChunk(response);
        EnsureRpc(reader->Finish());

        {
          std::lock_guard<std::mutex> lock(mutex_);
          std::vector<std::string> files = importer.Files();
          shard->Ingest(files);
          if (files.empty())
            std::cerr << info_.id() << ": Shard " << shard_id << " was empty"
                      << std::endl;
          else
            std::cerr << info_.id() << ": Imported shard " << shard_id
                      << std::endl;
        }
      }
    }

    if (info_.IsMigrating() && info_.NoMigrations())
      info_.Run();
  }
}

}  // namespace crocks
