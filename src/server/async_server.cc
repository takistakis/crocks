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

#include <atomic>
#include <chrono>
#include <iostream>
#include <map>
#include <unordered_map>
#include <utility>

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

namespace crocks {

// gRPC status indicating that the shard belongs to another node
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
    int shard_id;
    bool ask;

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
        shard_id = data_->info->ShardForKey(request_.key());
        if (data_->info->WrongShard(shard_id) && !request_.force()) {
          responder_.FinishWithError(invalid_status, this);
          status_ = FINISH;
          break;
        }
        shard_ = data_->shards->at(shard_id);
        if (!shard_) {
          responder_.FinishWithError(invalid_status, this);
          status_ = FINISH;
          break;
        }
        s = shard_->Get(request_.key(), &value, &ask);
        if (ask) {
          std::cerr << data_->info->id() << ": Asking the former master"
                    << std::endl;
          std::unique_ptr<pb::RPC::Stub> stub(
              pb::RPC::NewStub(grpc::CreateChannel(
                  shard_->old_address(), grpc::InsecureChannelCredentials())));
          request_.set_force(true);
          std::unique_ptr<grpc::ClientAsyncResponseReader<pb::Response>> rpc(
              stub->AsyncGet(&force_get_context_, request_, data_->cq));
          rpc->Finish(&response_, &force_get_status_, this);
          status_ = GET;
          break;
        }
        response_.set_status(RocksdbStatusCodeToInt(s.code()));
        response_.set_value(value);
        responder_.Finish(response_, grpc::Status::OK, this);
        status_ = FINISH;
        break;

      case GET:
        // If gRPC failed, the server must have shut down and if
        // RocksDB status is INVALID_ARGUMENT, he has deleted
        // the shard. Either way, we must have ingested by now.
        if (!force_get_status_.ok() ||
            response_.status() == rocksdb::StatusCode::INVALID_ARGUMENT) {
          std::cerr << data_->info->id() << ": Meanwhile importing finished"
                    << std::endl;
          s = shard_->Get(request_.key(), &value, &ask);
          assert(!ask);
          response_.set_status(RocksdbStatusCodeToInt(s.code()));
          response_.set_value(value);
        }
        // If he responded successfully we just forward his response
        responder_.Finish(response_, grpc::Status::OK, this);
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
  // We need to keep the shared_ptr in scope for the whole
  // lifetime of GetCall to make sure that the shard
  // doesn't get deleted while a get rpc is in progress.
  std::shared_ptr<Shard> shard_;
  grpc::ClientContext force_get_context_;
  grpc::Status force_get_status_;
  enum CallStatus { REQUEST, GET, FINISH };
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
    int shard_id;
    // We need to keep the shared_ptr in scope at least
    // until shard->Ref() is called. If shard->Ref()
    // succeeds we know that the shard won't be deleted.
    std::shared_ptr<Shard> shard;

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
        shard_id = data_->info->ShardForKey(request_.key());
        shard = data_->shards->at(shard_id);
        if (!shard || !shard->Ref()) {
          responder_.FinishWithError(invalid_status, this);
        } else {
          s = shard->Put(request_.key(), request_.value());
          shard->Unref();
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
    int shard_id;
    std::shared_ptr<Shard> shard;

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
        shard_id = data_->info->ShardForKey(request_.key());
        shard = data_->shards->at(shard_id);
        if (!shard || !shard->Ref()) {
          responder_.FinishWithError(invalid_status, this);
        } else {
          s = shard->Delete(request_.key());
          shard->Unref();
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
      : data_(data), stream_(&ctx_), status_(REQUEST) {
    data_->service->RequestBatch(&ctx_, &stream_, data_->cq, data_->cq, this);
  }

  void Proceed(bool ok) {
    rocksdb::Status s;

    if (ctx_.IsCancelled() && status_ != FINISH) {
      std::cerr << "Batch request cancelled. Finishing." << std::endl;
      stream_.Finish(grpc::Status::CANCELLED, this);
      status_ = FINISH;
      return;
    }

    switch (status_) {
      case REQUEST:
        new BatchCall(data_);
        if (!ok) {
          std::cerr << "Batch in REQUEST was not ok. Finishing." << std::endl;
          stream_.Finish(grpc::Status::CANCELLED, this);
          status_ = FINISH;
          break;
        }
        stream_.Read(&request_, this);
        status_ = READ;
        assert(request_.updates_size() == 0);
        break;

      case READ:
        if (ok) {
          int shard_id = data_->info->ShardForKey(request_.updates(0).key());
          std::shared_ptr<Shard> shard = data_->shards->at(shard_id);
          if (!got_ref_[shard_id]) {
            // We got the first buffer
            if (!shard || !shard->Ref()) {
              auto code = rocksdb::Status::Code::kInvalidArgument;
              response_.set_status(RocksdbStatusCodeToInt(code));
              stream_.Write(response_, this);
              status_ = WRITE;
              // break early to avoid applying the buffer
              break;
            } else {
              got_ref_[shard_id] = true;
              auto code = rocksdb::Status::Code::kOk;
              response_.set_status(RocksdbStatusCodeToInt(code));
              stream_.Write(response_, this);
              status_ = WRITE;
            }
          } else {
            stream_.Read(&request_, this);
            assert(status_ == READ);
          }
          for (const pb::BatchUpdate& batch_update : request_.updates())
            ApplyBatchUpdate(&batch_, shard->cf(), batch_update);
        } else {
          s = data_->db->Write(rocksdb::WriteOptions(), &batch_);
          response_.set_status(RocksdbStatusCodeToInt(s.code()));
          stream_.Write(response_, this);
          status_ = WRITE;
          finish_ = true;
        }
        break;

      case WRITE:
        assert(ok);
        if (!finish_) {
          stream_.Read(&request_, this);
          status_ = READ;
        } else {
          stream_.Finish(grpc::Status::OK, this);
          status_ = FINISH;
        }
        break;

      case FINISH:
        // Unreference every referenced shard
        for (auto pair : got_ref_)
          if (pair.second)
            data_->shards->at(pair.first)->Unref();
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
  grpc::ServerAsyncReaderWriter<pb::Response, pb::BatchBuffer> stream_;
  pb::BatchBuffer request_;
  pb::Response response_;
  std::unordered_map<int, bool> got_ref_;
  bool finish_ = false;
  enum CallStatus { REQUEST, READ, WRITE, FINISH };
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
      : data_(data), stream_(&ctx_), status_(REQUEST) {
    data_->service->RequestMigrate(&ctx_, &stream_, data_->cq, data_->cq, this);
  }

  void Proceed(bool ok) {
    rocksdb::Status s;
    pb::MigrateResponse response;
    bool retval;
    int shard_id;
    std::shared_ptr<Shard> shard;

    if (ctx_.IsCancelled() && status_ != FINISH) {
      std::cerr << "Migrate request cancelled. Finishing." << std::endl;
      stream_.Finish(grpc::Status::CANCELLED, this);
      status_ = FINISH;
      auto metadata = ctx_.client_metadata();
      auto pair = metadata.find("id");
      assert(pair != metadata.end());
      int node_id = std::stoi(pair->second.data());
      std::cerr << data_->info->id() << ": Setting node " << node_id
                << " as unavailable" << std::endl;
      data_->info->SetAvailable(node_id, false);
      return;
    }

    switch (status_) {
      case REQUEST:
        new MigrateCall(data_);
        if (!ok) {
          std::cerr << "Migrate in REQUEST was not ok. Finishing." << std::endl;
          stream_.Finish(grpc::Status::CANCELLED, this);
          status_ = FINISH;
          break;
        }
        stream_.Read(&request_, this);
        status_ = READ;
        break;

      case READ:
        shard_id = request_.shard();
        std::cerr << data_->info->id() << ": Migrating shard " << shard_id
                  << std::endl;
        shard = data_->shards->at(shard_id);
        if (!shard) {
          std::cerr << data_->info->id() << ": Already given and deleted"
                    << std::endl;
          stream_.Finish(invalid_status, this);
          status_ = FINISH;
          break;
        }
        retval = shard->Unref(true);
        if (!retval)
          std::cerr << data_->info->id() << ": Resuming from SST "
                    << request_.start_from() << std::endl;
        // From now on requests for the shard are rejected
        data_->info->GiveShard(shard_id);
        migrator_ = std::unique_ptr<ShardMigrator>(
            new ShardMigrator(data_->db, shard_id, request_.start_from()));
        // DumpShard() creates SST files by iterating on the shard. We can't
        // modify the database after the iterator snapshot is taken, and
        // there may be some unfinished requests. So we have to wait for
        // the reference counter to reach 0 before calling DumpShard().
        // If we took into account batches, we would have do to that even
        // before calling GiveShard(). This is necessary, because the batch is
        // committed on the server that referenced the shard and any writes
        // that happen from the moment the shard is given until the commit,
        // will appear to have happened after the commit. However waiting for
        // the references before giving the shard might cause a deadlock.
        if (retval)
          shard->WaitRefs();
        migrator_->DumpShard(shard->cf());
        retval = migrator_->ReadChunk(&response);
        assert(retval);
        stream_.Write(response, this);
        status_ = WRITE;
        break;

      case WRITE:
        if (migrator_->ReadChunk(&response)) {
          stream_.Write(response, this);
          status_ = WRITE;
        } else {
          stream_.Read(&request_, this);
          status_ = DONE;
        }
        break;

      case DONE:
        stream_.Finish(grpc::Status::OK, this);
        data_->shards->Remove(request_.shard());
        migrator_->ClearState();
        if (data_->shards->empty()) {
          data_->info->Remove();
          shutdown.store(true);
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
  grpc::ServerAsyncReaderWriter<pb::MigrateResponse, pb::MigrateRequest>
      stream_;
  pb::MigrateRequest request_;
  pb::MigrateResponse response_;
  enum CallStatus { REQUEST, READ, WRITE, DONE, FINISH };
  CallStatus status_;
  std::unique_ptr<ShardMigrator> migrator_;
};

AsyncServer::AsyncServer(const std::string& etcd_address,
                         const std::string& dbpath, int num_threads)
    : dbpath_(dbpath),
      options_(DefaultRocksdbOptions()),
      info_(etcd_address),
      num_threads_(num_threads) {}

AsyncServer::~AsyncServer() {
  std::cerr << "Shutting down..." << std::endl;
  for (auto cq = cqs_.begin(); cq != cqs_.end(); ++cq)
    (*cq)->Shutdown();
  void* tag;
  bool ok;
  for (auto cq = cqs_.begin(); cq != cqs_.end(); ++cq)
    while ((*cq)->Next(&tag, &ok))
      static_cast<Call*>(tag)->Delete();
  migrate_cq_->Shutdown();
  while (migrate_cq_->Next(&tag, &ok))
    static_cast<Call*>(tag)->Delete();
  info_.WatchCancel(call_);
  watcher_.join();
  delete shards_;
  delete db_;
}

void AsyncServer::Init(const std::string& listening_address,
                       const std::string& hostname) {
  // Initialize gRPC
  grpc::ServerBuilder builder;
  int selected_port;
  builder.AddListeningPort(listening_address, grpc::InsecureServerCredentials(),
                           &selected_port);
  builder.RegisterService(&service_);
  for (int i = 0; i < num_threads_; i++)
    cqs_.emplace_back(builder.AddCompletionQueue());
  migrate_cq_ = builder.AddCompletionQueue();
  server_ = builder.BuildAndStart();
  if (selected_port == 0) {
    std::cerr << "Could not bind to a port" << std::endl;
    exit(EXIT_FAILURE);
  }

  // Announce server to etcd
  std::string port = std::to_string(selected_port);
  std::string node_address = hostname + ":" + port;
  // TODO: This knows if we are resuming. We could return a relevant
  // bool, and if resuming check that we have the right column families.
  info_.Add(node_address);

  // Open RocksDB database
  std::vector<std::string> column_families;
  db_->ListColumnFamilies(options_, dbpath_, &column_families);
  if (!column_families.empty()) {
    std::cerr << info_.id() << ": Recovering from crash" << std::endl;
    column_families.push_back("default");
    std::vector<rocksdb::ColumnFamilyDescriptor> cf_descriptors;
    rocksdb::ColumnFamilyOptions cf_options;
    for (auto name : column_families) {
      rocksdb::ColumnFamilyDescriptor descriptor(name, cf_options);
      cf_descriptors.push_back(descriptor);
    }
    std::vector<rocksdb::ColumnFamilyHandle*> cf_handles;
    rocksdb::Status s =
        rocksdb::DB::Open(options_, dbpath_, cf_descriptors, &cf_handles, &db_);
    EnsureRocksdb("Open", s);
    shards_ = new Shards(db_, cf_handles);
  } else {
    rocksdb::Status s = rocksdb::DB::Open(options_, dbpath_, &db_);
    EnsureRocksdb("Open", s);
    shards_ = new Shards(db_, info_.shards());
  }

  // Watch etcd for changes to the cluster
  call_ = info_.Watch();
  // Create a thread that watches the "info" key and repeatedly
  // reads for updates. Gets cleaned up by the destructor.
  watcher_ = std::thread(&AsyncServer::WatchThread, this);
  std::cerr << "Asynchronous server listening on port " << port << std::endl;
}

void AsyncServer::Run() {
  std::vector<std::thread> threads;
  for (int i = 0; i < num_threads_; i++) {
    CallData data{&service_, cqs_[i].get(), db_, &info_, shards_};
    new GetCall(&data);
    new PutCall(&data);
    new DeleteCall(&data);
    new BatchCall(&data);
    new IteratorCall(&data);
    threads.emplace_back(std::thread(&AsyncServer::ServeThread, this, i));
  }
  CallData migrate_data{&service_, migrate_cq_.get(), db_, &info_, shards_};
  new MigrateCall(&migrate_data);
  void* tag;
  bool ok;
  // For the meaning of the return value of Next, and ok see:
  // https://groups.google.com/d/msg/grpc-io/qtZya6AuGAQ/Umepla-GAAAJ
  // http://www.grpc.io/grpc/cpp/classgrpc_1_1_completion_queue.html
  while (migrate_cq_->Next(&tag, &ok)) {
    if (shutdown.load()) {
      std::cerr << "Breaking from migrate_cq_->Next before Proceed"
                << std::endl;
      break;
    }
    static_cast<Call*>(tag)->Proceed(ok);
    if (shutdown.load())
      break;
  }
  server_->Shutdown(std::chrono::system_clock::now());
  for (auto thr = threads.begin(); thr != threads.end(); thr++)
    thr->join();
}

void AsyncServer::ServeThread(int i) {
  void* tag;
  bool ok;
  while (cqs_[i]->Next(&tag, &ok)) {
    if (shutdown.load()) {
      static_cast<Call*>(tag)->Delete();
      break;
    }
    static_cast<Call*>(tag)->Proceed(ok);
    if (shutdown.load()) {
      std::cerr << "Breaking from cq_->Next after Proceed" << std::endl;
      break;
    }
  }
}

void AsyncServer::WatchThread() {
  do {
    for (const auto& task : info_.Tasks()) {
      int node_id = task.first;
      std::string address = info_.Address(node_id);
      for (int shard_id : task.second) {
        if (!info_.IsAvailable(node_id)) {
          std::cerr << info_.id() << ": Node " << node_id
                    << " is unavailable. Skipping request for shard "
                    << shard_id << "." << std::endl;
          continue;
        }
        std::cerr << info_.id() << ": Requesting shard " << shard_id
                  << " from node " << node_id << std::endl;
        Shard* shard;
        // If it does not belong to us, we may
        // or may not have it and we must check.
        if (info_.IndexForShard(shard_id) != info_.id()) {
          if (!shards_->at(shard_id))
            // We don't have it so create it
            shard = shards_->Add(shard_id, address).get();
          else
            // We managed to create it before crashing. We
            // should ensure that it is empty as it should.
            shard = shards_->at(shard_id).get();
        } else {
          shard = shards_->at(shard_id).get();
        }

        ShardImporter importer(db_, shard_id);
        // If we are recovering from a crash there might be a file
        // that we didn't manage to ingest. Try to do that. If
        // there isn't such a file, Ingest() will silently fail.
        if (!importer.filename().empty())
          shard->Ingest(importer.filename(), importer.largest_key());

        pb::MigrateRequest request;
        pb::MigrateResponse response;
        grpc::ClientContext context;
        context.AddMetadata("id", std::to_string(info_.id()));
        std::unique_ptr<pb::RPC::Stub> stub(pb::RPC::NewStub(
            grpc::CreateChannel(address, grpc::InsecureChannelCredentials())));

        // Send a request for the shard
        request.set_shard(shard_id);
        request.set_start_from(importer.num());
        auto stream = stub->Migrate(&context);
        if (!stream->Write(request)) {
          std::cerr << "Error on first write" << std::endl;
          grpc::Status status = stream->Finish();
          HandleError(status, node_id);
          continue;
        }

        // The first read should be ok. Even if the shard is empty,
        // one message will be sent. So if it is not ok, it means he
        // crashed. We cannot know if he managed to give the shard.
        if (!stream->Read(&response)) {
          std::cerr << "Error on first read" << std::endl;
          grpc::Status status = stream->Finish();
          if (status.error_code() == grpc::StatusCode::INVALID_ARGUMENT) {
            std::cerr << "Migration was already finished but didn't manage to "
                         "announce it before crashing"
                      << std::endl;
            info_.MigrationOver(shard_id);
            // FIXME: If we crash here the state never gets cleared
            importer.ClearState();
            do {
              bool ret = info_.WatchNext(call_);
              assert(!ret);
            } while (info_.IsMigrating(shard_id));
          } else {
            HandleError(status, node_id);
          }
          continue;
        }

        // Once the old master gets the request, he is supposed to pass
        // ownership to us by informing etcd. We wait for that, so that
        // we can start serving requests for that shard immediately.
        while (info_.IndexForShard(shard_id) != info_.id()) {
          bool ret = info_.WatchNext(call_);
          assert(!ret);
        }
        // From now on requests for the shard are accepted

        do {
          if (response.finished())
            break;
          // If true an SST is ready to be imported
          if (importer.WriteChunk(response))
            shard->Ingest(importer.filename(), importer.largest_key());
        } while (stream->Read(&response));

        shard->set_importing(false);
        stream->Write(request);
        grpc::Status status = stream->Finish();
        if (!status.ok()) {
          std::cerr << "Error on finish" << std::endl;
          HandleError(status, node_id);
          continue;
        }

        info_.MigrationOver(shard_id);
        // FIXME: If we crash here the state never gets cleared
        importer.ClearState();
        // Wait for the confirmation from etcd
        do {
          bool ret = info_.WatchNext(call_);
          assert(!ret);
        } while (info_.IsMigrating(shard_id));
        std::cerr << info_.id() << ": Imported shard " << shard_id << std::endl;
      }
    }
  } while (!info_.WatchNext(call_));
}

void AsyncServer::HandleError(const grpc::Status& status, int node_id) {
  if (status.error_code() == grpc::StatusCode::UNAVAILABLE) {
    std::cerr << info_.id() << ": Setting node " << node_id << " as unavailable"
              << std::endl;
    info_.SetAvailable(node_id, false);
  } else if (!status.ok()) {
    // For every error other than UNAVAILABLE, exit
    EnsureRpc(status);
  }
}

}  // namespace crocks
