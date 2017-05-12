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

#include "src/server/migrate_util.h"

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>

#include <iostream>
#include <memory>
#include <utility>
#include <vector>

#include <grpc++/grpc++.h>
#include <rocksdb/db.h>
#include <rocksdb/env.h>
#include <rocksdb/iterator.h>
#include <rocksdb/options.h>
#include <rocksdb/sst_file_writer.h>
#include <rocksdb/status.h>

#include <crocks/status.h>
#include "gen/crocks.grpc.pb.h"
#include "gen/crocks.pb.h"
#include "src/common/info.h"
#include "src/server/util.h"

// First try. For each shard, write as many
// SSTs as needed, before sending anything.

namespace crocks {

std::string Filename(const std::string& path, int shard, int num) {
  return path + "/shard_" + std::to_string(shard) + "_" + std::to_string(num);
}

ShardMigrator::ShardMigrator(rocksdb::DB* db, int shard)
    : db_(db), total_(0), num_(0), shard_(shard), empty_(false), done_(false) {}

// ShardMigrator
void ShardMigrator::DumpShard(rocksdb::ColumnFamilyHandle* cf) {
  rocksdb::Status s;
  rocksdb::Options options(db_->GetOptions());
  rocksdb::ExternalSstFileInfo file_info;
  rocksdb::Iterator* it = db_->NewIterator(rocksdb::ReadOptions(), cf);
  rocksdb::SstFileWriter writer(rocksdb::EnvOptions(), options,
                                options.comparator);
  int num = 0;
  bool open = false;
  it->SeekToFirst();
  if (!it->Valid()) {
    empty_ = true;
    delete it;
    return;
  }

  while (it->Valid()) {
    if (!open) {
      // `num` starts from 0, and when the loop is over,
      // it contains the number of SST files written.
      s = writer.Open(Filename(db_->GetName(), shard_, num++));
      EnsureRocksdb("SstFileWriter::Open", s);
      open = true;
    }
    s = writer.Put(it->key(), it->value());
    EnsureRocksdb("SstFileWriter::Add", s);
    if (writer.FileSize() > options.target_file_size_base) {
      s = writer.Finish(&file_info);
      EnsureRocksdb("SstFileWriter::Finish", s);
      // NOTE: file_info.largest_key contains the largest key if the SST file
      open = false;
    }
    it->Next();
  }

  if (open) {
    s = writer.Finish(&file_info);
    EnsureRocksdb("SstFileWriter::Finish", s);
  }

  EnsureRocksdb("Iterator", it->status());
  delete it;

  total_ = num;
}

bool ShardMigrator::ReadChunk(pb::MigrateResponse* response) {
  if (done_)
    return false;

  if (empty_) {
    // If no file was written the shard was empty so we send a
    // single message with an empty chunk and the empty flag set
    // to true. XXX: Maybe we could ditch the empty field and at
    // the receiving server check if the first message has an empty
    // chunk. FWIW protobuf bools are inexpensive on the wire.
    assert(num_ == 0 && !in_.is_open());
    response->set_empty(true);
    done_ = true;
    return true;
  }

  if (!in_.is_open()) {
    filename_ = Filename(db_->GetName(), shard_, num_++);
    in_.open(filename_, std::ifstream::binary);
    if (!in_) {
      perror(filename_.c_str());
      exit(EXIT_FAILURE);
    }
  }

  const int kBufSize = 1 << 20;  // 1MB
  char buf[kBufSize];
  in_.read(buf, kBufSize);
  response->set_eof(false);
  response->set_chunk(buf, in_.gcount());

  if (in_.eof()) {
    response->set_eof(true);
    in_.close();
    if (remove(filename_.c_str()) < 0)
      perror(filename_.c_str());
  }

  // On the last message set done_ = true, so that the
  // next time ReadChunk is called, it will return false;
  if (num_ == total_ && !in_.is_open())
    done_ = true;

  return true;
}

void RequestShard(Info* info, rocksdb::DB* db,
                  std::unordered_map<int, rocksdb::ColumnFamilyHandle*>* cfs,
                  const std::string& address, int shard, void* call) {
  pb::MigrateRequest request;
  pb::MigrateResponse response;
  grpc::ClientContext context;

  std::unique_ptr<pb::RPC::Stub> stub(pb::RPC::NewStub(
      grpc::CreateChannel(address, grpc::InsecureChannelCredentials())));

  request.set_shard(shard);
  std::unique_ptr<grpc::ClientReaderInterface<pb::MigrateResponse>> reader =
      stub->Migrate(&context, request);

  // The server that is sending the shard is supposed to pass ownership to us
  while (info->IndexForShard(shard) != info->id()) {
    bool ret = info->WatchNext(call);
    assert(!ret);
  }

  ShardImporter importer(db, shard);
  while (reader->Read(&response))
    importer.WriteChunk(response);

  importer.Finish(info, cfs->at(shard));
  EnsureRpc(reader->Finish());
}

// ShardImporter
ShardImporter::ShardImporter(rocksdb::DB* db, int shard)
    : db_(db), num_(0), shard_(shard), empty_(false) {}

void ShardImporter::WriteChunk(const pb::MigrateResponse& response) {
  if (response.empty()) {
    empty_ = true;
    // If the shard is empty, WriteChunk is supposed to be called only once
    assert(num_ == 0 && !out_.is_open());
    return;
  }

  if (!out_.is_open()) {
    // Open next file
    std::string filename = Filename(db_->GetName(), shard_, num_++);
    out_.open(filename, std::ofstream::binary);
    if (!out_) {
      perror(filename.c_str());
      exit(EXIT_FAILURE);
    }
  }

  // Append chunk
  std::string chunk = response.chunk();
  out_.write(chunk.c_str(), chunk.size());
  // Close file if it was the last chunk for the sst
  if (response.eof())
    out_.close();
}

void ShardImporter::Finish(Info* info, rocksdb::ColumnFamilyHandle* cf) {
  if (empty_) {
    std::cout << info->id() << ": Shard " << shard_ << " was empty"
              << std::endl;
  } else {
    assert(info->IsImporting(shard_));
    IngestShard(cf);
    std::cout << info->id() << ": Succesfully ingested shard " << shard_
              << " from " << num_ << " SST files" << std::endl;
  }
  info->SetImported(shard_);
}

void ShardImporter::IngestShard(rocksdb::ColumnFamilyHandle* cf) {
  rocksdb::Status s;
  rocksdb::Options options(db_->GetOptions());
  rocksdb::IngestExternalFileOptions ifo;
  ifo.move_files = true;

  std::vector<std::string> files;
  for (int i = 0; i < num_; i++)
    files.push_back(Filename(db_->GetName(), shard_, i));

  s = db_->IngestExternalFile(cf, files, ifo);
  EnsureRocksdb("IngestExternalFile", s);
}

void WatchThread(Info* info, rocksdb::DB* db,
                 std::unordered_map<int, rocksdb::ColumnFamilyHandle*>* cfs,
                 void* call) {
  for (;;) {
    if (info->WatchNext(call))
      return;

    info->UpdateIndex();
    if (info->id() == -1)
      continue;

    std::vector<int> future = info->future();
    if (future.size() == 0)
      continue;

    for (int shard : future)
      info->SetImporting(shard);
    AddColumnFamilies(future, db, cfs);

    info->UpdateTasks();
    for (const auto& task : info->tasks()) {
      std::string address = info->Address(task.first);
      for (int shard : task.second)
        RequestShard(info, db, cfs, address, shard, call);
    }

    if (info->NoMigrations()) {
      std::cout << info->id()
                << ": Migrations are over. Switching back to RUNNING."
                << std::endl;
      info->Run();
    }
  }
}

}  // namespace crocks
