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

#include <rocksdb/db.h>
#include <rocksdb/env.h>
#include <rocksdb/iterator.h>
#include <rocksdb/options.h>
#include <rocksdb/slice.h>
#include <rocksdb/sst_file_writer.h>
#include <rocksdb/status.h>
#include <rocksdb/write_batch.h>

#include "gen/crocks.pb.h"
#include "src/server/util.h"

// First try. For each shard, write as many
// SSTs as needed, before sending anything.

namespace crocks {

std::string Key(int shard, const std::string& key) {
  return "shard_" + std::to_string(shard) + "_" + key;
}

std::string Filename(const std::string& path, int shard, int num) {
  return path + "/shard_" + std::to_string(shard) + "_" + std::to_string(num);
}

ShardMigrator::ShardMigrator(rocksdb::DB* db, int shard, int start_from)
    : db_(db),
      total_(0),
      num_(start_from),
      shard_(shard),
      done_(false),
      finished_(false) {}

void ShardMigrator::DumpShard(rocksdb::ColumnFamilyHandle* cf) {
  if (RestoreState())
    return;

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
    done_ = true;
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
      largest_keys_.push_back(file_info.largest_key);
      open = false;
    }
    it->Next();
  }

  if (open) {
    s = writer.Finish(&file_info);
    largest_keys_.push_back(file_info.largest_key);
    EnsureRocksdb("SstFileWriter::Finish", s);
  }

  EnsureRocksdb("Iterator", it->status());
  delete it;

  total_ = num;
  assert(largest_keys_.size() == total_);

  if (num_ >= total_)
    done_ = true;

  SaveState();
}

void ShardMigrator::SaveState() {
  rocksdb::WriteBatch batch;
  batch.Put(Key(shard_, "dumped"), "true");
  batch.Put(Key(shard_, "total"), std::to_string(total_));
  for (unsigned int i = 0; i < total_; i++) {
    std::string key = std::to_string(i) + "_largest_key";
    batch.Put(Key(shard_, key), largest_keys_[i]);
  }
  rocksdb::Status s = db_->Write(rocksdb::WriteOptions(), &batch);
  EnsureRocksdb("Write(migrator_state)", s);
}

bool ShardMigrator::RestoreState() {
  rocksdb::Status s;
  rocksdb::ReadOptions options;
  std::string value;
  s = db_->Get(options, Key(shard_, "dumped"), &value);
  if (s.IsNotFound())
    return false;
  EnsureRocksdb("Get(dumped)", s);
  if (value == "true") {
    s = db_->Get(options, Key(shard_, "total"), &value);
    EnsureRocksdb("Get(total)", s);
    total_ = std::stoi(value);
    for (unsigned int i = 0; i < total_; i++) {
      std::string key = std::to_string(i) + "_largest_key";
      s = db_->Get(options, Key(shard_, key), &value);
      EnsureRocksdb("Get(largest_key)", s);
      largest_keys_.push_back(value);
    }
    assert(num_ <= total_);
    if (num_ == total_)
      done_ = true;
    return true;
  }
  return false;
}

void ShardMigrator::ClearState() {
  rocksdb::WriteBatch batch;
  batch.Delete(Key(shard_, "dumped"));
  batch.Delete(Key(shard_, "total"));
  for (unsigned int i = 0; i < total_; i++) {
    std::string key = std::to_string(i) + "_largest_key";
    batch.Delete(Key(shard_, key));
  }
  rocksdb::Status s = db_->Write(rocksdb::WriteOptions(), &batch);
  EnsureRocksdb("Write(migrator_state)", s);
  // Delete the last file
  if (!filename_.empty())
    if (remove(filename_.c_str()) < 0)
      perror(filename_.c_str());
}

bool ShardMigrator::ReadChunk(pb::MigrateResponse* response) {
  // When the last chunk is sent, we set done = true.
  // The next message will have finished = true and will be the last.
  if (finished_)
    return false;

  if (done_) {
    response->set_finished(true);
    finished_ = true;
    return true;
  }

  if (!in_.is_open()) {
    // Delete the previous file
    if (!filename_.empty())
      if (remove(filename_.c_str()) < 0)
        perror(filename_.c_str());
    filename_ = Filename(db_->GetName(), shard_, num_);
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
  response->set_largest_key("");
  response->set_chunk(buf, in_.gcount());

  if (in_.eof()) {
    response->set_eof(true);
    response->set_largest_key(largest_keys_[num_++]);
    in_.close();
    // We cannot delete the file here because we may have to send it again
  }

  // On the last message set done_ = true, so that the
  // next time ReadChunk is called, it will return false;
  if (num_ == total_ && !in_.is_open())
    done_ = true;

  return true;
}

ShardImporter::ShardImporter(rocksdb::DB* db, int shard)
    : db_(db), num_(0), shard_(shard) {
  RestoreState();
}

bool ShardImporter::WriteChunk(const pb::MigrateResponse& response) {
  if (!out_.is_open()) {
    // Open next file
    filename_ = Filename(db_->GetName(), shard_, num_++);
    out_.open(filename_, std::ofstream::binary);
    if (!out_) {
      perror(filename_.c_str());
      exit(EXIT_FAILURE);
    }
  }

  // Append chunk
  std::string chunk = response.chunk();
  out_.write(chunk.c_str(), chunk.size());
  // Close file if it was the last chunk for the sst
  if (response.eof()) {
    largest_key_ = response.largest_key();
    out_.close();
    out_.flush();
    out_.rdbuf()->pubsync();
    SaveState();
    return true;
  }
  return false;
}

void ShardImporter::SaveState() {
  rocksdb::WriteBatch batch;
  batch.Put(Key(shard_, "next_num"), std::to_string(num_));
  batch.Put(Key(shard_, "largest_key"), largest_key_);
  batch.Put(Key(shard_, "filename"), filename_);
  rocksdb::WriteOptions options;
  options.sync = true;
  rocksdb::Status s = db_->Write(options, &batch);
  EnsureRocksdb("Write(importer_state)", s);
}

void ShardImporter::RestoreState() {
  rocksdb::Status s;
  rocksdb::ReadOptions options;
  s = db_->Get(options, Key(shard_, "largest_key"), &largest_key_);
  if (s.IsNotFound())
    return;
  EnsureRocksdb("Get(largest_key)", s);
  s = db_->Get(options, Key(shard_, "filename"), &filename_);
  EnsureRocksdb("Get(filename)", s);
  std::string next_num;
  s = db_->Get(options, Key(shard_, "next_num"), &next_num);
  EnsureRocksdb("Get(next_num)", s);
  num_ = std::stoi(next_num);
}

void ShardImporter::ClearState() {
  rocksdb::WriteBatch batch;
  batch.Delete(Key(shard_, "next_num"));
  batch.Delete(Key(shard_, "largest_key"));
  batch.Delete(Key(shard_, "filename"));
  rocksdb::Status s = db_->Write(rocksdb::WriteOptions(), &batch);
  EnsureRocksdb("Write(importer_state)", s);
}

}  // namespace crocks
