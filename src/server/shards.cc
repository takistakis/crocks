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

#include "src/server/shards.h"

#include <assert.h>

#include <utility>

#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <rocksdb/write_batch.h>

#include "src/server/util.h"

namespace crocks {

Shard::Shard(rocksdb::DB* db, rocksdb::ColumnFamilyHandle* cf, int shard)
    : db_(db), cf_(cf), importing_(false), migrating_(false), refs_(1) {}

Shard::Shard(rocksdb::DB* db, int shard, const std::string& old_address)
    : db_(db),
      importing_(true),
      migrating_(false),
      refs_(1),
      old_address_(old_address) {
  rocksdb::ColumnFamilyOptions options;
  std::string name = std::to_string(shard);
  rocksdb::Status s = db_->CreateColumnFamily(options, name, &cf_);
  EnsureRocksdb("CreateColumnFamily", s);
  name += "-backup";
  s = db_->CreateColumnFamily(options, name, &backup_);
  EnsureRocksdb("CreateColumnFamily", s);
  newer_ = new rocksdb::WriteBatch();
}

Shard::~Shard() {
  db_->DropColumnFamily(cf_);
  delete cf_;
}

rocksdb::Status Shard::Get(const std::string& key, std::string* value,
                           bool* ask) {
  std::lock_guard<std::mutex> lock(mutex_);
  rocksdb::Status s;
  *ask = false;
  if (importing_.load()) {
    s = db_->Get(rocksdb::ReadOptions(), backup_, key, value);
    if (s.IsNotFound())
      *ask = true;
  } else {
    s = db_->Get(rocksdb::ReadOptions(), cf_, key, value);
  }
  return s;
}

rocksdb::Status Shard::Put(const std::string& key, const std::string& value) {
  std::lock_guard<std::mutex> lock(mutex_);
  rocksdb::Status s;
  if (importing_.load()) {
    s = db_->Put(rocksdb::WriteOptions(), backup_, key, value);
    EnsureRocksdb("Put", s);
    newer_->Put(cf_, key, value);
  } else {
    s = db_->Put(rocksdb::WriteOptions(), cf_, key, value);
  }
  return s;
}

rocksdb::Status Shard::Delete(const std::string& key) {
  std::lock_guard<std::mutex> lock(mutex_);
  rocksdb::Status s;
  if (importing_.load()) {
    s = db_->Delete(rocksdb::WriteOptions(), backup_, key);
    EnsureRocksdb("Delete", s);
    newer_->Delete(cf_, key);
  } else {
    s = db_->Delete(rocksdb::WriteOptions(), cf_, key);
  }
  return s;
}

void Shard::Ingest(const std::vector<std::string>& files) {
  std::lock_guard<std::mutex> lock(mutex_);
  rocksdb::Status s;
  if (files.size() > 0) {
    rocksdb::IngestExternalFileOptions ifo;
    ifo.move_files = true;
    s = db_->IngestExternalFile(cf_, files, ifo);
    EnsureRocksdb("IngestExternalFile", s);
  }
  s = db_->Write(rocksdb::WriteOptions(), newer_);
  EnsureRocksdb("Write", s);
  delete newer_;
  db_->DropColumnFamily(backup_);
  delete backup_;
  importing_.store(false);
}

bool Shard::Ref() {
  std::lock_guard<std::mutex> lock(ref_mutex_);
  if (migrating_)
    return false;
  // Using an atomic integer for the reference counter and remove the
  // lock would be nice but would cause a race condition. It would be
  // possible to enter Ref with migrating_ == false, pass the if, run
  // the whole Unref in another thread and then increment the counter.
  // That way the reference would reach zero twice and it would crash.
  refs_++;
  return true;
}

void Shard::Unref(bool migrating) {
  std::lock_guard<std::mutex> lock(ref_mutex_);
  refs_--;
  if (migrating)
    migrating_ = true;
  if (refs_ == 0)
    zero_refs_.set_value();
}

void Shard::WaitRefs() {
  std::future<void> f = zero_refs_.get_future();
  f.wait();
}

Shards::Shards(rocksdb::DB* db, std::vector<int> shards) : db_(db) {
  std::vector<std::string> names;
  for (int shard : shards)
    names.push_back(std::to_string(shard));
  std::vector<rocksdb::ColumnFamilyHandle*> handles;
  rocksdb::Status s = db_->CreateColumnFamilies(rocksdb::ColumnFamilyOptions(),
                                                names, &handles);
  EnsureRocksdb("CreateColumnFamilies", s);
  int i = 0;
  for (int shard : shards)
    shards_[shard] = new Shard(db_, handles[i++], shard);
}

Shards::~Shards() {
  for (auto shard : shards_)
    delete shard.second;
}

Shard* Shards::Add(int id, const std::string& old_address) {
  std::lock_guard<std::mutex> lock(mutex_);
  Shard* shard = new Shard(db_, id, old_address);
  shards_[id] = shard;
  return shard;
}

void Shards::Remove(int id) {
  std::lock_guard<std::mutex> lock(mutex_);
  delete shards_[id];
  shards_.erase(id);
}

std::vector<rocksdb::ColumnFamilyHandle*> Shards::ColumnFamilies() const {
  std::lock_guard<std::mutex> lock(mutex_);
  std::vector<rocksdb::ColumnFamilyHandle*> column_families;
  for (const auto& pair : shards_) {
    Shard* shard = pair.second;
    column_families.push_back(shard->cf());
  }
  return column_families;
}

Shard* Shards::Ref(int id) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (shards_.find(id) == shards_.end())
    return nullptr;
  Shard* shard = shards_.at(id);
  if (!shard->Ref())
    return nullptr;
  else
    return shard;
}

}  // namespace crocks
