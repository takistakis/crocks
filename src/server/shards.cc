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
}

Shard::~Shard() {
  db_->DropColumnFamily(cf_);
  delete cf_;
}

rocksdb::Status Shard::Get(const std::string& key, std::string* value,
                           bool* ask) {
  rocksdb::Status s;
  bool not_ingested_up_to_key;
  {
    read_lock lock(largest_key_mutex_);
    not_ingested_up_to_key = key > largest_key_;
  }
  // The get must take place after not_ingested_up_to_key
  // is set. Otherwise there is a race: the get can
  // happen before the ingestion and the check after.
  s = db_->Get(rocksdb::ReadOptions(), cf_, key, value);
  // If we are importing and have not yet ingested the SST with the
  // key range that contains the given key and there is not a more
  // recent value, we have no choice but to ask the former master.
  *ask = importing_.load() && not_ingested_up_to_key && s.IsNotFound();
  return s;
}

rocksdb::Status Shard::Put(const std::string& key, const std::string& value) {
  return db_->Put(rocksdb::WriteOptions(), cf_, key, value);
}

rocksdb::Status Shard::Delete(const std::string& key) {
  return db_->Delete(rocksdb::WriteOptions(), cf_, key);
}

void Shard::Ingest(const std::string& filename,
                   const std::string& largest_key) {
  std::vector<std::string> files{filename};
  rocksdb::IngestExternalFileOptions ifo;
  ifo.move_files = true;
  // Ingest file at the bottommost level, so
  // that it won't overwrite any newer keys
  ifo.ingest_behind = true;
  rocksdb::Status s = db_->IngestExternalFile(cf_, files, ifo);
  // Skip IOError. We may have ingested the file before crashing.
  if (!s.IsIOError())
    EnsureRocksdb("IngestExternalFile", s);
  {
    write_lock lock(largest_key_mutex_);
    largest_key_ = largest_key;
  }
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

bool Shard::Unref(bool migrating) {
  std::lock_guard<std::mutex> lock(ref_mutex_);
  if (migrating && migrating_)
    // We have already called this with migrating == true
    return false;
  refs_--;
  if (migrating)
    migrating_ = true;
  if (refs_ == 0)
    zero_refs_.set_value();
  return true;
}

void Shard::WaitRefs() {
  std::future<void> f = zero_refs_.get_future();
  f.wait();
}

Shards::Shards(rocksdb::DB* db, const std::vector<int>& shards) : db_(db) {
  std::vector<std::string> names;
  for (int shard : shards)
    names.push_back(std::to_string(shard));
  std::vector<rocksdb::ColumnFamilyHandle*> handles;
  rocksdb::Status s = db_->CreateColumnFamilies(rocksdb::ColumnFamilyOptions(),
                                                names, &handles);
  EnsureRocksdb("CreateColumnFamilies", s);
  int i = 0;
  for (int shard : shards)
    shards_[shard] = std::make_shared<Shard>(db_, handles[i++], shard);
}

Shards::Shards(rocksdb::DB* db,
               const std::vector<rocksdb::ColumnFamilyHandle*>& handles)
    : db_(db) {
  // TODO: Check that we have every column
  // family that we should, according to etcd.
  for (auto cf : handles) {
    std::string name = cf->GetName();
    if (name == "default")
      continue;
    int shard = std::stoi(name);
    shards_[shard] = std::make_shared<Shard>(db_, cf, shard);
  }
}

std::shared_ptr<Shard> Shards::Add(int id, const std::string& old_address) {
  write_lock lock(mutex_);
  auto shard = std::make_shared<Shard>(db_, id, old_address);
  shards_[id] = shard;
  return shard;
}

void Shards::Remove(int id) {
  write_lock lock(mutex_);
  shards_.erase(id);
}

std::vector<rocksdb::ColumnFamilyHandle*> Shards::ColumnFamilies() const {
  read_lock lock(mutex_);
  std::vector<rocksdb::ColumnFamilyHandle*> column_families;
  for (const auto& pair : shards_) {
    Shard* shard = pair.second.get();
    assert(shard != nullptr);
    column_families.push_back(shard->cf());
  }
  return column_families;
}

}  // namespace crocks
