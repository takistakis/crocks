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

namespace crocks {

rocksdb::Status Shard::Get(const std::string& key, std::string* value) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (importing_)
    return newer_->GetFromBatch(cf_, db_->GetOptions(), key, value);
  else
    return db_->Get(rocksdb::ReadOptions(), cf_, key, value);
}

rocksdb::Status Shard::GetFromBatch(const std::string& key,
                                    std::string* value) {
  std::lock_guard<std::mutex> lock(mutex_);
  return newer_->GetFromBatch(cf_, db_->GetOptions(), key, value);
}

rocksdb::Status Shard::Put(const std::string& key, const std::string& value) {
  std::lock_guard<std::mutex> lock(mutex_);
  rocksdb::Status s;
  if (importing_)
    newer_->Put(cf_, key, value);
  else
    s = db_->Put(rocksdb::WriteOptions(), cf_, key, value);
  return s;
}

rocksdb::Status Shard::Delete(const std::string& key) {
  std::lock_guard<std::mutex> lock(mutex_);
  rocksdb::Status s;
  if (importing_)
    newer_->Delete(cf_, key);
  else
    s = db_->Delete(rocksdb::WriteOptions(), cf_, key);
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
  s = db_->Write(rocksdb::WriteOptions(), newer_->GetWriteBatch());
  EnsureRocksdb("Write", s);
  delete newer_;
  importing_ = false;
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

}  // namespace crocks
