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

#ifndef CROCKS_SERVER_MIGRATE_UTIL_H
#define CROCKS_SERVER_MIGRATE_UTIL_H

#include <fstream>
#include <string>
#include <vector>

namespace rocksdb {
class DB;
class ColumnFamilyHandle;
}

namespace crocks {

namespace pb {
class MigrateResponse;
}

std::string Key(int shard, const std::string& key);
std::string Filename(const std::string& path, int shard, int num);

class ShardMigrator {
 public:
  ShardMigrator(rocksdb::DB* db, int shard, int start_from);

  void DumpShard(rocksdb::ColumnFamilyHandle* cf);

  // Save the state (whether the shard has been dumped,
  // the total number of SSTs and the largest key of each
  // SST), in order to be able to recover from crashes.
  void SaveState();

  // Return whether the shard has already been dumped,
  // and if it has restore the state from the database.
  bool RestoreState();

  void ClearState();

  // Read the next chunk, and put it in the given response.
  // Return whether there are more chunks to read.
  bool ReadChunk(pb::MigrateResponse* response);

 private:
  rocksdb::DB* db_;
  std::vector<std::string> largest_keys_;
  std::ifstream in_;
  unsigned int total_;
  unsigned int num_;
  int shard_;
  bool empty_;

  std::string filename_;
  bool done_;
  bool finished_;
};

class ShardImporter {
 public:
  ShardImporter(rocksdb::DB* db, int shard);

  bool WriteChunk(const pb::MigrateResponse& response);

  std::string filename() const {
    return filename_;
  }

  std::string largest_key() const {
    return largest_key_;
  }

  int num() const {
    return num_;
  }

  void SaveState();
  void RestoreState();
  void ClearState();

 private:
  rocksdb::DB* db_;
  std::string filename_;
  std::string largest_key_;
  std::ofstream out_;
  int num_;
  int shard_;
};

}  // namespace crocks

#endif  // CROCKS_SERVER_MIGRATE_UTIL_H
