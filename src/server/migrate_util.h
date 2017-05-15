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

std::string Filename(const std::string& path, int shard, int num);

class ShardMigrator {
 public:
  ShardMigrator(rocksdb::DB* db, int shard);

  void DumpShard(rocksdb::ColumnFamilyHandle* cf);

  bool ReadChunk(pb::MigrateResponse* response);

 private:
  rocksdb::DB* db_;
  std::ifstream in_;
  int total_;
  int num_;
  int shard_;
  bool empty_;

  std::string filename_;
  bool done_;
};

class ShardImporter {
 public:
  ShardImporter(rocksdb::DB* db, int shard);

  void WriteChunk(const pb::MigrateResponse& response);

  std::vector<std::string> Files();

 private:
  rocksdb::DB* db_;
  std::ofstream out_;
  int num_;
  int shard_;
};

}  // namespace crocks

#endif  // CROCKS_SERVER_MIGRATE_UTIL_H
