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

#include "src/server/util.h"

#include <assert.h>
#include <stdlib.h>

#include <iostream>
#include <string>

#include <rocksdb/slice.h>
#include <rocksdb/write_batch.h>

#include <crocks/status.h>
#include "gen/crocks.pb.h"

namespace crocks {

int RocksdbStatusCodeToInt(const rocksdb::Status::Code& status) {
  int rocksdb_code = static_cast<int>(status);
  assert(rocksdb_code >= 0 && rocksdb_code <= 13);
  return rocksdb_code;
}

void EnsureRocksdb(const std::string& what, const rocksdb::Status& status) {
  if (!status.ok()) {
    std::cerr << "RocksDB " << what << " failed with status " << status.code()
              << " (" << status.getState() << ")" << std::endl;
    exit(EXIT_FAILURE);
  }
}

void ApplyBatchUpdate(rocksdb::WriteBatch* batch,
                      const pb::BatchUpdate& batch_update) {
  switch (batch_update.op()) {
    case pb::BatchUpdate::PUT:
      batch->Put(batch_update.key(), batch_update.value());
      break;
    case pb::BatchUpdate::DELETE:
      batch->Delete(batch_update.key());
      break;
    case pb::BatchUpdate::SINGLE_DELETE:
      batch->SingleDelete(batch_update.key());
      break;
    case pb::BatchUpdate::MERGE:
      batch->Merge(batch_update.key(), batch_update.value());
      break;
    case pb::BatchUpdate::CLEAR:
      batch->Clear();
      break;
    default:
      assert(false);
  }
}

rocksdb::Options DefaultRocksdbOptions() {
  rocksdb::Options options;

  options.create_if_missing = true;
  options.IncreaseParallelism(4);
  options.OptimizeLevelStyleCompaction();
  options.compaction_pri = rocksdb::kOldestSmallestSeqFirst;
  options.level_compaction_dynamic_level_bytes = true;

  return options;
}

}  // namespace crocks
