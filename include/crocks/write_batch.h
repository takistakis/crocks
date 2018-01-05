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

// Wrapper around rocksdb::WriteBatch. We retain more or less the same API, with
// the following exceptions:
//
// 1. In RocksDB, the WriteBatch is completely decoupled from the database. Here
//    however, we need to send requests while adding updates, before calling
//    Write(). So our WriteBatch() has a Cluster* parameter and instead of
//    committing with db->Write(batch), we just do batch.Write().
// 2. Buffers are cleared as soon as they are sent. So updates in a batch cannot
//    be reused and after Write() has returned, the batch is empty and there is
//    no need to call Clear() to use it again.

#ifndef CROCKS_WRITE_BATCH_H
#define CROCKS_WRITE_BATCH_H

#include <string>

#include <crocks/cluster.h>
#include <crocks/status.h>

namespace crocks {

class WriteBatch {
 public:
  explicit WriteBatch(Cluster* db);
  explicit WriteBatch(Cluster* db, int threshold_low, int threshold_high);
  ~WriteBatch();

  // Batch operations get inserted in a per-node buffer. Once the buffer exceeds
  // a certain threshold (kByteSizeThreshold defined in src/client/node.h), it
  // is sent asynchronously to the corresponding node. If there is a pending
  // request for a previous buffer, to the same node, for which there is no
  // response yet, the call blocks.
  void Put(const std::string& key, const std::string& value);
  void Delete(const std::string& key);
  void SingleDelete(const std::string& key);
  void Merge(const std::string& key, const std::string& value);
  void Clear();

  // Write() sends the remaining buffers (again waiting for any pending requests
  // to be completed) followed by a commit request, simultaneously. Then, blocks
  // waiting for every single server to respond.
  Status Write();

  // The same as Write, but wrap the request in a lock. This ensures that
  // batches get committed in the same order at each node.
  Status WriteWithLock();

 private:
  class WriteBatchImpl;
  WriteBatchImpl* const impl_;
  // No copying allowed
  WriteBatch(const WriteBatch&) = delete;
  void operator=(const WriteBatch&) = delete;
};

}  // namespace crocks

#endif  // CROCKS_WRITE_BATCH_H
