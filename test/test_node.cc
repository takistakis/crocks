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

#include <iostream>
#include <string>

#include <crocks/status.h>
#include "src/client/node.h"
#include "src/client/sync_write_batch.h"
#include "src/client/write_batch_buffered.h"
#include "src/client/write_batch_streaming.h"

#include "util.h"

inline void TestGet(crocks::Node* db, const std::string& key) {
  crocks::Status status;
  std::string value;

  std::cout << "get('" + key + "') -> ";
  status = db->Get(key, &value);
  EnsureRpc(status);

  if (status.IsNotFound()) {
    std::cout << "not found" << std::endl;
  } else {
    std::cout << "'" + value + "'" << std::endl;
  }
}

inline void TestPut(crocks::Node* db, const std::string& key,
                    const std::string& value) {
  std::cout << "put('" + key + "', '" + value + "')" << std::endl;
  EnsureRpc(db->Put(key, value));
}

inline void TestDelete(crocks::Node* db, const std::string& key) {
  std::cout << "delete('" + key + "')" << std::endl;
  EnsureRpc(db->Delete(key));
}

inline void TestMultiPut(crocks::Node* db) {
  std::cout << "Starting 5000 puts" << std::endl;
  for (int i = 0; i < 5000; i++)
    db->Put("yo" + std::to_string(i), "yoyoyoyo" + std::to_string(i));
}

inline void TestStreamingBatch(crocks::Node* db) {
  crocks::WriteBatchStreaming batch(db);

  std::cout << "Starting 75000 streaming batch puts" << std::endl;
  for (int i = 0; i < 75000; i++)
    batch.Put("yo" + std::to_string(i), "yoyoyoyo" + std::to_string(i));
  EnsureRpc(batch.Write());
}

inline void TestSingleBatch(crocks::Node* db) {
  crocks::WriteBatchBuffered batch;

  std::cout << "Starting ~750.000 buffered batch puts (three 4MB messages)"
            << std::endl;
  for (int j = 0; j < 3; j++) {
    for (int i = 0; i < 4 * 64 * 1024; i++)
      batch.Put("yo", "yoyoyoyo");
    EnsureRpc(db->Write(batch));
    batch.Clear();
  }
}

inline void TestBatch(crocks::Node* db) {
  crocks::SyncWriteBatch batch(db);

  std::cout << "Starting 1.500.000 batch puts" << std::endl;
  for (int i = 0; i < 1500000; i++)
    batch.Put("yo" + std::to_string(i), "yoyoyoyo" + std::to_string(i));
  EnsureRpc(batch.Write());
}

inline void TestBatchRandom(crocks::Node* db) {
  crocks::SyncWriteBatch batch(db);

  std::cout << "Starting 1.500.000 random batch puts" << std::endl;
  for (int i = 0; i < 1500000; i++)
    batch.Put(RandomKey(), RandomValue());
  EnsureRpc(batch.Write());
}

int main() {
  RandomInit();
  crocks::Node* db = crocks::DBOpen("localhost:50051");

  TestPut(db, "key", "value");
  TestGet(db, "key");
  TestDelete(db, "key");
  TestGet(db, "key");
  std::cout << std::endl;

  Measure(TestMultiPut, db);
  std::cout << std::endl;

  Measure(TestStreamingBatch, db);
  std::cout << std::endl;

  Measure(TestSingleBatch, db);
  std::cout << std::endl;

  Measure(TestBatch, db);
  std::cout << std::endl;

  Measure(TestBatchRandom, db);
  std::cout << std::endl;

  delete db;

  return 0;
}
