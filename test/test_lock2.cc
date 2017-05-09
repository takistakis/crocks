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

// Write a bunch of keys with value "1" in one thread and "2" in
// another in parallel, with and without locking the database.
// When writing with the lock, there should never be mixed values.

#include <assert.h>

#include <iostream>
#include <string>
#include <thread>

#include <crocks/cluster.h>
#include <crocks/iterator.h>
#include <crocks/status.h>
#include <crocks/write_batch.h>

const int kNumItems = 10000;

void Thread(const std::string& value, bool lock) {
  crocks::Cluster* db = crocks::DBOpen("localhost:2379");
  crocks::WriteBatch batch(db);
  for (int i = 0; i < kNumItems; i++)
    batch.Put(std::to_string(i), value);
  if (lock)
    EnsureRpc(batch.WriteWithLock());
  else
    EnsureRpc(batch.Write());
  delete db;
}

int CountOnes() {
  int i = 0;
  crocks::Cluster* db = crocks::DBOpen("localhost:2379");
  crocks::Iterator* it = new crocks::Iterator(db);
  for (it->SeekToFirst(); it->Valid(); it->Next())
    if (it->value() == "1")
      i++;
  assert(it->status().ok());
  delete it;
  delete db;
  return i;
}

int main() {
  for (int i = 0; i < 20; i++) {
    std::thread t1(Thread, "1", false);
    std::thread t2(Thread, "2", false);
    t1.join();
    t2.join();
    int num = CountOnes();
    std::cout << "Without lock: ";
    if ((num != 0) && (num != kNumItems))
      std::cout << "FAILED\t";
    else
      std::cout << "OK\t";

    std::thread t3(Thread, "1", true);
    std::thread t4(Thread, "2", true);
    t3.join();
    t4.join();
    num = CountOnes();
    std::cout << "With lock: ";
    if ((num != 0) && (num != kNumItems))
      std::cout << "FAILED" << std::endl;
    else
      std::cout << "OK" << std::endl;
  }

  return 0;
}
