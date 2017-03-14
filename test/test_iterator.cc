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

#include <assert.h>

#include <iostream>
#include <string>
#include <vector>

#include <crocks/cluster.h>
#include <crocks/iterator.h>
#include <crocks/status.h>
#include <crocks/write_batch.h>

int main() {
  std::vector<std::string> addresses = {"localhost:50051", "localhost:50052",
                                        "localhost:50053", "localhost:50054"};
  crocks::Cluster* db = crocks::DBOpen(addresses);

  // Put some key-values in the database in random order
  {
    crocks::WriteBatch batch(db);
    batch.Put("h", "h1");
    batch.Put("d", "d1");
    batch.Put("c", "c1");
    batch.Put("b", "b1");
    batch.Put("j", "j1");
    batch.Put("a", "a1");
    batch.Put("e", "e1");
    batch.Put("g", "g1");
    batch.Put("f", "f1");
    batch.Put("i", "i1");
    EnsureRpc(batch.Write());
  }

  // https://github.com/facebook/rocksdb/wiki/Basic-Operations#iteration
  crocks::Iterator* it;

  // Print key-values in ascending order
  it = new crocks::Iterator(db);
  for (it->SeekToFirst(); it->Valid(); it->Next())
    std::cout << it->key() << ": " << it->value() << std::endl;
  assert(it->status().ok());

  // Do a random seek
  it->Seek("d");
  assert(it->value() == "d1");

  // Print key-values in descending order
  for (it->SeekToLast(); it->Valid(); it->Prev())
    std::cout << it->key() << ": " << it->value() << std::endl;
  assert(it->status().ok());

  // Do some random direction changes
  it->Seek("e");
  assert(it->value() == "e1");
  it->Prev();
  assert(it->value() == "d1");
  it->Prev();
  assert(it->value() == "c1");
  it->Next();
  assert(it->value() == "d1");
  it->Next();
  assert(it->value() == "e1");
  it->Next();
  assert(it->value() == "f1");

  // Delete every key
  for (it->SeekToFirst(); it->Valid(); it->Next())
    EnsureRpc(db->Delete(it->key()));
  delete it;

  // Create a new iterator to get a recent snapshot of the
  // database, and check that the database is indeed empty.
  it = new crocks::Iterator(db);
  it->SeekToFirst();
  assert(!it->Valid());
  delete it;

  // Do 1000 puts
  {
    crocks::WriteBatch batch(db);
    for (int i = 1000; i < 2000; i++)
      batch.Put(std::to_string(i), "asdf" + std::to_string(i));
    EnsureRpc(batch.Write());
  }

  // Check that the database has 1000 items
  it = new crocks::Iterator(db);
  int i = 1000;
  for (it->SeekToFirst(); it->Valid(); it->Next()) {
    assert(it->key() == std::to_string(i));
    assert(it->value() == "asdf" + std::to_string(i));
    i++;
  }
  assert(i == 2000);

  // Delete everything
  for (it->SeekToFirst(); it->Valid(); it->Next())
    EnsureRpc(db->Delete(it->key()));
  delete it;

  // Check that the database is empty
  it = new crocks::Iterator(db);
  it->SeekToFirst();
  assert(!it->Valid());
  delete it;

  delete db;

  return 0;
}
