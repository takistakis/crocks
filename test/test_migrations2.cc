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

#include <crocks/cluster.h>
#include <crocks/status.h>
#include <crocks/write_batch.h>
#include "src/common/util.h"

#include "util.h"

inline void TestBatch(crocks::Cluster* db) {
  crocks::WriteBatch batch(db);

  std::cout << "Starting 1.000.000 batch puts" << std::endl;
  for (int i = 0; i < 1000000; i++)
    batch.Put("yo" + std::to_string(i), "yoyoyoyo" + std::to_string(i));
  EnsureRpc(batch.Write());
}

int main() {
  RandomInit();
  crocks::Cluster* db = crocks::DBOpen(crocks::GetEtcdEndpoint());

  for (int i = 0; i < 1000; i++) {
    Measure(TestBatch, db);
    std::cout << std::endl;
  }

  delete db;

  return 0;
}
