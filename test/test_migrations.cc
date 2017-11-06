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

// Repeatedly write keys and read them back. This is supposed to be running with
// random migrations happening from time to time and it should never report
// unexpected results. Optionally a prefix can be given as an argument so that
// two or more clients can be writing at the same time without interfering.

#include <stdio.h>

#include <iostream>
#include <string>

#include <crocks/cluster.h>
#include <crocks/status.h>
#include "src/common/util.h"

#include "util.h"

const int kSize = 5000;

void yo(crocks::Cluster* db, const std::string& prefix, int i) {
  for (int j = 0; j < kSize; j++) {
    char key[6];
    sprintf(key, "%05d", j);
    std::string value;
    EnsureRpc(db->Get(prefix + key, &value));
    if (value != std::to_string(i - 1))
      std::cout << "expected " << i - 1 << " got " << value << std::endl;
    EnsureRpc(db->Put(prefix + key, std::to_string(i)));
    EnsureRpc(db->Get(prefix + key, &value));
    if (value != std::to_string(i))
      std::cout << "put " << i << " got " << value << std::endl;
  }
}

int main(int argc, char** argv) {
  crocks::Cluster* db = crocks::DBOpen(crocks::GetEtcdEndpoint());

  std::string prefix = (argc == 1) ? "" : argv[1];

  // Fill with zeros
  for (int i = 0; i < kSize; i++) {
    char key[6];
    sprintf(key, "%05d", i);
    EnsureRpc(db->Put(prefix + key, "0"));
  }

  for (int i = 1; i < 10000; i++)
    Measure(yo, db, prefix, i);

  delete db;

  return 0;
}
