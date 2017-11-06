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

// Requires a cluster size of at least 2

#include <iostream>
#include <string>
#include <vector>

#include <crocks/status.h>
#include "src/client/node.h"
#include "src/common/info.h"
#include "src/common/util.h"

int main() {
  crocks::Info info(crocks::GetEtcdEndpoint());
  info.Get();

  // Find a key intended for node 0
  std::string key;
  int i = 0;
  do {
    key = std::to_string(i++);
  } while (info.IndexForKey(key) != 0);

  std::cout << "Sending key intended for node 0 to node 1" << std::endl;
  std::string address = info.Addresses()[1];
  crocks::Node* db = new crocks::Node(address);
  EnsureRpc(db->Put(key, "test"));

  delete db;

  return 0;
}
