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

inline void TestGet(crocks::Cluster* db, const std::string& key) {
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

inline void TestPut(crocks::Cluster* db, const std::string& key,
                    const std::string& value) {
  std::cout << "put('" + key + "', '" + value + "')" << std::endl;
  EnsureRpc(db->Put(key, value));
}

inline void TestDelete(crocks::Cluster* db, const std::string& key) {
  std::cout << "delete('" + key + "')" << std::endl;
  EnsureRpc(db->Delete(key));
}

int main() {
  crocks::Cluster* db = crocks::DBOpen("localhost:2379");

  TestPut(db, "asdf", "asdf");
  TestGet(db, "asdf");

  TestPut(db, "hoho", "hoho");
  TestGet(db, "hoho");

  TestPut(db, "test", "test");
  TestGet(db, "test");

  TestDelete(db, "hoho");
  TestGet(db, "hoho");

  delete db;

  return 0;
}
