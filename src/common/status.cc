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

#include <crocks/status.h>

#include <assert.h>
#include <stdlib.h>

#include <iostream>

namespace crocks {

std::string RocksdbStatusDescription(const rocksdb::StatusCode& code) {
  switch (code) {
    case rocksdb::StatusCode::OK:
      return "OK";
    case rocksdb::StatusCode::NOT_FOUND:
      return "Not found";
    case rocksdb::StatusCode::CORRUPTION:
      return "Corruption";
    case rocksdb::StatusCode::NOT_SUPPORTED:
      return "Not supported";
    case rocksdb::StatusCode::INVALID_ARGUMENT:
      return "Invalid argument";
    case rocksdb::StatusCode::IO_ERROR:
      return "IO error";
    case rocksdb::StatusCode::MERGE_IN_PROGRESS:
      return "Merge in progress";
    case rocksdb::StatusCode::INCOMPLETE:
      return "Incomplete";
    case rocksdb::StatusCode::SHUTDOWN_IN_PROGRESS:
      return "Shutdown in progress";
    case rocksdb::StatusCode::TIMED_OUT:
      return "Timed out";
    case rocksdb::StatusCode::ABORTED:
      return "Aborted";
    case rocksdb::StatusCode::BUSY:
      return "Busy";
    case rocksdb::StatusCode::EXPIRED:
      return "Expired";
    case rocksdb::StatusCode::TRY_AGAIN:
      return "Try again";
    default:
      assert(false);
  }
}

std::string RocksdbStatusDescription(int code) {
  return RocksdbStatusDescription(static_cast<rocksdb::StatusCode>(code));
}

void EnsureRpc(const Status& status) {
  if (!status.grpc_ok()) {
    std::cerr << "RPC failed with status " << status.grpc_code() << " ("
              << status.error_message() << ")" << std::endl;
    exit(EXIT_FAILURE);
  }
}

void EnsureRpc(const grpc::Status& status) {
  if (!status.ok()) {
    std::cerr << "RPC failed with status " << status.error_code() << " ("
              << status.error_message() << ")" << std::endl;
    exit(EXIT_FAILURE);
  }
}

}  // namespace crocks
