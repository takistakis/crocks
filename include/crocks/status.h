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

#ifndef CROCKS_STATUS_H
#define CROCKS_STATUS_H

#include <string>

#include <grpc++/grpc++.h>

namespace rocksdb {
// Must match rocksdb::Status::Code enum from <rocksdb/status.h>
enum StatusCode {
  OK = 0,
  NOT_FOUND = 1,
  CORRUPTION = 2,
  NOT_SUPPORTED = 3,
  INVALID_ARGUMENT = 4,
  IO_ERROR = 5,
  MERGE_IN_PROGRESS = 6,
  INCOMPLETE = 7,
  SHUTDOWN_IN_PROGRESS = 8,
  TIMED_OUT = 9,
  ABORTED = 10,
  BUSY = 11,
  EXPIRED = 12,
  TRY_AGAIN = 13,
  UNKNOWN = -1,
};
}

namespace crocks {

std::string RocksdbStatusDescription(const rocksdb::StatusCode& code);
std::string RocksdbStatusDescription(int code);

class Status {
 public:
  // Default all good status
  Status()
      : grpc_code_(grpc::StatusCode::OK),
        rocksdb_code_(rocksdb::StatusCode::OK) {}

  // gRPC failure, RocksDB status is unknown
  explicit Status(const grpc::Status& grpc_status)
      : grpc_code_(grpc_status.error_code()),
        rocksdb_code_(rocksdb::StatusCode::UNKNOWN),
        error_message_(grpc_status.error_message()) {}

  // gRPC success, RocksDB status set accordingly
  explicit Status(int rocksdb_code)
      : grpc_code_(grpc::StatusCode::OK),
        rocksdb_code_(static_cast<rocksdb::StatusCode>(rocksdb_code)),
        error_message_(RocksdbStatusDescription(rocksdb_code_)) {}

  Status(const grpc::Status& grpc_status, int rocksdb_code) {
    if (grpc_status.ok()) {
      grpc_code_ = grpc::StatusCode::OK;
      rocksdb_code_ = static_cast<rocksdb::StatusCode>(rocksdb_code);
      error_message_ = RocksdbStatusDescription(rocksdb_code_);
    } else {
      grpc_code_ = grpc_status.error_code();
      rocksdb_code_ = rocksdb::StatusCode::UNKNOWN;
      error_message_ = grpc_status.error_message();
    }
  }

  bool ok() const {
    return grpc_code_ == grpc::StatusCode::OK &&
           rocksdb_code_ == rocksdb::StatusCode::OK;
  }

  bool grpc_ok() const {
    return grpc_code_ == grpc::StatusCode::OK;
  }

  grpc::StatusCode grpc_code() const {
    return grpc_code_;
  }

  bool rocksdb_ok() const {
    return rocksdb_code_ == rocksdb::StatusCode::OK;
  }

  rocksdb::StatusCode rocksdb_code() const {
    return rocksdb_code_;
  }

  std::string error_message() const {
    return error_message_;
  }

  bool IsNotFound() const {
    return rocksdb_code_ == rocksdb::StatusCode::NOT_FOUND;
  }

  bool IsUnavailable() const {
    return grpc_code_ == grpc::StatusCode::UNAVAILABLE;
  }

 private:
  grpc::StatusCode grpc_code_;
  rocksdb::StatusCode rocksdb_code_;
  std::string error_message_;
};

// Exit unsuccessfully in case of gRPC failure
void EnsureRpc(const Status& status);
void EnsureRpc(const grpc::Status& status);

}  // namespace crocks

#endif  // CROCKS_STATUS_H
