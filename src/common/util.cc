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

#include "src/common/util.h"

#include <stdlib.h>

#include <chrono>
#include <iostream>

namespace crocks {

grpc::Status Ensure(std::function<grpc::Status(grpc::ClientContext*)> rpc,
                    const std::string& what) {
  grpc::ClientContext context;
  grpc::Status status = rpc(&context);
  if (status.error_code() == grpc::StatusCode::UNAVAILABLE) {
    std::cerr << what << " failed. Retrying..." << std::endl;
    grpc::ClientContext retry_context;
    std::chrono::system_clock::time_point deadline =
        std::chrono::system_clock::now() + std::chrono::milliseconds(500);
    retry_context.set_deadline(deadline);
    grpc::Status retry_status = rpc(&retry_context);
    if (retry_status.error_code() != grpc::StatusCode::DEADLINE_EXCEEDED)
      status = retry_status;
  }
  return status;
}

bool GetEnv(const char* name, std::string* value) {
  char* tmp = secure_getenv(name);
  if (tmp == NULL)
    return false;
  *value = tmp;
  return true;
}

std::string GetEtcdEndpoint() {
  std::string value;
  return GetEnv("ETCD_ENDPOINT", &value) ? value : "localhost:2379";
}

}  // namespace crocks
