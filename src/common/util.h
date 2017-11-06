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

#ifndef CROCKS_COMMON_UTIL_H
#define CROCKS_COMMON_UTIL_H

#include <functional>
#include <string>

#include <grpc++/grpc++.h>

namespace crocks {

// Make RPC and if it fails with status UNAVAILABLE, retry once
grpc::Status Ensure(std::function<grpc::Status(grpc::ClientContext*)> rpc);

// Get environment variable, and return whether it was set
bool GetEnv(const char* name, std::string* value);

// Return the etcd endpoint from ETCD_ENDPOINT environment variable,
// and fall back to the default (localhost:2379) if it's not set.
std::string GetEtcdEndpoint();

}  // namespace crocks

#endif  // CROCKS_COMMON_UTIL_H
