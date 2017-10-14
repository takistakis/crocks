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

#include <grpc++/grpc++.h>

namespace crocks {

// Make RPC and if it fails with status UNAVAILABLE, retry once
grpc::Status Ensure(std::function<grpc::Status(grpc::ClientContext*)> rpc);

}  // namespace crocks

#endif  // CROCKS_COMMON_UTIL_H
