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

#ifndef CROCKS_COMMON_LOCK_H
#define CROCKS_COMMON_LOCK_H

#include <mutex>
#if __cplusplus >= 201703L
#include <shared_mutex>
#endif

namespace crocks {

#if __cplusplus >= 201703L
typedef std::shared_mutex shared_mutex;
typedef std::lock_guard<std::shared_mutex> write_lock;
typedef std::shared_lock<std::shared_mutex> read_lock;
#else
typedef std::mutex shared_mutex;
typedef std::lock_guard<std::mutex> write_lock;
typedef std::lock_guard<std::mutex> read_lock;
#endif

}  // namespace crocks

#endif  // CROCKS_COMMON_LOCK_H
