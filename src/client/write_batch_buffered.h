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

#ifndef CROCKS_CLIENT_WRITE_BATCH_BUFFERED_H
#define CROCKS_CLIENT_WRITE_BATCH_BUFFERED_H

#include <string>

#include "gen/crocks.pb.h"

namespace crocks {

class WriteBatchBuffered {
 public:
  void Put(const std::string& key, const std::string& value);
  void Delete(const std::string& key);
  void SingleDelete(const std::string& key);
  void Merge(const std::string& key, const std::string& value);
  void Clear();

  pb::BatchBuffer request() const {
    return request_;
  }

 private:
  pb::BatchBuffer request_;
};

}  // namespace crocks

#endif  // CROCKS_CLIENT_WRITE_BATCH_BUFFERED_H
