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

#include "src/client/write_batch_buffered.h"

namespace crocks {

void WriteBatchBuffered::Put(const std::string& key, const std::string& value) {
  // request_ is of type BatchBuffer. request_.add_updates() allocates a new
  // BatchUpdate. When the WriteBatchBuffered is ready we call
  // Node::Write(batch) and the node makes the appropriate RPC.
  pb::BatchUpdate* batch_update = request_.add_updates();
  batch_update->set_op(pb::BatchUpdate::PUT);
  batch_update->set_key(key);
  batch_update->set_value(value);
}

void WriteBatchBuffered::Delete(const std::string& key) {
  pb::BatchUpdate* batch_update = request_.add_updates();
  batch_update->set_op(pb::BatchUpdate::DELETE);
  batch_update->set_key(key);
}

void WriteBatchBuffered::SingleDelete(const std::string& key) {
  pb::BatchUpdate* batch_update = request_.add_updates();
  batch_update->set_op(pb::BatchUpdate::SINGLE_DELETE);
  batch_update->set_key(key);
}

void WriteBatchBuffered::Merge(const std::string& key,
                               const std::string& value) {
  pb::BatchUpdate* batch_update = request_.add_updates();
  batch_update->set_op(pb::BatchUpdate::MERGE);
  batch_update->set_key(key);
  batch_update->set_value(value);
}

void WriteBatchBuffered::Clear() {
  // Un-sets the updates field back to its empty state. It is the same as
  // request_.mutable_updates().Clear().
  request_.Clear();
}

}  // namespace crocks
