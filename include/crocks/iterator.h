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

// Wrapper around rocksdb::Iterator

#ifndef CROCKS_ITERATOR_H
#define CROCKS_ITERATOR_H

#include <string>

#include <grpc++/grpc++.h>

#include <crocks/status.h>
#include <crocks/write_batch.h>
#include "gen/crocks.pb.h"

namespace crocks {

class Cluster;

class Iterator {
 public:
  explicit Iterator(Cluster* db);
  ~Iterator();

  // Accessors (i.e const functions) are guaranteed not to block. Seeks always
  // block until every node has responded. For Next() and Prev() we try hard
  // not to block by requesting early the next keys, and hoping that they will
  // have arrived until the buffers are empty, but that's not always possible.
  bool Valid() const;
  void SeekToFirst();
  void SeekToLast();
  void Seek(const std::string& target);
  void SeekForPrev(const std::string& target);
  void Next();
  void Prev();
  std::string key() const;
  std::string value() const;
  Status status() const;

 private:
  class IteratorImpl;
  IteratorImpl* const impl_;
  // No copying allowed
  Iterator(const Iterator&) = delete;
  void operator=(const Iterator&) = delete;
};

}  // namespace crocks

#endif  // CROCKS_ITERATOR_H
