//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef CROCKS_COMMON_HASH_H
#define CROCKS_COMMON_HASH_H

#include <stddef.h>
#include <stdint.h>

#include <string>

namespace crocks {

uint32_t MurmurHash(const char* data, size_t n, uint32_t seed);

inline uint32_t Hash(const std::string& string) {
  return MurmurHash(string.c_str(), string.size(), 0xACABACAB);
}

}  // namespace crocks

#endif  // CROCKS_COMMON_HASH_H
