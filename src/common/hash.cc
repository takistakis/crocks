//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

// Straight outta RocksDB (util/hash.cc)

#include "src/common/hash.h"

// NOTE: endian.h is not standard
#include <endian.h>
#include <string.h>

namespace crocks {

const bool kLittleEndian(__BYTE_ORDER == __LITTLE_ENDIAN);

inline uint32_t DecodeFixed32(const char* ptr) {
  if (kLittleEndian) {
    // Load the raw bytes
    uint32_t result;
    memcpy(&result, ptr, sizeof(result));  // gcc optimizes this to a plain load
    return result;
  } else {
    return ((static_cast<uint32_t>(static_cast<unsigned char>(ptr[0]))) |
            (static_cast<uint32_t>(static_cast<unsigned char>(ptr[1])) << 8) |
            (static_cast<uint32_t>(static_cast<unsigned char>(ptr[2])) << 16) |
            (static_cast<uint32_t>(static_cast<unsigned char>(ptr[3])) << 24));
  }
}

uint32_t MurmurHash(const char* data, size_t n, uint32_t seed) {
  const uint32_t m = 0xc6a4a793;
  const uint32_t r = 24;
  const char* limit = data + n;
  uint32_t h = static_cast<uint32_t>(seed ^ (n * m));

  // Pick up four bytes at a time
  while (data + 4 <= limit) {
    uint32_t w = DecodeFixed32(data);
    data += 4;
    h += w;
    h *= m;
    h ^= (h >> 16);
  }

  // Pick up remaining bytes
  switch (limit - data) {
    case 3:
      h += static_cast<uint32_t>(static_cast<unsigned char>(data[2]) << 16);
    // fall through
    case 2:
      h += static_cast<uint32_t>(static_cast<unsigned char>(data[1]) << 8);
    // fall through
    case 1:
      h += static_cast<uint32_t>(static_cast<unsigned char>(data[0]));
      h *= m;
      h ^= (h >> r);
      break;
  }
  return h;
}

}  // namespace crocks
