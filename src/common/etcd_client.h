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

// Custom etcd client using the etcdv3 gRPC API

#ifndef CROCKS_COMMON_ETCD_CLIENT_H
#define CROCKS_COMMON_ETCD_CLIENT_H

#include <memory>
#include <string>

#include <grpc++/grpc++.h>

#include "gen/etcd.grpc.pb.h"

namespace crocks {

class EtcdClient {
 public:
  EtcdClient(const std::string& address);

  // Returns the number of key-value pairs found. Since we
  // request a single key, it should be 1 if found or 0 if not.
  int Get(const std::string& key, std::string* value);
  void Put(const std::string& key, const std::string& value);
  int Delete(const std::string& key);
  bool KeyMissing(const std::string& key);

  bool TxnPutIfValueEquals(const std::string& key, const std::string& value,
                           const std::string& old_value);
  bool TxnPutIfKeyMissing(const std::string& key, const std::string& value);

 private:
  std::shared_ptr<grpc::Channel> channel_;
  std::unique_ptr<etcdserverpb::KV::Stub> kv_stub_;
  std::unique_ptr<etcdserverpb::Watch::Stub> watch_stub_;
};

}  // namespace crocks

#endif  // CROCKS_COMMON_ETCD_CLIENT_H
