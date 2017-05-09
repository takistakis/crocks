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
#include "gen/etcd.pb.h"

namespace crocks {

struct WatchCall {
  int id;
  etcdserverpb::WatchRequest request;
  etcdserverpb::WatchResponse response;
  grpc::ClientContext context;
  std::unique_ptr<grpc::ClientReaderWriter<etcdserverpb::WatchRequest,
                                           etcdserverpb::WatchResponse>>
      stream;
};

class EtcdClient {
 public:
  EtcdClient(const std::string& address);

  // Return 0 if the key was not found, or the revision of
  // the last modification on this key if it was found.
  int Get(const std::string& key, std::string* value);
  void Put(const std::string& key, const std::string& value);
  // Return 0 if the key was not found, or 1 if it was found.
  int Delete(const std::string& key);
  bool KeyMissing(const std::string& key);

  bool TxnPutIfValueEquals(const std::string& key, const std::string& value,
                           const std::string& old_value);
  bool TxnPutIfKeyMissing(const std::string& key, const std::string& value);

  // Store the current value of "key" in *value, and start watching
  // it from the next revision. Return a pointer to the WatchCall
  // instance associated with that call (cast as void*), which
  // will need to be provided for getting updates and canceling.
  void* Watch(const std::string& key, std::string* value);

  // Wait for a response and return true if it responds to a cancel
  // request. If not, store the value of the key being watched in *value.
  bool WatchNext(void* call, std::string* value);

  // Send a request to cancel the watch call. After canceling,
  // WatchNext should be called until it returns true. This must happen
  // in a different thread, otherwise WatchCancel blocks forever.
  void WatchCancel(void* call);

  // Send a request to cancel the watch call,
  // wait for the response and finish the RPC.
  void WatchEnd(void* call);

  void Lock();
  void Unlock();

 private:
  std::shared_ptr<grpc::Channel> channel_;
  std::unique_ptr<etcdserverpb::KV::Stub> kv_stub_;
  std::unique_ptr<etcdserverpb::Watch::Stub> watch_stub_;
};

}  // namespace crocks

#endif  // CROCKS_COMMON_ETCD_CLIENT_H
