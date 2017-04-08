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

#include "src/common/etcd_client.h"

#include <crocks/status.h>
#include "src/common/etcd_util.h"

namespace crocks {

EtcdClient::EtcdClient(const std::string& address)
    : channel_(
          grpc::CreateChannel(address, grpc::InsecureChannelCredentials())),
      kv_stub_(etcdserverpb::KV::NewStub(channel_)),
      watch_stub_(etcdserverpb::Watch::NewStub(channel_)) {}

int EtcdClient::Get(const std::string& key, std::string* value) {
  etcdserverpb::RangeRequest request;
  etcdserverpb::RangeResponse response;
  grpc::ClientContext context;
  request.set_key(key);
  EnsureRpc(kv_stub_->Range(&context, request, &response));
  if (response.count() > 0)
    *value = response.kvs()[0].value();
  return response.count();
}

void EtcdClient::Put(const std::string& key, const std::string& value) {
  etcdserverpb::PutRequest request;
  etcdserverpb::PutResponse response;
  grpc::ClientContext context;
  request.set_key(key);
  request.set_value(value);
  EnsureRpc(kv_stub_->Put(&context, request, &response));
}

int EtcdClient::Delete(const std::string& key) {
  etcdserverpb::DeleteRangeRequest request;
  etcdserverpb::DeleteRangeResponse response;
  grpc::ClientContext context;
  request.set_key(key);
  EnsureRpc(kv_stub_->DeleteRange(&context, request, &response));
  return response.deleted();
}

bool EtcdClient::KeyMissing(const std::string& key) {
  etcdserverpb::RangeRequest request;
  etcdserverpb::RangeResponse response;
  grpc::ClientContext context;
  request.set_key(key);
  EnsureRpc(kv_stub_->Range(&context, request, &response));
  return response.count() == 0;
}

bool EtcdClient::TxnPutIfValueEquals(const std::string& key,
                                     const std::string& value,
                                     const std::string& old_value) {
  etcdserverpb::TxnRequest request;
  etcdserverpb::TxnResponse response;
  grpc::ClientContext context;
  AddCompareValueEquals(key, old_value, &request);
  AddSuccessPut(key, value, &request);
  EnsureRpc(kv_stub_->Txn(&context, request, &response));
  return response.succeeded();
}

bool EtcdClient::TxnPutIfKeyMissing(const std::string& key,
                                    const std::string& value) {
  etcdserverpb::TxnRequest request;
  etcdserverpb::TxnResponse response;
  grpc::ClientContext context;
  AddCompareKeyMissing(key, &request);
  AddSuccessPut(key, value, &request);
  EnsureRpc(kv_stub_->Txn(&context, request, &response));
  return response.succeeded();
}

}  // namespace crocks
