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

#include <assert.h>

#include <crocks/status.h>
#include "gen/etcd_lock.pb.h"
#include "src/common/etcd_util.h"
#include "src/common/util.h"

namespace crocks {

EtcdClient::EtcdClient(const std::string& address)
    : channel_(
          grpc::CreateChannel(address, grpc::InsecureChannelCredentials())),
      kv_stub_(etcdserverpb::KV::NewStub(channel_)),
      watch_stub_(etcdserverpb::Watch::NewStub(channel_)),
      lock_stub_(v3lockpb::Lock::NewStub(channel_)) {}

int EtcdClient::Get(const std::string& key, std::string* value) {
  etcdserverpb::RangeRequest request;
  etcdserverpb::RangeResponse response;
  request.set_key(key);
  grpc::Status status = Ensure(
      [&](grpc::ClientContext* ctx) {
        return kv_stub_->Range(ctx, request, &response);
      },
      "EtcdClient::Get");
  EnsureRpc(status);
  if (response.count() == 0)
    return 0;
  assert(response.count() == 1);
  *value = response.kvs(0).value();
  return response.kvs(0).mod_revision();
}

void EtcdClient::Put(const std::string& key, const std::string& value) {
  etcdserverpb::PutRequest request;
  etcdserverpb::PutResponse response;
  request.set_key(key);
  request.set_value(value);
  grpc::Status status = Ensure(
      [&](grpc::ClientContext* ctx) {
        return kv_stub_->Put(ctx, request, &response);
      },
      "EtcdClient::Put");
  EnsureRpc(status);
}

int EtcdClient::Delete(const std::string& key) {
  etcdserverpb::DeleteRangeRequest request;
  etcdserverpb::DeleteRangeResponse response;
  request.set_key(key);
  grpc::Status status = Ensure(
      [&](grpc::ClientContext* ctx) {
        return kv_stub_->DeleteRange(ctx, request, &response);
      },
      "EtcdClient::Delete");
  EnsureRpc(status);
  return response.deleted();
}

bool EtcdClient::KeyMissing(const std::string& key) {
  etcdserverpb::RangeRequest request;
  etcdserverpb::RangeResponse response;
  request.set_key(key);
  grpc::Status status = Ensure(
      [&](grpc::ClientContext* ctx) {
        return kv_stub_->Range(ctx, request, &response);
      },
      "EtcdClient::KeyMissing");
  EnsureRpc(status);
  return response.count() == 0;
}

bool EtcdClient::TxnPutIfValueEquals(const std::string& key,
                                     const std::string& value,
                                     const std::string& old_value) {
  etcdserverpb::TxnRequest request;
  etcdserverpb::TxnResponse response;
  AddCompareValueEquals(key, old_value, &request);
  AddSuccessPut(key, value, &request);
  grpc::Status status = Ensure(
      [&](grpc::ClientContext* ctx) {
        return kv_stub_->Txn(ctx, request, &response);
      },
      "EtcdClient::TxnPutIfValueEquals");
  EnsureRpc(status);
  return response.succeeded();
}

bool EtcdClient::TxnPutIfKeyMissing(const std::string& key,
                                    const std::string& value) {
  etcdserverpb::TxnRequest request;
  etcdserverpb::TxnResponse response;
  AddCompareKeyMissing(key, &request);
  AddSuccessPut(key, value, &request);
  grpc::Status status = Ensure(
      [&](grpc::ClientContext* ctx) {
        return kv_stub_->Txn(ctx, request, &response);
      },
      "EtcdClient::TxnPutIfKeyMissing");
  EnsureRpc(status);
  return response.succeeded();
}

void* EtcdClient::Watch(const std::string& key, std::string* value) {
  WatchCall* call = new WatchCall;
  call->context =
      std::unique_ptr<grpc::ClientContext>(new grpc::ClientContext());
  call->stream = watch_stub_->Watch(call->context.get());
  // Make sure we have the latest update, and instruct etcd to
  // send updates starting from the last revision exclusive, so
  // that we don't miss any updates that took place in-between.
  int revision = Get(key, value);
  WatchKeyRequest(key, revision + 1, &call->request);
  call->stream->Write(call->request);
  call->stream->Read(&call->response);
  assert(call->response.created());
  call->id = call->response.watch_id();
  return call;
}

bool EtcdClient::WatchNext(void* _call, std::string* value) {
  WatchCall* call = static_cast<WatchCall*>(_call);
  if (!call->stream->Read(&call->response)) {
    grpc::Status status = call->stream->Finish();
    // If the read failed because the server is
    // unavailable, try to start a new watch, else die.
    if (status.error_code() == grpc::StatusCode::UNAVAILABLE) {
      call->context =
          std::unique_ptr<grpc::ClientContext>(new grpc::ClientContext());
      call->stream = watch_stub_->Watch(call->context.get());
    } else {
      EnsureRpc(status);
    }
  }
  if (call->response.canceled())
    return true;
  // XXX: We get the latest value and ignore the
  // rest. There should be no problem with that.
  int size = call->response.events_size();
  const auto& event = call->response.events(size - 1);
  *value = event.kv().value();
  return false;
}

void EtcdClient::WatchCancel(void* _call) {
  WatchCall* call = static_cast<WatchCall*>(_call);
  WatchCancelRequest(call->id, &call->request);
  call->stream->Write(call->request);
  call->stream->WritesDone();
}

void EtcdClient::WatchEnd(void* _call) {
  WatchCall* call = static_cast<WatchCall*>(_call);
  call->context->TryCancel();
  grpc::Status status = call->stream->Finish();
  assert(status.error_code() == grpc::StatusCode::CANCELLED);
  assert(!call->stream->Read(&call->response));
  delete call;
}

void EtcdClient::Lock() {
  v3lockpb::LockRequest request;
  v3lockpb::LockResponse response;
  grpc::ClientContext context;
  request.set_name("lock");
  EnsureRpc(lock_stub_->Lock(&context, request, &response));
  lock_key_ = response.key();
}

void EtcdClient::Unlock() {
  v3lockpb::UnlockRequest request;
  v3lockpb::UnlockResponse response;
  grpc::ClientContext context;
  request.set_key(lock_key_);
  EnsureRpc(lock_stub_->Unlock(&context, request, &response));
}

}  // namespace crocks
