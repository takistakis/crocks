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

#include "src/common/etcd_util.h"

#include "gen/etcd.pb.h"

namespace crocks {

void AddCompareValueEquals(const std::string& key, const std::string& value,
                           etcdserverpb::TxnRequest* request) {
  etcdserverpb::Compare* compare = request->add_compare();
  compare->set_key(key);
  compare->set_target(etcdserverpb::Compare::VALUE);
  compare->set_result(etcdserverpb::Compare::EQUAL);
  compare->set_value(value);
}

void AddCompareKeyMissing(const std::string& key,
                          etcdserverpb::TxnRequest* request) {
  etcdserverpb::Compare* compare = request->add_compare();
  compare->set_key(key);
  compare->set_target(etcdserverpb::Compare::VERSION);
  compare->set_result(etcdserverpb::Compare::EQUAL);
  compare->set_version(0);
}

void AddSuccessPut(const std::string& key, const std::string& value,
                   etcdserverpb::TxnRequest* request) {
  etcdserverpb::RequestOp* request_op = request->add_success();
  etcdserverpb::PutRequest* put_request = new etcdserverpb::PutRequest;
  put_request->set_key(key);
  put_request->set_value(value);
  request_op->set_allocated_request_put(put_request);
}

void WatchKeyRequest(const std::string& key,
                     etcdserverpb::WatchRequest* request) {
  etcdserverpb::WatchCreateRequest* create_request;
  create_request = new etcdserverpb::WatchCreateRequest;
  create_request->set_key(key);
  request->set_allocated_create_request(create_request);
}

void WatchKeyRequest(const std::string& key, int start_revision,
                     etcdserverpb::WatchRequest* request) {
  etcdserverpb::WatchCreateRequest* create_request;
  create_request = new etcdserverpb::WatchCreateRequest;
  create_request->set_key(key);
  create_request->set_start_revision(start_revision);
  request->set_allocated_create_request(create_request);
}

void WatchCancelRequest(int id, etcdserverpb::WatchRequest* request) {
  etcdserverpb::WatchCancelRequest* cancel_request;
  cancel_request = new etcdserverpb::WatchCancelRequest;
  cancel_request->set_watch_id(id);
  request->set_allocated_cancel_request(cancel_request);
}

}  // namespace crocks
