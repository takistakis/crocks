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

#ifndef CROCKS_COMMON_ETCD_UTIL_H
#define CROCKS_COMMON_ETCD_UTIL_H

#include <string>

namespace etcdserverpb {
class TxnRequest;
class WatchRequest;
}

namespace crocks {

// Transaction helpers
void AddCompareValueEquals(const std::string& key, const std::string& value,
                           etcdserverpb::TxnRequest* request);
void AddCompareKeyMissing(const std::string& key,
                          etcdserverpb::TxnRequest* request);
void AddSuccessPut(const std::string& key, const std::string& value,
                   etcdserverpb::TxnRequest* request);

// Watch helpers
void WatchKeyRequest(const std::string& key,
                     etcdserverpb::WatchRequest* request);
void WatchKeyRequest(const std::string& key, int start_revision,
                     etcdserverpb::WatchRequest* request);
void WatchCancelRequest(int id, etcdserverpb::WatchRequest* request);

}  // namespace crocks

#endif  // CROCKS_COMMON_ETCD_UTIL_H
