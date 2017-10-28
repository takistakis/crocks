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

#ifndef CROCKS_OPTIONS_H
#define CROCKS_OPTIONS_H

namespace crocks {

struct Options {
  // If true, when a node is detected to be unavailable
  // and etcd is not yet aware, the client updates it.
  bool inform_on_unavailable = true;

  // If true, after a status UNAVAILABLE is received, the client waits
  // until the cluster is healthy again, and then retries the request.
  bool wait_on_unhealthy = true;
};

}  // namespace crocks

#endif  // CROCKS_OPTIONS_H
