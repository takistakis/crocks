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

#include <chrono>
#include <iostream>
#include <thread>

#include "src/common/util.h"

void Thread() {
  crocks::EtcdClient etcd(crocks::GetEtcdEndpoint());

  std::cout << "thread 2: trying to get lock" << std::endl;
  etcd.Lock();
  std::cout << "thread 2: got lock" << std::endl;
  etcd.Unlock();
}

int main() {
  crocks::EtcdClient etcd(crocks::GetEtcdEndpoint());

  etcd.Lock();
  std::cout << "thread 1: got lock" << std::endl;
  etcd.Unlock();
  std::cout << "thread 1: released lock" << std::endl;
  etcd.Lock();
  std::cout << "thread 1: got lock" << std::endl;

  std::thread t(Thread);
  std::this_thread::sleep_for(std::chrono::milliseconds(1000));
  etcd.Unlock();
  std::cout << "thread 1: released lock" << std::endl;
  t.join();

  return 0;
}
