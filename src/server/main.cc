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

#include <assert.h>
#include <getopt.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include <iostream>
#include <memory>
#include <string>

#include <grpc++/grpc++.h>

#include "src/server/server.h"

// Global because it is used by the signal handler
std::unique_ptr<grpc::Server> server;

const std::string version("crocks v0.1.0");
const std::string usage_message(
    "Usage: crocks [options]\n"
    "\n"
    "Start a crocks server.\n"
    "\n"
    "Options:\n"
    "  -p, --path <path>  RocksDB database path.\n"
    "  -P, --port <port>  Listening port [default: chosen by OS].\n"
    "  -d, --daemon       Daemonize process.\n"
    "  -v, --version      Show version and exit.\n"
    "  -h, --help         Show this help message and exit.\n");

void signal_handler(int signum) {
  switch (signum) {
    case SIGINT:
      std::cout << "Caught SIGINT" << std::endl;
      break;
    case SIGTERM:
      std::cout << "Caught SIGTERM" << std::endl;
      break;
    default:
      assert(false);
      break;
  }
  server->Shutdown();
}

int main(int argc, char** argv) {
  std::string address;
  std::string port = "0";
  char* dbpath = nullptr;
  const char* optstring = "p:P:dvh";
  static struct option longopts[] = {
      // clang-format off
      {"path",    required_argument, 0, 'p'},
      {"port",    required_argument, 0, 'P'},
      {"daemon",  no_argument,       0, 'd'},
      {"version", no_argument,       0, 'v'},
      {"help",    no_argument,       0, 'h'},
      {0, 0, 0, 0},
      // clang-format on
  };
  int c, index = 0;

  // Command-line options
  while ((c = getopt_long(argc, argv, optstring, longopts, &index)) != -1) {
    switch (c) {
      case 'p':
        dbpath = optarg;
        break;
      case 'P':
        port = optarg;
        break;
      case 'd':
        if (daemon(0, 0) < 0) {
          perror("daemon");
          exit(EXIT_FAILURE);
        }
        break;
      case 'v':
        std::cout << version << std::endl;
        exit(EXIT_SUCCESS);
      case 'h':
        std::cout << usage_message;
        exit(EXIT_SUCCESS);
      default:
        std::cout << usage_message;
        exit(EXIT_FAILURE);
    }
  }

  if (dbpath == nullptr) {
    char dbpath_template[] = "/tmp/testdb_XXXXXX";
    dbpath = mkdtemp(dbpath_template);
    if (dbpath == nullptr) {
      perror("mkdtemp");
      exit(EXIT_FAILURE);
    }
  }

  address = "0.0.0.0:" + port;

  // Signal handling
  // XXX: Might not be properly setup
  struct sigaction sa;
  sigemptyset(&sa.sa_mask);
  sa.sa_flags = 0;
  sa.sa_handler = signal_handler;

  if (sigaction(SIGINT, &sa, NULL) < 0) {
    perror("sigaction");
    exit(EXIT_FAILURE);
  }

  if (sigaction(SIGTERM, &sa, NULL) < 0) {
    perror("sigaction");
    exit(EXIT_FAILURE);
  }

  // Start sync server
  crocks::Service service(dbpath);
  grpc::ServerBuilder builder;
  builder.AddListeningPort(address, grpc::InsecureServerCredentials());
  builder.RegisterService(&service);
  server = builder.BuildAndStart();
  std::cout << "Server listening on " << address << std::endl;
  server->Wait();

  return 0;
}
