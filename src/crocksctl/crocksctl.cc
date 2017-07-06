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

#include <getopt.h>
#include <stdlib.h>

#include <iostream>
#include <string>

#include <crocks/cluster.h>
#include <crocks/iterator.h>
#include <crocks/status.h>
#include <crocks/write_batch.h>
#include "src/client/node.h"
#include "src/common/info.h"

const std::string usage_message(
    "Usage: crocksctl [options] command [args]...\n"
    "\n"
    "A simple command line client for crocks.\n"
    "\n"
    "Commands:\n"
    "  get <key>          Get key.\n"
    "  put <key> <value>  Put key.\n"
    "  del <key>          Delete key.\n"
    "  run                Change cluster state to RUNNING.\n"
    "  migrate            Change cluster state to MIGRATING.\n"
    "  health             Check the health of the cluster.\n"
    "  list               Print every key.\n"
    "  dump               Print every key-value pair.\n"
    "  clear              Delete all keys.\n"
    "  remove <id>        Remove node from the cluster.\n"
    "  info               Print cluster info.\n"
    "\n"
    "Options:\n"
    "  -e, --etcd <address>  Etcd address [default: localhost:2379].\n"
    "  -h, --help            Show this help message and exit.\n");

void EnsureArguments(bool expected) {
  if (!expected) {
    std::cout << usage_message;
    exit(EXIT_FAILURE);
  }
}

void Get(const std::string& address, const std::string& key) {
  crocks::Cluster* db = new crocks::Cluster(address);
  std::cout << "shard:\t" << db->ShardForKey(key) << std::endl;
  std::cout << "node:\t" << db->IndexForKey(key) << std::endl;
  std::string value;
  crocks::Status status = db->Get(key, &value);
  EnsureRpc(status);
  std::cout << "value:\t" << value << std::endl;
  std::cout << "status:\t" << status.rocksdb_code() << " ("
            << status.error_message() << ")" << std::endl;
  delete db;
}

void Put(const std::string& address, const std::string& key,
         const std::string& value) {
  crocks::Cluster* db = new crocks::Cluster(address);
  std::cout << "shard:\t" << db->ShardForKey(key) << std::endl;
  std::cout << "node:\t" << db->IndexForKey(key) << std::endl;
  crocks::Status status = db->Put(key, value);
  EnsureRpc(status);
  std::cout << "status:\t" << status.rocksdb_code() << " ("
            << status.error_message() << ")" << std::endl;
  delete db;
}

void Delete(const std::string& address, const std::string& key) {
  crocks::Cluster* db = new crocks::Cluster(address);
  std::cout << "shard:\t" << db->ShardForKey(key) << std::endl;
  std::cout << "node:\t" << db->IndexForKey(key) << std::endl;
  crocks::Status status = db->Delete(key);
  EnsureRpc(status);
  std::cout << "status:\t" << status.rocksdb_code() << " ("
            << status.error_message() << ")" << std::endl;
  delete db;
}

void Run(const std::string& address) {
  crocks::Info info(address);
  info.Run();
}

void Migrate(const std::string& address) {
  crocks::Info info(address);
  info.Migrate();
}

void Health(const std::string& address) {
  crocks::Info info(address);
  info.Get();
  for (int i = 0; i < info.num_nodes(); i++) {
    std::string address = info.Address(i);
    if (address.empty())
      continue;
    if (!info.IsAvailable(i)) {
      std::cout << address << ": DOWN" << std::endl;
      continue;
    }
    crocks::Node* node = new crocks::Node(address);
    std::string value;
    crocks::Status status = node->Get("test", &value);
    delete node;
    if (status.IsUnavailable()) {
      info.SetAvailable(i, false);
      std::cout << address << ": DOWN" << std::endl;
    } else {
      std::cout << address << ": OK" << std::endl;
    }
  }
}

void List(const std::string& address) {
  crocks::Cluster* db = new crocks::Cluster(address);
  crocks::Iterator* it = new crocks::Iterator(db);
  int i;
  for (it->SeekToFirst(), i = 0; it->Valid(); it->Next(), i++)
    std::cout << it->key() << std::endl;
  std::cout << "total " << i << std::endl;
  delete it;
  delete db;
}

void Dump(const std::string& address) {
  crocks::Cluster* db = new crocks::Cluster(address);
  crocks::Iterator* it = new crocks::Iterator(db);
  int i;
  for (it->SeekToFirst(), i = 0; it->Valid(); it->Next(), i++)
    std::cout << it->key() << ": " << it->value() << std::endl;
  std::cout << "total " << i << std::endl;
  delete it;
  delete db;
}

void Clear(const std::string& address) {
  crocks::Cluster* db = new crocks::Cluster(address);
  crocks::Iterator* it = new crocks::Iterator(db);
  crocks::WriteBatch batch(db);
  for (it->SeekToFirst(); it->Valid(); it->Next())
    batch.Delete(it->key());
  crocks::Status status = batch.Write();
  EnsureRpc(status);
  delete it;
  delete db;
}

void Remove(const std::string& etcd_address, int id) {
  crocks::Info info(etcd_address);
  info.Remove(id);
}

void Info(const std::string& address) {
  crocks::Info info(address);
  info.Get();
  info.Print();
}

int main(int argc, char** argv) {
  std::string etcd_address = "localhost:2379";

  const char* optstring = "e:h";
  static struct option longopts[] = {
      {"etcd", required_argument, 0, 'e'},
      {"help", no_argument, 0, 'h'},
      {0, 0, 0, 0},
  };
  int c, index = 0;

  // Command-line options
  while ((c = getopt_long(argc, argv, optstring, longopts, &index)) != -1) {
    switch (c) {
      case 'e':
        etcd_address = optarg;
        break;
      case 'h':
        std::cout << usage_message;
        exit(EXIT_SUCCESS);
      default:
        std::cout << usage_message;
        exit(EXIT_FAILURE);
    }
  }

  // After calling getopt_long, argv is rearranged with parsed options
  // having moved to the beginning, and trailing arguments starting from
  // index optind until argc - 1. If optind == argc, no command was given.
  EnsureArguments(argc != optind);
  std::string command = argv[optind++];
  if (command == "get") {
    EnsureArguments(argc - optind == 1);
    Get(etcd_address, argv[optind]);

  } else if (command == "put") {
    EnsureArguments(argc - optind == 2);
    Put(etcd_address, argv[optind], argv[optind + 1]);

  } else if (command == "del") {
    EnsureArguments(argc - optind == 1);
    Delete(etcd_address, argv[optind]);

  } else if (command == "run") {
    EnsureArguments(argc == optind);
    Run(etcd_address);

  } else if (command == "migrate") {
    EnsureArguments(argc == optind);
    Migrate(etcd_address);

  } else if (command == "health") {
    EnsureArguments(argc == optind);
    Health(etcd_address);

  } else if (command == "list") {
    EnsureArguments(argc == optind);
    List(etcd_address);

  } else if (command == "dump") {
    EnsureArguments(argc == optind);
    Dump(etcd_address);

  } else if (command == "clear") {
    EnsureArguments(argc == optind);
    Clear(etcd_address);

  } else if (command == "remove") {
    EnsureArguments(argc - optind == 1);
    Remove(etcd_address, std::stoi(argv[optind]));

  } else if (command == "info") {
    EnsureArguments(argc == optind);
    Info(etcd_address);

  } else {
    EnsureArguments(false);
  }

  return 0;
}
