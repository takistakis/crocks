# crocks

A fast, sharded key-value store, based on [RocksDB].

## Quickstart

Compile and install the server and shared library for the client:

```
$ make crocks shared
$ [sudo] make install
```

Start a server:

```
$ crocks --port 50051
```

Compile and run this program:

```cpp
// test_crocks.cc

#include <iostream>
#include <string>

#include <crocks/cluster.h>
#include <crocks/status.h>

using namespace crocks;

int main() {
  std::vector<std::string> addresses = {"localhost:50051"};
  Cluster* db = DBOpen(addresses);
  std::string value;

  EnsureRpc(db->Put("key", "value"));
  EnsureRpc(db->Get("key", &value));
  std::cout << value << std::endl;

  EnsureRpc(db->Delete("key"));
  if (db->Get("key", &value).IsNotFound())
    std::cout << "not found" << std::endl;

  delete db;

  return 0;
}
```

```
$ g++ test_crocks.cc -o test_crocks -lcrocks
$ ./test_crocks
value
not found
```

## License

Licensed under GPLv3 or any later version.

[RocksDB]: http://www.rocksdb.org
