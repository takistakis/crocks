// Merging iterator that allows iterating in every column family

#ifndef CROCKS_SERVER_ITERATOR_H
#define CROCKS_SERVER_ITERATOR_H

#include <assert.h>

#include <unordered_map>
#include <utility>
#include <vector>

#include <rocksdb/db.h>
#include <rocksdb/iterator.h>
#include <rocksdb/options.h>
#include <rocksdb/slice.h>
#include <rocksdb/status.h>

#include <crocks/status.h>
#include <rocksdb/comparator.h>
#include "src/common/heap.h"

namespace crocks {

// Used for the min heap (forward iteration)
class RocksdbIteratorGreater {
 public:
  RocksdbIteratorGreater() : comparator_(rocksdb::BytewiseComparator()) {}

  bool operator()(rocksdb::Iterator* a, rocksdb::Iterator* b) const {
    return comparator_->Compare(a->key(), b->key()) > 0;
  }

 private:
  const rocksdb::Comparator* comparator_;
};

// Used for the min heap (forward iteration)
class RocksdbIteratorLess {
 public:
  RocksdbIteratorLess() : comparator_(rocksdb::BytewiseComparator()) {}

  bool operator()(rocksdb::Iterator* a, rocksdb::Iterator* b) const {
    return comparator_->Compare(a->key(), b->key()) < 0;
  }

 private:
  const rocksdb::Comparator* comparator_;
};

typedef Heap<rocksdb::Iterator, RocksdbIteratorGreater> MinHeap;
typedef Heap<rocksdb::Iterator, RocksdbIteratorLess> MaxHeap;

class MultiIterator {
 public:
  MultiIterator(
      rocksdb::DB* db,
      const std::unordered_map<int, rocksdb::ColumnFamilyHandle*>& cfs) {
    std::vector<rocksdb::ColumnFamilyHandle*> column_families;
    for (const auto& pair : cfs)
      column_families.push_back(pair.second);
    db->NewIterators(rocksdb::ReadOptions(), column_families, &iters_);
  }

  ~MultiIterator() {
    for (const auto& iter : iters_)
      delete iter;
  }

  bool Valid() const {
    return current_ != nullptr;
  }

  void SeekToFirst() {
    ClearHeaps();
    forward_ = true;
    for (auto iter : iters_) {
      iter->SeekToFirst();
      if (iter->Valid())
        min_heap_.push_back(iter);
    }
    if (!min_heap_.empty()) {
      min_heap_.make_heap();
      current_ = min_heap_.top();
    }
  }

  void SeekToLast() {
    ClearHeaps();
    forward_ = false;
    for (auto iter : iters_) {
      iter->SeekToLast();
      if (iter->Valid())
        max_heap_.push_back(iter);
    }
    if (!max_heap_.empty()) {
      max_heap_.make_heap();
      current_ = max_heap_.top();
    }
  }

  void Seek(const rocksdb::Slice& target) {
    ClearHeaps();
    forward_ = true;
    for (auto iter : iters_) {
      iter->Seek(target);
      if (iter->Valid())
        min_heap_.push_back(iter);
    }
    if (!min_heap_.empty()) {
      min_heap_.make_heap();
      current_ = min_heap_.top();
    }
  }

  void SeekForPrev(const rocksdb::Slice& target) {
    ClearHeaps();
    forward_ = false;
    for (auto iter : iters_) {
      iter->SeekForPrev(target);
      if (iter->Valid())
        max_heap_.push_back(iter);
    }
    if (!max_heap_.empty()) {
      max_heap_.make_heap();
      current_ = max_heap_.top();
    }
  }

  void Next() {
    assert(Valid());
    if (!forward_)
      Seek(key());
    assert(current_ == min_heap_.top());
    min_heap_.pop();
    current_->Next();
    if (current_->Valid())
      min_heap_.push(current_);
    current_ = min_heap_.top();
  }

  void Prev() {
    assert(Valid());
    if (forward_)
      SeekForPrev(key());
    assert(current_ == max_heap_.top());
    max_heap_.pop();
    current_->Prev();
    if (current_->Valid())
      max_heap_.push(current_);
    current_ = max_heap_.top();
  }

  rocksdb::Slice key() const {
    assert(Valid());
    return current_->key();
  }

  rocksdb::Slice value() const {
    assert(Valid());
    return current_->value();
  }

  Status status() const {
    rocksdb::Status status;
    for (auto& iter : iters_) {
      status = iter->status();
      if (!status.ok())
        break;
    }
    return Status(status.code());
  }

 private:
  void ClearHeaps() {
    min_heap_.clear();
    max_heap_.clear();
    current_ = nullptr;
  }

  // iters_ keeps at all times a rocksdb::Iterator* for each node in the cluster
  std::vector<rocksdb::Iterator*> iters_;
  // Iterators in iters_ that are Valid(), are also in the active heap
  MinHeap min_heap_;
  MaxHeap max_heap_;
  rocksdb::Iterator* current_ = nullptr;
  bool forward_;
};

}  // namespace crocks

#endif  // CROCKS_SERVER_ITERATOR_H
