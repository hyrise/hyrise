#pragma once

#include <array>
#include <atomic>
#include <string>

namespace opossum {

enum SegmentAccessType {
  Other,
  IteratorCreate,
  IteratorAccess,
  AccessorCreate,
  AccessorAccess,
  // number of elements in enum
    Count
};

class AtomicAccessStrategy {
 public:
  uint64_t count(SegmentAccessType type) const;

  void reset_all();

  void increase(SegmentAccessType type, uint64_t count);

 private:
  std::array<std::atomic_uint64_t, SegmentAccessType::Count> _count;
};

class NonLockingStrategy {
 public:
  uint64_t count(SegmentAccessType type) const;

  void reset_all();

  void increase(SegmentAccessType type, uint64_t count);

 private:
  std::array<uint64_t, SegmentAccessType::Count> _count;
};

template<class DataAccessStrategy>
class BulkCountingStrategy {
 public:
  explicit BulkCountingStrategy(DataAccessStrategy& data_access_strategy) : _data_access_strategy {
    data_access_strategy} {}

  void on_iterator_create(uint64_t count) {
    _data_access_strategy.increase(IteratorCreate, 1);
    _data_access_strategy.increase(IteratorAccess, count);
  }

  void on_iterator_create_with_pos_list(uint64_t count) {
    _data_access_strategy.increase(IteratorCreate, 1);
    _data_access_strategy.increase(IteratorAccess, count);
  }

  void on_iterator_dereference(uint64_t count) {}

  void on_iterator_dereference_using_pos_list(uint64_t count) {}

  void on_accessor_create(uint64_t count) {
    _data_access_strategy.increase(AccessorCreate, 1);
  }

  void on_accessor_access(uint64_t count) {
    _data_access_strategy.increase(AccessorAccess, count);
  }

  void on_other_access(uint64_t count) {
    _data_access_strategy.increase(Other, count);
  }

 private:
  DataAccessStrategy& _data_access_strategy;
};

template<class DataAccessStrategy>
class SingleAccessCountingStrategy {
 public:
  explicit SingleAccessCountingStrategy(DataAccessStrategy& data_access_strategy) : _data_access_strategy{
    data_access_strategy} {}

  void on_iterator_create(uint64_t count) {
    _data_access_strategy.increase(IteratorCreate, 1);
  }

  void on_iterator_create_with_pos_list(uint64_t count) {
    _data_access_strategy.increase(IteratorCreate, 1);
  }

  void on_iterator_dereference(uint64_t count) {
    _data_access_strategy.increase(IteratorAccess, count);
  }

  void on_iterator_dereference_using_pos_list(uint64_t count) {
    _data_access_strategy.increase(IteratorAccess, count);
  }

  void on_accessor_create(uint64_t count) {
    _data_access_strategy.increase(AccessorCreate, 1);
  }

  void on_accessor_access(uint64_t count) {
    _data_access_strategy.increase(AccessorAccess, count);
  }

  void on_other_access(uint64_t count) {
    _data_access_strategy.increase(Other, count);
  }

 private:
  DataAccessStrategy& _data_access_strategy;
};

template<class AccessStrategyType, class CountingStrategyType>
class SegmentAccessStatistics {
 public:
  explicit SegmentAccessStatistics() : _data_access_strategy{}, _counting_strategy{_data_access_strategy} {};

  void on_iterator_create(uint64_t count) {
    _counting_strategy.on_iterator_create(count);
  }

  void on_iterator_create_with_pos_list(uint64_t count) {
    _counting_strategy.on_iterator_create_with_pos_list(count);
  }

  void on_iterator_dereference(uint64_t count) {
    _counting_strategy.on_iterator_dereference(count);
  }

  void on_iterator_dereference_using_pos_list(uint64_t count) {
    _counting_strategy.on_iterator_dereference_using_pos_list(count);
  }

  void on_accessor_create(uint64_t count) {
    _counting_strategy.on_accessor_create(count);
  }

  void on_accessor_access(uint64_t count) {
    _counting_strategy.on_accessor_access(count);
  }

  void on_other_access(uint64_t count) {
    _counting_strategy.on_other_access(count);
  }

  uint64_t count(SegmentAccessType type) const {
    return _data_access_strategy.count(type);
  }

  void reset_all() {
    _data_access_strategy.reset_all();
  }

  std::string to_string() const {
    std::string str;
    str.reserve(SegmentAccessType::Count * 4);
    str.append(std::to_string(count(static_cast<SegmentAccessType>(0))));

    for (uint8_t type = 1; type < Count; ++type) {
      str.append(",");
      str.append(std::to_string(count(static_cast<SegmentAccessType>(type))));
    }

    return str;
  }

 private:
  AccessStrategyType _data_access_strategy;
  CountingStrategyType _counting_strategy;
};

  using SegmentAccessStatistics_T = SegmentAccessStatistics<AtomicAccessStrategy, BulkCountingStrategy<AtomicAccessStrategy>>;
}  // namespace opossum
