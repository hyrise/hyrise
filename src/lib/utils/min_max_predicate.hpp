#pragma once

#include <atomic>
// #include <cstdint>

#include "types.hpp"
#include "utils/assert.hpp"
#include "utils/atomic_max.hpp"

namespace hyrise {

class BaseMinMaxPredicate {
 public:
  virtual ~BaseMinMaxPredicate() = default;

  //   virtual void insert(const int32_t& value) = 0;
  //   virtual bool probe(const int32_t& value) const = 0;
  //   virtual int32_t min_value() const = 0;
  //   virtual int32_t max_value() const = 0;
};

template <typename DataType>
class MinMaxPredicate : public BaseMinMaxPredicate {
 public:
  MinMaxPredicate() : _min_value(INT32_MAX), _max_value(INT32_MIN) {
    Assert((std::is_same<DataType, int32_t>::value), "MinMaxPredicate can only be instantiated with int32_t.");
  }

  void insert(const DataType& value) {
    if constexpr (std::is_same_v<DataType, int32_t>) {
      if (_min_value > value) {
        _min_value = value;
      }
      if (_max_value < value) {
        _max_value = value;
      }
    }
  }

  bool probe(const DataType& value) const {
    // return false;
    if constexpr (std::is_same_v<DataType, int32_t>) {
      return value >= _min_value && value <= _max_value;
    } else {
      return true;  // Default behavior for unsupported types
    }
  }

  void merge_from(const MinMaxPredicate& other) {
    if constexpr (std::is_same_v<DataType, int32_t>) {
      set_atomic_min(_min_value, other._min_value);
      set_atomic_max(_max_value, other._max_value);
    }
  }

  int32_t min_value() const {
    return _min_value;
  }

  int32_t max_value() const {
    return _max_value;
  }

 private:
  std::atomic<int32_t> _min_value;
  std::atomic<int32_t> _max_value;
};

// template class MinMaxPredicate<int32_t>;

}  // namespace hyrise