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
};

template <typename DataType>
class MinMaxPredicate : public BaseMinMaxPredicate {
 public:
  MinMaxPredicate() : _min_value(INT32_MAX), _max_value(INT32_MIN) {
    Assert((std::is_same<DataType, int32_t>::value), "MinMaxPredicate can only be instantiated with int32_t.");
  }

  void merge_from(const DataType& minimum, const DataType& maximum) {
    if constexpr (std::is_same_v<DataType, int32_t>) {
      set_atomic_min(_min_value, minimum);
      set_atomic_max(_max_value, maximum);
    } else {
      Fail("MinMaxPredicate only supports int32_t values.");
    }
  }

  DataType min_value() const {
    if constexpr (std::is_same_v<DataType, int32_t>) {
      return _min_value;
    }
    Fail("MinMaxPredicate only supports int32_t values.");
  }

  DataType max_value() const {
    if constexpr (std::is_same_v<DataType, int32_t>) {
      return _max_value;
    }
    Fail("MinMaxPredicate only supports int32_t values.");
  }

 private:
  std::atomic<int32_t> _min_value;
  std::atomic<int32_t> _max_value;
};

// template class MinMaxPredicate<int32_t>;

}  // namespace hyrise