#pragma once

// #include <array>
// #include <cstdint>

#include "types.hpp"

namespace hyrise {

// class BaseMinMaxFilter {
//  public:
//   virtual ~BaseMinMaxFilter() = default;

// //   virtual void insert(const int32_t& value) = 0;
// //   virtual bool probe(const int32_t& value) const = 0;
// //   virtual int32_t min_value() const = 0;
// //   virtual int32_t max_value() const = 0;
// };

// template <typename DataType>
class MinMaxFilter {
 public:
  MinMaxFilter() : _min_value(INT32_MAX), _max_value(INT32_MIN) {}

  template <typename T>
  void insert(const T& value) {
    if constexpr (std::is_same_v<T, int32_t>) {
      _min_value = std::min(_min_value, value);
      _max_value = std::max(_max_value, value);
    }
  }

  template <typename T>
  bool probe(const T& value) const {
    if constexpr (std::is_same_v<T, int32_t>) {
      return value >= _min_value && value <= _max_value;
    } else {
      return true;  // Default behavior for unsupported types
    }
  }

  int32_t min_value() const {
    return _min_value;
  }

  int32_t max_value() const {
    return _max_value;
  }

 private:
  int32_t _min_value;
  int32_t _max_value;
};

// template class MinMaxFilter<int32_t>;

}  // namespace hyrise