#pragma once

// #include <array>
// #include <cstdint>

namespace hyrise {

class MinMaxFilter {
 public:
  MinMaxFilter() : _min_value(INT32_MAX), _max_value(INT32_MIN) {}

  void insert(const int32_t value) {
    _min_value = std::min(_min_value, value);
    _max_value = std::max(_max_value, value);
  }

  bool probe(const int32_t value) const {
    return value >= _min_value && value <= _max_value;
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

}  // namespace hyrise