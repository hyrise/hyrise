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
  MinMaxPredicate();

  void merge_from(const DataType& minimum, const DataType& maximum);

  DataType min_value() const;

  DataType max_value() const;

 private:
  std::atomic<int32_t> _min_value;
  std::atomic<int32_t> _max_value;
};

}  // namespace hyrise