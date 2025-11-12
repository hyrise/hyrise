#include "min_max_predicate.hpp"

#include "types.hpp"
#include "utils/assert.hpp"
#include "utils/atomic_max.hpp"

namespace hyrise {

template <typename DataType>
MinMaxPredicate<DataType>::MinMaxPredicate() : _min_value(INT32_MAX), _max_value(INT32_MIN) {
  Assert((std::is_same<DataType, int32_t>::value), "MinMaxPredicate can only be instantiated with int32_t.");
}

template <typename DataType>
void MinMaxPredicate<DataType>::merge_from(const DataType& minimum, const DataType& maximum) {
  if constexpr (std::is_same_v<DataType, int32_t>) {
    set_atomic_min(_min_value, minimum);
    set_atomic_max(_max_value, maximum);
  } else {
    Fail("MinMaxPredicate only supports int32_t values.");
  }
}

template <typename DataType>
DataType MinMaxPredicate<DataType>::min_value() const {
  if constexpr (std::is_same_v<DataType, int32_t>) {
    return _min_value;
  }
  Fail("MinMaxPredicate only supports int32_t values.");
}

template <typename DataType>
DataType MinMaxPredicate<DataType>::max_value() const {
  if constexpr (std::is_same_v<DataType, int32_t>) {
    return _max_value;
  }
  Fail("MinMaxPredicate only supports int32_t values.");
}

EXPLICITLY_INSTANTIATE_DATA_TYPES(MinMaxPredicate);
// template class MinMaxPredicate<int32_t>;

}  // namespace hyrise