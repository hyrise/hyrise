#pragma once

#include <memory>
#include <string>
#include <type_traits>

#include "base_column_encoder.hpp"

#include "storage/encoded_columns/run_length_column.hpp"
#include "storage/iterables/value_column_iterable.hpp"
#include "storage/value_column.hpp"
#include "types.hpp"
#include "utils/enum_constant.hpp"

namespace opossum {

class RunLengthEncoder : public ColumnEncoder<RunLengthEncoder> {
 public:
  static constexpr auto _encoding_type = enum_c<EncodingType, EncodingType::RunLength>;

  template <typename T>
  std::shared_ptr<BaseColumn> _on_encode(const std::shared_ptr<ValueColumn<T>>& value_column) {
    auto null_value = get_null_value<T>();

    const auto alloc = value_column->values().get_allocator();

    auto values = pmr_vector<T>{alloc};
    auto end_positions = pmr_vector<ChunkOffset>{alloc};

    auto iterable = ValueColumnIterable<T>{*value_column};

    iterable.with_iterators([&](auto it, auto end) {
      // Init current_value such that it does not equal the first entry
      auto current_value = it->is_null() ? T{} : null_value;
      auto current_index = 0u;

      for (; it != end; ++it) {
        auto column_value = *it;

        const auto prev_value = current_value;
        current_value = column_value.is_null() ? null_value : column_value.value();

        if (prev_value == current_value) {
          end_positions.back() = current_index;
        } else {
          values.push_back(current_value);
          end_positions.push_back(current_index);
        }

        ++current_index;
      }
    });

    auto values_ptr = std::allocate_shared<pmr_vector<T>>(alloc, std::move(values));
    auto end_positions_ptr = std::allocate_shared<pmr_vector<ChunkOffset>>(alloc, std::move(end_positions));
    return std::allocate_shared<RunLengthColumn<T>>(alloc, values_ptr, end_positions_ptr, null_value);
  }

 private:
  // Use quiet NaN as null value for floating point types
  template <typename T>
  std::enable_if_t<std::numeric_limits<T>::has_quiet_NaN, T> get_null_value() {
    return std::numeric_limits<T>::quiet_NaN();
  }

  // Use bell signal as null value for strings
  template <typename T>
  std::enable_if_t<std::is_same_v<std::string, T>, T> get_null_value() {
    return std::string{'\7'};
  }

  // Use maximum value as null value for integral types
  template <typename T>
  std::enable_if_t<std::is_integral_v<T>, T> get_null_value() {
    return std::numeric_limits<T>::max();
  }
};

}  // namespace opossum
