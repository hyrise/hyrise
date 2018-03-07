#pragma once

#include <array>
#include <limits>
#include <memory>

#include "storage/base_column_encoder.hpp"

#include "storage/frame_of_reference_column.hpp"
#include "storage/value_column.hpp"
#include "storage/value_column/value_column_iterable.hpp"
#include "storage/vector_compression/vector_compression.hpp"
#include "types.hpp"
#include "utils/enum_constant.hpp"

namespace opossum {

class FrameOfReferenceEncoder : public ColumnEncoder<FrameOfReferenceEncoder> {
 public:
  static constexpr auto _encoding_type = enum_c<EncodingType, EncodingType::FrameOfReference>;
  static constexpr auto _uses_vector_compression = true;

  template <typename T>
  std::shared_ptr<BaseEncodedColumn> _on_encode(const std::shared_ptr<const ValueColumn<T>>& value_column) {
    const auto alloc = value_column->values().get_allocator();

    static constexpr auto block_size = FrameOfReferenceColumn<T>::block_size;

    const auto size = value_column->size();
    const auto num_blocks = size / block_size;

    auto block_minima = pmr_vector<T>{alloc};
    block_minima.reserve(num_blocks);

    auto offset_values = pmr_vector<uint32_t>{alloc};
    offset_values.reserve(size);

    auto null_values = pmr_vector<bool>{alloc};
    null_values.reserve(size);

    auto max_value = uint32_t{0u};

    auto iterable = ValueColumnIterable<T>{*value_column};
    iterable.with_iterators([&](auto column_it, auto column_end) {
      auto value_block = std::array<T, block_size>{};

      while (column_it != column_end) {
        auto value_block_it = value_block.begin();
        for (; value_block_it != value_block.end() && column_it != column_end; ++value_block_it, ++column_it) {
          const auto column_value = *column_it;

          *value_block_it = column_value.is_null() ? T{0u} : column_value.value();
          null_values.push_back(column_value.is_null());
        }

        const auto[min_it, max_it] = std::minmax_element(value_block.begin(), value_block_it);
        Assert(static_cast<std::make_unsigned_t<T>>(*max_it - *min_it) <= std::numeric_limits<uint32_t>::max(),
               "Value range in block must fit into uint32_t.");

        const auto minimum = *min_it;
        block_minima.push_back(minimum);

        for (auto value : value_block) {
          const auto offset = static_cast<uint32_t>(value - minimum);
          offset_values.push_back(offset);
          max_value |= offset;
        }
      }
    });

    auto encoded_offset_values = compress_vector(offset_values, vector_compression_type(), alloc, {max_value});

    auto block_minima_ptr = std::allocate_shared<pmr_vector<T>>(alloc, std::move(block_minima));
    auto encoded_offset_values_sptr = std::shared_ptr<const BaseCompressedVector>(std::move(encoded_offset_values));
    auto null_values_ptr = std::allocate_shared<pmr_vector<bool>>(alloc, std::move(null_values));
    return std::allocate_shared<FrameOfReferenceColumn<T>>(alloc, block_minima_ptr, encoded_offset_values_sptr,
                                                           null_values_ptr);
  }
};

}  // namespace opossum
