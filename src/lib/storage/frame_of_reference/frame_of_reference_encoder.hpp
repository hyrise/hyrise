#pragma once

#include <memory>
#include <array>
#include <limits>

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

    static constexpr auto frame_size = FrameOfReferenceColumn<T>::frame_size;

    const auto size = value_column->size();
    const auto num_reference_frames = size / frame_size;

    auto reference_frames = pmr_vector<T>{alloc};
    reference_frames.reserve(num_reference_frames);

    auto offset_values = pmr_vector<uint32_t>{alloc};
    offset_values.reserve(size);

    auto null_values = pmr_vector<bool>{alloc};
    null_values.reserve(size);

    auto max_value = uint32_t{0u};

    auto iterable = ValueColumnIterable<T>{*value_column};
    iterable.with_iterators([&](auto column_it, auto column_end) {
      auto value_block = std::array<T, frame_size>{};

      while (column_it != column_end) {
        auto value_block_it = value_block.begin();
        for (; value_block_it != value_block.end() && column_it != column_end; ++value_block_it, ++column_it) {
          const auto column_value = *column_it;

          *value_block_it = column_value.value();
          null_values.push_back(column_value.is_null());
        }

        const auto [min_it, max_it] = std::minmax_element(value_block.begin(), value_block_it);
        // Assert((*max_it - *min_it) <= static_cast<T>(std::numeric_limits<uint32_t>::max()), "Ups:" + type_cast<std::string>(*min_it) + "-" + type_cast<std::string>(*max_it));

        const auto reference_frame = *min_it;
        reference_frames.push_back(reference_frame);

        for (auto value : value_block) {
          const auto offset = static_cast<uint32_t>(value - reference_frame);
          offset_values.push_back(offset);
          max_value |= offset;
        }
      }
    });

    auto encoded_offset_values = compress_vector(offset_values, vector_compression_type(), alloc, {max_value});

    auto reference_frames_ptr = std::allocate_shared<pmr_vector<T>>(alloc, std::move(reference_frames));
    auto encoded_offset_values_sptr = std::shared_ptr<const BaseCompressedVector>(std::move(encoded_offset_values));
    auto null_values_ptr = std::allocate_shared<pmr_vector<bool>>(alloc, std::move(null_values));
    return std::allocate_shared<FrameOfReferenceColumn<T>>(alloc, reference_frames_ptr, encoded_offset_values_sptr, null_values_ptr);
  }
};

}  // namespace opossum
