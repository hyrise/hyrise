#pragma once

#include <memory>

#include "storage/base_segment_encoder.hpp"

#include "storage/run_length_segment.hpp"
#include "storage/value_segment.hpp"
#include "storage/value_segment/value_segment_iterable.hpp"
#include "types.hpp"
#include "utils/enum_constant.hpp"

namespace opossum {

class RunLengthEncoder : public SegmentEncoder<RunLengthEncoder> {
 public:
  static constexpr auto _encoding_type = enum_c<EncodingType, EncodingType::RunLength>;
  static constexpr auto _uses_vector_compression = false;

  template <typename T>
  std::shared_ptr<BaseEncodedSegment> _on_encode(const AnySegmentIterable<T> segment_iterable,
                                                 const PolymorphicAllocator<T>& allocator) {
    auto values = pmr_vector<T>{allocator};
    auto null_values = pmr_vector<bool>{allocator};
    auto end_positions = pmr_vector<ChunkOffset>{allocator};

    segment_iterable.with_iterators([&](auto it, auto end) {
      // Early out for empty segments, code below assumes it to be non-empty
      if (it == end) {
        return;
      }

      // Init is_current_null such that it does not equal the first entry
      auto current_value = T{};
      auto is_current_null = !it->is_null();
      auto current_index = 0u;

      for (; it != end; ++it) {
        auto segment_value = *it;

        const auto previous_value = current_value;
        const auto is_previous_null = is_current_null;

        current_value = segment_value.value();
        is_current_null = segment_value.is_null();

        if ((is_previous_null == is_current_null) && (is_previous_null || (previous_value == current_value))) {
          end_positions.back() = current_index;
        } else {
          values.push_back(current_value);
          null_values.push_back(is_current_null);
          end_positions.push_back(current_index);
        }

        ++current_index;
      }
    });

    values.shrink_to_fit();
    null_values.shrink_to_fit();
    end_positions.shrink_to_fit();

    auto values_ptr = std::allocate_shared<pmr_vector<T>>(allocator, std::move(values));
    auto null_values_ptr = std::allocate_shared<pmr_vector<bool>>(allocator, std::move(null_values));
    auto end_positions_ptr = std::allocate_shared<pmr_vector<ChunkOffset>>(allocator, std::move(end_positions));
    return std::allocate_shared<RunLengthSegment<T>>(allocator, values_ptr, null_values_ptr, end_positions_ptr);
  }
};

}  // namespace opossum
