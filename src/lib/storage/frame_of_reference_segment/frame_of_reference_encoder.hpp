#pragma once

#include <algorithm>
#include <array>
#include <limits>
#include <memory>

#include "storage/base_segment_encoder.hpp"

#include "storage/frame_of_reference_segment.hpp"
#include "storage/value_segment.hpp"
#include "storage/value_segment/value_segment_iterable.hpp"
#include "storage/vector_compression/vector_compression.hpp"
#include "types.hpp"
#include "utils/enum_constant.hpp"

namespace opossum {

class FrameOfReferenceEncoder : public SegmentEncoder<FrameOfReferenceEncoder> {
 public:
  static constexpr auto _encoding_type = enum_c<EncodingType, EncodingType::FrameOfReference>;
  static constexpr auto _uses_vector_compression = true;  // see base_segment_encoder.hpp for details

  template <typename T>
  std::shared_ptr<BaseEncodedSegment> _on_encode(const AnySegmentIterable<T> segment_iterable,
                                                 const PolymorphicAllocator<T>& allocator) {
    static constexpr auto block_size = FrameOfReferenceSegment<T>::block_size;

    // Ceiling of integer division
    const auto div_ceil = [](auto x, auto y) { return (x + y - 1u) / y; };

    // holds the minimum of each block
    auto block_minima = pmr_vector<T>{allocator};

    // holds the uncompressed offset values
    auto offset_values = pmr_vector<uint32_t>{allocator};

    // holds whether a segment value is null
    auto null_values = pmr_vector<bool>{allocator};

    // used as optional input for the compression of the offset values
    auto max_offset = uint32_t{0u};

    segment_iterable.with_iterators([&](auto segment_it, auto segment_end) {
      const auto size = std::distance(segment_it, segment_end);
      const auto num_blocks = div_ceil(size, block_size);

      block_minima.reserve(num_blocks);
      offset_values.reserve(size);
      null_values.reserve(size);

      // a temporary storage to hold the values of one block
      auto current_value_block = std::array<T, block_size>{};

      // store iterator to the null values written within this block
      auto current_block_null_values_it = null_values.end();

      while (segment_it != segment_end) {
        auto min_value = std::numeric_limits<T>::max();
        auto max_value = std::numeric_limits<T>::lowest();
        auto block_contains_values = false;

        auto value_block_it = current_value_block.begin();
        for (; value_block_it != current_value_block.end() && segment_it != segment_end;
             ++value_block_it, ++segment_it) {
          const auto segment_value = *segment_it;

          const auto value = segment_value.value();
          const auto value_is_null = segment_value.is_null();
          *value_block_it = value_is_null ? T{0u} : value;
          null_values.push_back(value_is_null);

          if (!value_is_null) {
            min_value = std::min(min_value, value);
            max_value = std::max(max_value, value);
          }
          block_contains_values |= !value_is_null;
        }

        // The last value block might not be filled completely
        const auto this_value_block_end = value_block_it;

        if (block_contains_values) {
          // Make sure that the largest offset fits into uint32_t (required for vector compression).
          Assert(static_cast<std::make_unsigned_t<T>>(max_value - min_value) <= std::numeric_limits<uint32_t>::max(),
                 "Value range in block must fit into uint32_t.");
        }

        block_minima.push_back(min_value);

        value_block_it = current_value_block.begin();
        for (; value_block_it != this_value_block_end; ++value_block_it, ++current_block_null_values_it) {
          auto value = *value_block_it;
          if (*current_block_null_values_it) {
            // To ensure NULL values do not interfere with the min/max calculation (needed to calculate (i) the frame
            // offset and (ii) the required width of the compressed vector), we set them to the minimum value. As NULL
            // values are stored as zeros, we might run in an overflow of the uint32_t when minimum > 0.
            value = min_value;
          }
          const auto offset = static_cast<uint32_t>(value - min_value);
          offset_values.push_back(offset);
          max_offset = std::max(max_offset, offset);
        }
      }
    });

    auto compressed_offset_values = compress_vector(offset_values, vector_compression_type(), allocator, {max_offset});

    return std::allocate_shared<FrameOfReferenceSegment<T>>(allocator, std::move(block_minima), std::move(null_values),
                                                            std::move(compressed_offset_values));
  }
};

}  // namespace opossum
