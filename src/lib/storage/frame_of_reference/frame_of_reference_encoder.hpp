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
  std::shared_ptr<BaseEncodedSegment> _on_encode(const std::shared_ptr<const ValueSegment<T>>& value_segment) {
    const auto alloc = value_segment->values().get_allocator();

    static constexpr auto block_size = FrameOfReferenceSegment<T>::block_size;

    const auto size = value_segment->size();

    // Ceiling of integer division
    const auto div_ceil = [](auto x, auto y) { return (x + y - 1u) / y; };

    const auto num_blocks = div_ceil(size, block_size);

    // holds the minimum of each block
    auto block_minima = pmr_vector<T>{alloc};
    block_minima.reserve(num_blocks);

    // holds the uncompressed offset values
    auto offset_values = pmr_vector<uint32_t>{alloc};
    offset_values.reserve(size);

    // holds whether a segment value is null
    auto null_values = pmr_vector<bool>{alloc};
    null_values.reserve(size);

    // used as optional input for the compression of the offset values
    auto max_offset = uint32_t{0u};

    auto iterable = ValueSegmentIterable<T>{*value_segment};
    iterable.with_iterators([&](auto segment_it, auto segment_end) {
      // a temporary storage to hold the values of one block
      auto current_value_block = std::array<T, block_size>{};

      while (segment_it != segment_end) {
        auto value_block_it = current_value_block.begin();
        for (; value_block_it != current_value_block.end() && segment_it != segment_end;
             ++value_block_it, ++segment_it) {
          const auto segment_value = *segment_it;

          *value_block_it = segment_value.is_null() ? T{0u} : segment_value.value();
          null_values.push_back(segment_value.is_null());
        }

        // The last value block might not be filled completely
        const auto this_value_block_end = value_block_it;

        const auto [min_it, max_it] = std::minmax_element(current_value_block.begin(), this_value_block_end);  // NOLINT

        // Make sure that the largest offset fits into uint32_t (required for vector compression.)
        Assert(static_cast<std::make_unsigned_t<T>>(*max_it - *min_it) <= std::numeric_limits<uint32_t>::max(),
               "Value range in block must fit into uint32_t.");

        const auto minimum = *min_it;
        block_minima.push_back(minimum);

        value_block_it = current_value_block.begin();
        for (; value_block_it != this_value_block_end; ++value_block_it) {
          const auto value = *value_block_it;
          const auto offset = static_cast<uint32_t>(value - minimum);
          offset_values.push_back(offset);
          max_offset = std::max(max_offset, offset);
        }
      }
    });

    auto compressed_offset_values = compress_vector(offset_values, vector_compression_type(), alloc, {max_offset});

    return std::allocate_shared<FrameOfReferenceSegment<T>>(alloc, std::move(block_minima), std::move(null_values),
                                                            std::move(compressed_offset_values));
  }
};

}  // namespace opossum
