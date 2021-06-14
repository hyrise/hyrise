#pragma once

#include <algorithm>
#include <array>
#include <limits>
#include <memory>
#include <string>

#include "storage/base_segment_encoder.hpp"
#include "storage/value_segment.hpp"
#include "storage/value_segment/value_segment_iterable.hpp"
#include "types.hpp"
#include "utils/assert.hpp"
#include "utils/enum_constant.hpp"

namespace opossum {

class FSSTEncoder : public SegmentEncoder<FSSTEncoder> {
 public:
  static constexpr auto _encoding_type = enum_c<EncodingType, EncodingType::FSST>;
  static constexpr auto _uses_vector_compression = true;

  // TODO (anyone): check if this method is needed for FSST
  template <typename T>
  std::shared_ptr<AbstractEncodedSegment> _on_encode(const AnySegmentIterable<T> segment_iterable,
                                                     const PolymorphicAllocator<T>& allocator) {
    auto values = pmr_vector<T>{allocator};
    auto null_values = pmr_vector<bool>{allocator};

    segment_iterable.with_iterators([&](auto it, auto end) {
      // Early out for empty segments, code below assumes it to be non-empty
      // is this needed?
      if (it == end) {  // TODO: remove
        return;
      }

      const auto segment_size = static_cast<size_t>(std::distance(it, end));
      values.reserve(segment_size);
      null_values.reserve(segment_size);

      // Init is_current_null such that it does not equal the first entry
      T current_value;       // = T{};
      bool is_current_null;  //= !it->is_null();
                             //      SegmentPosition<T> segment_value;
      //      auto current_index = 0u;

      for (; it != end; ++it) {
        auto segment_value = *it;

        current_value = segment_value.value();
        is_current_null = segment_value.is_null();

        values.emplace_back(current_value);  // TODO (anyone): check if we can save null values
        null_values.emplace_back(is_current_null);
      }
    });

    // The resize method of the vector might have overallocated memory - hand that memory back to the system
    values.shrink_to_fit();
    null_values.shrink_to_fit();

    return std::make_shared<FSSTSegment<T>>(std::move(values), std::move(null_values));
    //TODO (anyone): add parameters to create empty Segment
    //    return nullptr;
  }

  //  std::shared_ptr<AbstractEncodedSegment> _on_encode(const AnySegmentIterable<pmr_string> segment_iterable,
  //                                                     const PolymorphicAllocator<pmr_string>& allocator) {
  //
  //    //TODO (anyone): add parameters to create empty Segment
  //    return nullptr;
  //  }
};

}  // namespace opossum