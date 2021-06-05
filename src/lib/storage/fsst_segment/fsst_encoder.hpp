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

  // TODO (anyone): check if this method is needed for FSST
  template <typename T>
  std::shared_ptr<AbstractEncodedSegment> _on_encode(const AnySegmentIterable<T> segment_iterable,
                                                     const PolymorphicAllocator<T>& allocator) {
//    const auto alloc = value_segment->values().get_allocator();
//    const auto num_elements = value_segment->size();

    //TODO (anyone): add parameters to create empty Segment
    return std::make_shared<FSSTSegment<T>>(...);
  }

  std::shared_ptr<AbstractEncodedSegment> _on_encode(const AnySegmentIterable<pmr_string> segment_iterable,
                                                     const PolymorphicAllocator<pmr_string>& allocator) {

    //TODO (anyone): add parameters to create empty Segment
    return std::make_shared<FSSTSegment<T>>();
  }
};

}  // namespace opossum