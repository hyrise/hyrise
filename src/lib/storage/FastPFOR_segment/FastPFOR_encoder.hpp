#pragma once

#include <memory>

#include "storage/base_segment_encoder.hpp"
#include "headers/codecfactory.h"

#include "storage/fastPFOR_segment.hpp"

namespace opossum {

class FastPFOREncoder : public SegmentEncoder<FastPFOREncoder> {

 public:
  static constexpr auto _encoding_type = enum_c<EncodingType, EncodingType::FastPFOR>;
  static constexpr auto _uses_vector_compression = false;

  template <typename T>
  std::shared_ptr<AbstractEncodedSegment> _on_encode(const AnySegmentIterable<T> segment_iterable,
                                                     const PolymorphicAllocator<T>& allocator) {
    auto values = std::make_shared<pmr_vector<T>>(allocator);
    auto null_values = std::make_shared<pmr_vector<bool>>(allocator);

    // todo: can we get a pointer so we don't have to copy everything?
    segment_iterable.with_iterators([&](auto it, auto end) {
      for (; it != end; ++it) {
        values->push_back(it->is_null() ? 0 : it->value());
        null_values->push_back(it->is_null());
      }
    });

    // The resize method of the vector might have overallocated memory - hand that memory back to the system
    values->shrink_to_fit();
    null_values->shrink_to_fit();

    auto codec = *FastPForLib::CODECFactory::getFromName("simdbinarypacking");

    return std::make_shared<FastPFORSegment<T>>(values, null_values);
  }
};


}  // namespace opossum
