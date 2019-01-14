#pragma once

#include <algorithm>
#include <array>
#include <limits>
#include <memory>

#include "storage/base_segment_encoder.hpp"

#include "storage/lz4_segment.hpp"
#include "storage/value_segment.hpp"
#include "storage/value_segment/value_segment_iterable.hpp"
#include "storage/vector_compression/vector_compression.hpp"
#include "types.hpp"
#include "utils/enum_constant.hpp"

#include "lib/lz4hc.h"

namespace opossum {

class LZ4Encoder : public SegmentEncoder<LZ4Encoder> {
 public:
  static constexpr auto _encoding_type = enum_c<EncodingType, EncodingType::LZ4>;
  static constexpr auto _uses_vector_compression = false;

  template <typename T>
  std::shared_ptr<BaseEncodedSegment> _on_encode(const std::shared_ptr<const ValueSegment<T>>& value_segment) {
    // TODO what happens here?
    const auto alloc = value_segment->values().get_allocator();

    const auto input_size = static_cast<int>(value_segment->size());

    // calculate output size
    auto output_size = LZ4_compressBound(input_size);

    // create output buffer
    std::vector<char> compressed_data(static_cast<size_t>(output_size));

    // use the HC (high compression) compress method
    const int compressed_result = LZ4_compress_HC(reinterpret_cast<char*>(value_segment->data()), compressed_data.data(),
                                                  input_size, output_size, LZ4HC_CLEVEL_MAX);

    if (compressed_result <= 0) {
      // something went wrong
      throw std::runtime_error("LZ4 compression failed");
    }

    // create lz4 segment
    return std::allocate_shared<LZ4Segment<T>>(alloc, input_size, output_size, std::move(compressed_data));
  }
};

}  // namespace opossum
