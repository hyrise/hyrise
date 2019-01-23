#pragma once

#include <algorithm>
#include <array>
#include <limits>
#include <memory>
#include <string>

#include "storage/base_segment_encoder.hpp"
#include "storage/lz4_segment.hpp"
#include "storage/value_segment.hpp"
#include "storage/value_segment/value_segment_iterable.hpp"
#include "storage/vector_compression/vector_compression.hpp"
#include "types.hpp"
#include "utils/enum_constant.hpp"

#include "lib/lz4hc.h"
#include "lib/dictBuilder/zdict.h"

namespace opossum {

class LZ4Encoder : public SegmentEncoder<LZ4Encoder> {
 public:
  static constexpr auto _encoding_type = enum_c<EncodingType, EncodingType::LZ4>;
  static constexpr auto _uses_vector_compression = false;

  template <typename T>
  std::shared_ptr<BaseEncodedSegment> _on_encode(const std::shared_ptr<const ValueSegment<T>>& value_segment) {
    const auto values = value_segment->values();
    const auto alloc = value_segment->values().get_allocator();

    // copy data from concurrent vector to stl vector
    auto input_data = std::vector<T>(values.size());
    for (size_t index = 0u; index < values.size(); index++) {
      input_data[index] = values[index];
    }

    // size in bytes
    const auto input_size = static_cast<int>(input_data.size()) * sizeof(T);

    // calculate output size
    auto output_size = LZ4_compressBound(static_cast<int>(input_size));

    // create output buffer
    auto compressed_data = std::make_shared<std::vector<char>>(static_cast<size_t>(output_size));

    // use the HC (high compression) compress method
    const int compressed_result = LZ4_compress_HC(reinterpret_cast<char*>(input_data.data()),
                                                  compressed_data->data(), static_cast<int>(input_size), output_size,
                                                  LZ4HC_CLEVEL_MAX);

    if (compressed_result <= 0) {
      // something went wrong
      throw std::runtime_error("LZ4 compression failed");
    }

    // create lz4 segment
    return std::allocate_shared<LZ4Segment<T>>(alloc, input_size, output_size, std::move(compressed_data));
  }

  std::shared_ptr<BaseEncodedSegment> _on_encode(const std::shared_ptr<const ValueSegment<std::string>>& value_segment) {
    const auto values = value_segment->values();
    const auto alloc = value_segment->values().get_allocator();

    // copy data from concurrent vector to stl vector
    auto input_data = std::vector<char>();

    for (const auto& element : values) {
      auto c_string = element.c_str();
      input_data.insert(input_data.cend(), c_string, c_string + strlen(c_string) + 1);
    }

    auto input_size = static_cast<int>(input_data.size());

//    for (size_t index = 0u; index < values.size(); index++) {
//      auto c_string = values[index].c_str();
//      auto fixed_string = FixedString(element.data(), element.size());
//      input_data[index] = fixed_string;
//      input_size += fixed_string.size();
//
//      const auto c_string = elem.c_str();
//      // append char pointer as well as null byte (as separator) to vector
//      converted.insert(converted.end(), c_string, c_string + strlen(c_string) + 1);
//    }

    // calculate output size
    auto output_size = LZ4_compressBound(static_cast<int>(input_size));

    // create output buffer
    auto compressed_data = std::make_shared<std::vector<char>>(static_cast<size_t>(output_size));

    // use the HC (high compression) compress method
    const int compressed_result = LZ4_compress_HC(input_data.data(),
                                                  compressed_data->data(), static_cast<int>(input_size), output_size,
                                                  LZ4HC_CLEVEL_MAX);

    if (compressed_result <= 0) {
      // something went wrong
      throw std::runtime_error("LZ4 compression failed");
    }

    // create lz4 segment
    return std::allocate_shared<LZ4Segment<std::string>>(alloc, input_size, output_size, std::move(compressed_data));
  }

// private:
//  std::shared_ptr<char> _generate_dictionary(std::data samples) {
//
//    std::shared_ptr<char> dictSize = ZDICT_trainFromBuffer()
//
//  }
};

}  // namespace opossum
