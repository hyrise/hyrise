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
#include "utils/assert.hpp"

#include "lib/lz4hc.h"
#include "lib/dictBuilder/zdict.h"

namespace opossum {

class LZ4Encoder : public SegmentEncoder<LZ4Encoder> {
  public:
    static constexpr auto _encoding_type = enum_c<EncodingType, EncodingType::LZ4>;
    static constexpr auto _uses_vector_compression = false;

    template <typename T>
    std::shared_ptr<BaseEncodedSegment> _on_encode_with_point_access(const std::shared_ptr<const ValueSegment<T>>& value_segment) {
      const auto alloc = value_segment->values().get_allocator();
      const auto num_elements = value_segment->size();
      DebugAssert(num_elements <= std::numeric_limits<int>::max(), "Trying to compress a ValueSegment with more "
                                                                  "elements than fit into an int.");

      // TODO (anyone): when value segments switch to using pmr_vectors, the data can be copied directly instead of
      // copying it element by element
      auto values = pmr_vector<T>{alloc};
      values.reserve(num_elements);
      auto null_values = pmr_vector<bool>{alloc};
      null_values.reserve(num_elements);

      // copy values and null flags from value segment
      auto iterable = ValueSegmentIterable<T>{*value_segment};
      iterable.with_iterators([&](auto it, auto end) {
        for (; it != end; ++it) {
          auto segment_value = *it;
          values.emplace_back(segment_value.value());
          null_values.push_back(segment_value.is_null());
        }
      });

      // lz4 compression
      const auto input_size = static_cast<int>(values.size() * sizeof(T));
      auto output_size = LZ4_compressBound(input_size);
      auto compressed_data = pmr_vector<char>{alloc};
      compressed_data.reserve(static_cast<size_t>(output_size));
      const int compression_result = LZ4_compress_HC(reinterpret_cast<char*>(values.data()), compressed_data.data(),
                                                    input_size, output_size, LZ4HC_CLEVEL_MAX);
      if (compression_result <= 0) {
        Fail("LZ4 compression failed");
      }

      auto data_ptr = std::allocate_shared<pmr_vector<char>>(alloc, std::move(compressed_data));
      auto null_values_ptr = std::allocate_shared<pmr_vector<bool>>(alloc, std::move(null_values));

      return std::allocate_shared<LZ4Segment<T>>(alloc, data_ptr, null_values_ptr, nullptr, compression_result,
                                                input_size, num_elements, nullptr);
    }

  std::shared_ptr<BaseEncodedSegment> _on_encode(const std::shared_ptr<const ValueSegment<std::string>>& value_segment) {
    const auto alloc = value_segment->values().get_allocator();
    const auto num_elements = value_segment->size();

    // copy values and null flags from value segment
    auto values = pmr_vector<char>{alloc};
    auto null_values = pmr_vector<bool>{alloc};
    null_values.reserve(num_elements);
    // the offset is the beginning of the string in the decompressed data vector
    // to look up the end the offset of the next element has to be looked up (in case of the last element the end is
    // the end of the vector)
    // null values are not saved and the offset is the same as the next element (i.e. the previous offset + the length
    // of the previous value)
    auto offsets = pmr_vector<size_t>{alloc};
    offsets.reserve(num_elements);

    auto iterable = ValueSegmentIterable<std::string>{*value_segment};
    iterable.with_iterators([&](auto it, auto end) {
      size_t offset = 0u;
      bool is_null;
      for (; it != end; ++it) {
        auto segment_value = *it;
        is_null = segment_value.is_null();
        null_values.push_back(is_null);
        offsets.emplace_back(offset);
        if (!is_null) {
          auto c_string = segment_value.value().c_str();
          auto length = strlen(c_string);
          values.insert(values.cend(), c_string, c_string + length);
          offset += length;
        }
      }
    });

    // lz4 compression
    const auto input_size = static_cast<int>(values.size());
    auto output_size = LZ4_compressBound(input_size);
    auto compressed_data = pmr_vector<char>{alloc};
    compressed_data.reserve(static_cast<size_t>(output_size));
    const int compression_result = LZ4_compress_HC(values.data(), compressed_data.data(),
                                                    input_size, output_size, LZ4HC_CLEVEL_MAX);
    if (compression_result <= 0) {
      Fail("LZ4 compression failed");
    }

    auto data_ptr = std::allocate_shared<pmr_vector<char>>(alloc, std::move(compressed_data));
    auto null_values_ptr = std::allocate_shared<pmr_vector<bool>>(alloc, std::move(null_values));
    auto offset_ptr = std::allocate_shared<pmr_vector<size_t>>(alloc, std::move(offsets));

    return std::allocate_shared<LZ4Segment<std::string>>(alloc, data_ptr, null_values_ptr, offset_ptr,
                                                          compression_result, input_size, num_elements, nullptr);
  }

  template <typename T>
  std::shared_ptr<BaseEncodedSegment> _on_encode(const std::shared_ptr<const ValueSegment<T>>& value_segment) {
    const auto alloc = value_segment->values().get_allocator();
    const auto num_elements = value_segment->size();
    DebugAssert(num_elements <= std::numeric_limits<int>::max(), "Trying to compress a ValueSegment with more "
                                                                "elements than fit into an int.");

    // TODO (anyone): when value segments switch to using pmr_vectors, the data can be copied directly instead of
    // copying it element by element
    auto values = pmr_vector<T>{alloc};
    values.reserve(num_elements);
    auto null_values = pmr_vector<bool>{alloc};
    null_values.reserve(num_elements);

    // copy values and null flags from value segment
    auto iterable = ValueSegmentIterable<T>{*value_segment};
    iterable.with_iterators([&](auto it, auto end) {
      for (; it != end; ++it) {
        auto segment_value = *it;
        values.emplace_back(segment_value.value());
        null_values.push_back(segment_value.is_null());
      }
    });

    // lz4 stream compression in blocks
    const auto input_size = static_cast<int>(values.size() * sizeof(T));
    const int block_size = 4096;
    const int compression_level = LZ4HC_CLEVEL_MAX;

    std::shared_ptr<pmr_vector<char>> dictionary_ptr;
    if (input_size > block_size) {
        // train a dictionary in advance for better compression
        dictionary_ptr = _generate_dictionary(values);
    } else {
        dictionary_ptr = nullptr;
    }

    LZ4_streamHC_t* stream_ptr = LZ4_createStreamHC();
    LZ4_resetStreamHC(stream_ptr, compression_level);

    const auto decompressed_block_size = input_size < block_size ? input_size : block_size;
    auto compressed_data = pmr_vector<char>{alloc};
    auto compressed_block_bound = LZ4_compressBound(decompressed_block_size);
    auto num_blocks = input_size / block_size + 1;
    auto compressed_data_bound = compressed_block_bound * num_blocks;
    compressed_data.reserve(compressed_data_bound);

    size_t compressed_data_size = 0;
    auto offsets = pmr_vector<size_t>{alloc};
    offsets.resize(num_blocks);

    for (int block_count = 0; block_count < num_blocks; ++block_count) {

      auto compressed_block = pmr_vector<char>{alloc};
      compressed_block.resize(static_cast<size_t>(compressed_block_bound));

      int outlen;
      auto block_ptr = reinterpret_cast<char*>(values.data()) + (block_count * block_size);
      int block_length = block_count + 1 == num_blocks ? input_size - (block_count * (num_blocks - 1)) : block_size;

      if (dictionary_ptr != nullptr) {
          // Forget previously compressed data and load the dictionary, so blocks are compressed independently
          LZ4_loadDictHC(stream_ptr, dictionary_ptr->data(), static_cast<int>(dictionary_ptr->size()));
      }
      outlen = LZ4_compress_HC_continue(stream_ptr, block_ptr, compressed_block.data(),
                                  block_length, compressed_block_bound);
      if (outlen <= 0) {
        Fail("LZ4 stream compression failed");
      }
      compressed_data_size += outlen;
      offsets.at(block_count) = compressed_data_size;
      compressed_block.resize(outlen);
      compressed_data.insert(compressed_data.end(), compressed_block.begin(), compressed_block.end());
    }

    LZ4_freeStreamHC(stream_ptr);

    compressed_data.resize(compressed_data_size);

    auto data_ptr = std::allocate_shared<pmr_vector<char>>(alloc, std::move(compressed_data));
    auto null_values_ptr = std::allocate_shared<pmr_vector<bool>>(alloc, std::move(null_values));
    auto offset_ptr = std::allocate_shared<pmr_vector<size_t>>(alloc, std::move(offsets));

    return std::allocate_shared<LZ4Segment<T>>(alloc, data_ptr, null_values_ptr, offset_ptr, compressed_data_size,
                                                input_size, num_elements, dictionary_ptr);
  }

  private:
    template<typename T>
    std::shared_ptr<pmr_vector<char>> _generate_dictionary(const pmr_vector<T> &samples) {

      const size_t num_samples = samples.size();
      const size_t sample_size = sizeof(T);
      const std::vector<size_t> sample_lens(num_samples, sample_size); // all samples are of size of T
      size_t max_dict_bytes = num_samples * sample_size / 100; // recommended dict size is about 1/100th of size of all samples
      if (max_dict_bytes < 1000) {
          max_dict_bytes = 1000; // smaller dictionaries won't work
      }
      DebugAssert(samples.empty() == sample_lens.empty(), "Error in dictionary generation");
      if (samples.empty()) {
        return nullptr;
      }
      auto dict_data = pmr_vector<char>{samples.get_allocator()};
      dict_data.resize(max_dict_bytes);

      const size_t dict_len = ZDICT_trainFromBuffer(
          dict_data.data(),
          max_dict_bytes,
          samples.data(),
          sample_lens.data(),
          static_cast<unsigned>(sample_lens.size()));
      
      if (ZDICT_isError(dict_len)) {
        Fail("ZSTD dictionary generation failed");
      }
      DebugAssert(dict_len <= max_dict_bytes, "Dictionary is larger than the memory allocated for it.");
      dict_data.resize(dict_len);

      return std::make_shared<pmr_vector<char>>(dict_data);
    }
};

}  // namespace opossum
