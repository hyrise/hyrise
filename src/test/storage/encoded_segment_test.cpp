#include <boost/hana/at_key.hpp>

#include <cctype>
#include <memory>
#include <random>
#include <sstream>

#include "base_test.hpp"
#include "gtest/gtest.h"

#include "constant_mappings.hpp"
#include "storage/chunk_encoder.hpp"
#include "storage/create_iterable_from_segment.hpp"
#include "storage/encoding_type.hpp"
#include "storage/resolve_encoded_segment_type.hpp"
#include "storage/segment_encoding_utils.hpp"
#include "storage/value_segment.hpp"

#include "types.hpp"
#include "utils/enum_constant.hpp"

namespace opossum {

class EncodedSegmentTest : public BaseTestWithParam<SegmentEncodingSpec> {
 protected:
  static constexpr auto max_value = 1'024;

 protected:
  size_t row_count() {
    static constexpr auto default_row_count = size_t{1u} << 14;

    const auto encoding_spec = GetParam();

    switch (encoding_spec.encoding_type) {
      case EncodingType::FrameOfReference:
        // fill three blocks and a bit more
        return FrameOfReferenceSegment<int32_t>::block_size * (3.3);
      default:
        return default_row_count;
    }
  }

  std::shared_ptr<ValueSegment<int32_t>> create_int_value_segment() {
    auto values = pmr_concurrent_vector<int32_t>(row_count());

    std::default_random_engine engine{};
    std::uniform_int_distribution<int32_t> dist{0u, max_value};

    for (auto& elem : values) {
      elem = dist(engine);
    }

    return std::make_shared<ValueSegment<int32_t>>(std::move(values));
  }

  std::shared_ptr<ValueSegment<int32_t>> create_int_w_null_value_segment() {
    auto values = pmr_concurrent_vector<int32_t>(row_count());
    auto null_values = pmr_concurrent_vector<bool>(row_count());

    std::default_random_engine engine{};
    std::uniform_int_distribution<int32_t> dist{0u, max_value};
    std::bernoulli_distribution bernoulli_dist{0.3};

    for (auto i = 0u; i < row_count(); ++i) {
      values[i] = dist(engine);
      null_values[i] = bernoulli_dist(engine);
    }

    return std::make_shared<ValueSegment<int32_t>>(std::move(values), std::move(null_values));
  }

  std::shared_ptr<PosList> create_sequential_position_filter() {
    auto list = std::make_shared<PosList>();
    list->guarantee_single_chunk();

    std::default_random_engine engine{};
    std::bernoulli_distribution bernoulli_dist{0.5};

    for (auto offset_in_referenced_chunk = 0u; offset_in_referenced_chunk < row_count(); ++offset_in_referenced_chunk) {
      if (bernoulli_dist(engine)) {
        list->push_back(RowID{ChunkID{0}, offset_in_referenced_chunk});
      }
    }

    return list;
  }

  std::shared_ptr<PosList> create_random_access_position_filter() {
    auto list = create_sequential_position_filter();

    auto random_device = std::random_device{};
    std::default_random_engine engine{random_device()};
    std::shuffle(list->begin(), list->end(), engine);

    return list;
  }

  template <typename T>
  std::shared_ptr<BaseEncodedSegment> encode_value_segment(DataType data_type,
                                                           const std::shared_ptr<ValueSegment<T>>& value_segment) {
    const auto segment_encoding_spec = GetParam();
    return encode_segment(segment_encoding_spec.encoding_type, data_type, value_segment,
                          segment_encoding_spec.vector_compression_type);
  }
};

auto formatter = [](const ::testing::TestParamInfo<SegmentEncodingSpec> info) {
  const auto spec = info.param;

  auto stream = std::stringstream{};
  stream << encoding_type_to_string.left.at(spec.encoding_type);
  if (spec.vector_compression_type) {
    stream << "-" << vector_compression_type_to_string.left.at(*spec.vector_compression_type);
  }

  auto string = stream.str();
  string.erase(std::remove_if(string.begin(), string.end(), [](char c) { return !std::isalnum(c); }), string.end());

  return string;
};

INSTANTIATE_TEST_CASE_P(
    SegmentEncodingSpecs, EncodedSegmentTest,
    ::testing::Values(SegmentEncodingSpec{EncodingType::Dictionary, VectorCompressionType::SimdBp128},
                      SegmentEncodingSpec{EncodingType::Dictionary, VectorCompressionType::FixedSizeByteAligned},
                      SegmentEncodingSpec{EncodingType::FrameOfReference, VectorCompressionType::SimdBp128},
                      SegmentEncodingSpec{EncodingType::FrameOfReference, VectorCompressionType::FixedSizeByteAligned},
                      SegmentEncodingSpec{EncodingType::RunLength}),
    formatter);

TEST_P(EncodedSegmentTest, SequentiallyReadNotNullableIntSegment) {
  auto value_segment = this->create_int_value_segment();
  auto base_encoded_segment = this->encode_value_segment(DataType::Int, value_segment);

  EXPECT_EQ(value_segment->size(), base_encoded_segment->size());

  resolve_encoded_segment_type<int32_t>(*base_encoded_segment, [&](const auto& encoded_segment) {
    auto value_segment_iterable = create_iterable_from_segment(*value_segment);
    auto encoded_segment_iterable = create_iterable_from_segment(encoded_segment);

    value_segment_iterable.with_iterators([&](auto value_segment_it, auto value_segment_end) {
      encoded_segment_iterable.with_iterators([&](auto encoded_segment_it, auto encoded_segment_end) {
        for (; encoded_segment_it != encoded_segment_end; ++encoded_segment_it, ++value_segment_it) {
          EXPECT_EQ(value_segment_it->value(), encoded_segment_it->value());
        }
      });
    });
  });
}

TEST_P(EncodedSegmentTest, SequentiallyReadNullableIntSegment) {
  auto value_segment = this->create_int_w_null_value_segment();
  auto base_encoded_segment = this->encode_value_segment(DataType::Int, value_segment);

  EXPECT_EQ(value_segment->size(), base_encoded_segment->size());

  resolve_encoded_segment_type<int32_t>(*base_encoded_segment, [&](const auto& encoded_segment) {
    auto value_segment_iterable = create_iterable_from_segment(*value_segment);
    auto encoded_segment_iterable = create_iterable_from_segment(encoded_segment);

    value_segment_iterable.with_iterators([&](auto value_segment_it, auto value_segment_end) {
      encoded_segment_iterable.with_iterators([&](auto encoded_segment_it, auto encoded_segment_end) {
        auto row_idx = 0;
        for (; encoded_segment_it != encoded_segment_end; ++encoded_segment_it, ++value_segment_it, ++row_idx) {
          // This covers `EncodedSegment::operator[]`
          if (variant_is_null((*value_segment)[row_idx])) {
            EXPECT_TRUE(variant_is_null(encoded_segment[row_idx]));
          } else {
            EXPECT_EQ((*value_segment)[row_idx], encoded_segment[row_idx]);
          }

          // This covers the point access iterator
          EXPECT_EQ(value_segment_it->is_null(), encoded_segment_it->is_null());

          if (!value_segment_it->is_null()) {
            EXPECT_EQ(value_segment_it->value(), encoded_segment_it->value());
          }
        }
      });
    });
  });
}

TEST_P(EncodedSegmentTest, SequentiallyReadNullableIntSegmentWithChunkOffsetsList) {
  auto value_segment = this->create_int_w_null_value_segment();
  auto base_encoded_segment = this->encode_value_segment(DataType::Int, value_segment);

  EXPECT_EQ(value_segment->size(), base_encoded_segment->size());

  auto position_filter = this->create_sequential_position_filter();

  resolve_encoded_segment_type<int32_t>(*base_encoded_segment, [&](const auto& encoded_segment) {
    auto value_segment_iterable = create_iterable_from_segment(*value_segment);
    auto encoded_segment_iterable = create_iterable_from_segment(encoded_segment);

    value_segment_iterable.with_iterators(position_filter, [&](auto value_segment_it, auto value_segment_end) {
      encoded_segment_iterable.with_iterators(position_filter, [&](auto encoded_segment_it, auto encoded_segment_end) {
        for (; encoded_segment_it != encoded_segment_end; ++encoded_segment_it, ++value_segment_it) {
          EXPECT_EQ(value_segment_it->is_null(), encoded_segment_it->is_null());

          if (!value_segment_it->is_null()) {
            EXPECT_EQ(value_segment_it->value(), encoded_segment_it->value());
          }
        }
      });
    });
  });
}

TEST_P(EncodedSegmentTest, SequentiallyReadNullableIntSegmentWithShuffledChunkOffsetsList) {
  auto value_segment = this->create_int_w_null_value_segment();
  auto base_encoded_segment = this->encode_value_segment(DataType::Int, value_segment);

  EXPECT_EQ(value_segment->size(), base_encoded_segment->size());

  auto position_filter = this->create_random_access_position_filter();

  resolve_encoded_segment_type<int32_t>(*base_encoded_segment, [&](const auto& encoded_segment) {
    auto value_segment_iterable = create_iterable_from_segment(*value_segment);
    auto encoded_segment_iterable = create_iterable_from_segment(encoded_segment);

    value_segment_iterable.with_iterators(position_filter, [&](auto value_segment_it, auto value_segment_end) {
      encoded_segment_iterable.with_iterators(position_filter, [&](auto encoded_segment_it, auto encoded_segment_end) {
        for (; encoded_segment_it != encoded_segment_end; ++encoded_segment_it, ++value_segment_it) {
          EXPECT_EQ(value_segment_it->is_null(), encoded_segment_it->is_null());

          if (!value_segment_it->is_null()) {
            EXPECT_EQ(value_segment_it->value(), encoded_segment_it->value());
          }
        }
      });
    });
  });
}

}  // namespace opossum
