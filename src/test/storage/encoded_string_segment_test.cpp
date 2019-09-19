#include <boost/hana/at_key.hpp>

#include <algorithm>
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

class EncodedStringSegmentTest : public BaseTestWithParam<SegmentEncodingSpec> {
 protected:
  static constexpr auto max_length = 32;
  static constexpr auto row_count = size_t{1u} << 10;

  std::shared_ptr<ValueSegment<pmr_string>> create_empty_string_value_segment() {
    auto values = pmr_concurrent_vector<pmr_string>(row_count);
    return std::make_shared<ValueSegment<pmr_string>>(std::move(values));
  }

  std::shared_ptr<ValueSegment<pmr_string>> create_empty_string_with_null_value_segment() {
    auto values = pmr_concurrent_vector<pmr_string>(row_count);
    auto null_values = pmr_concurrent_vector<bool>(row_count);

    for (auto index = size_t{0u}; index < row_count; ++index) {
      null_values[index] = index % 4 == 0;
    }

    return std::make_shared<ValueSegment<pmr_string>>(std::move(values), std::move(null_values));
  }

  std::shared_ptr<ValueSegment<pmr_string>> create_string_value_segment() {
    auto values = pmr_concurrent_vector<pmr_string>(row_count);

    for (auto index = size_t{0u}; index < row_count; ++index) {
      if (index % 3 == 0) {
        values[index] = "Hello world!!1!12345";
      } else if (index % 3 == 1) {
        values[index] = "This IS A ...";
      } else {
        values[index] = "0987654312poiuytrewq";
      }
    }

    return std::make_shared<ValueSegment<pmr_string>>(std::move(values));
  }

  std::shared_ptr<ValueSegment<pmr_string>> create_string_with_null_value_segment() {
    auto values = pmr_concurrent_vector<pmr_string>(row_count);
    auto null_values = pmr_concurrent_vector<bool>(row_count);

    for (auto index = 0u; index < row_count; ++index) {
      null_values[index] = index % 4 == 0;
      if (index % 3 == 0) {
        values[index] = "Hello world!!1!12345";
      } else if (index % 3 == 1) {
        values[index] = "This IS A ...";
      } else {
        values[index] = "0987654312poiuytrewq";
      }
    }

    return std::make_shared<ValueSegment<pmr_string>>(std::move(values), std::move(null_values));
  }

  std::shared_ptr<PosList> create_sequential_position_filter() {
    auto list = std::make_shared<PosList>();
    list->guarantee_single_chunk();

    std::default_random_engine engine{};
    std::bernoulli_distribution bernoulli_dist{0.5};

    for (auto offset_in_referenced_chunk = 0u; offset_in_referenced_chunk < row_count; ++offset_in_referenced_chunk) {
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

  std::shared_ptr<BaseEncodedSegment> encode_segment(const std::shared_ptr<BaseSegment>& base_segment,
                                                     const DataType data_type) {
    auto segment_encoding_spec = GetParam();
    return this->encode_segment(base_segment, data_type, segment_encoding_spec);
  }

  std::shared_ptr<BaseEncodedSegment> encode_segment(const std::shared_ptr<BaseSegment>& base_segment,
                                                     const DataType data_type,
                                                     const SegmentEncodingSpec& segment_encoding_spec) {
    return encode_and_compress_segment(base_segment, data_type, segment_encoding_spec);
  }
};

auto formatter = [](const ::testing::TestParamInfo<SegmentEncodingSpec> info) {
  const auto spec = info.param;

  auto stream = std::stringstream{};
  stream << spec.encoding_type;
  if (spec.vector_compression_type) {
    stream << "-" << *spec.vector_compression_type;
  }

  auto string = stream.str();
  string.erase(std::remove_if(string.begin(), string.end(), [](char c) { return !std::isalnum(c); }), string.end());

  return string;
};

INSTANTIATE_TEST_SUITE_P(SegmentEncodingSpecs, EncodedStringSegmentTest,
                         ::testing::ValuesIn(BaseTest::get_supporting_segment_encodings_specs(DataType::String, false)),
                         formatter);

TEST_P(EncodedStringSegmentTest, SequentiallyReadNotNullableEmptyStringSegment) {
  auto value_segment = create_empty_string_value_segment();
  auto base_encoded_segment = this->encode_segment(value_segment, DataType::String);

  EXPECT_EQ(value_segment->size(), base_encoded_segment->size());

  resolve_encoded_segment_type<pmr_string>(*base_encoded_segment, [&](const auto& encoded_segment) {
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

TEST_P(EncodedStringSegmentTest, SequentiallyReadNullableEmptyStringSegment) {
  auto value_segment = create_empty_string_with_null_value_segment();
  auto base_encoded_segment = this->encode_segment(value_segment, DataType::String);

  EXPECT_EQ(value_segment->size(), base_encoded_segment->size());

  resolve_encoded_segment_type<pmr_string>(*base_encoded_segment, [&](const auto& encoded_segment) {
    auto value_segment_iterable = create_iterable_from_segment(*value_segment);
    auto encoded_segment_iterable = create_iterable_from_segment(encoded_segment);

    value_segment_iterable.with_iterators([&](auto value_segment_it, auto value_segment_end) {
      encoded_segment_iterable.with_iterators([&](auto encoded_segment_it, auto encoded_segment_end) {
        auto row_idx = 0u;
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

TEST_P(EncodedStringSegmentTest, SequentiallyReadNotNullableStringSegment) {
  auto value_segment = create_string_value_segment();
  auto base_encoded_segment = this->encode_segment(value_segment, DataType::String);

  EXPECT_EQ(value_segment->size(), base_encoded_segment->size());

  resolve_encoded_segment_type<pmr_string>(*base_encoded_segment, [&](const auto& encoded_segment) {
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

TEST_P(EncodedStringSegmentTest, SequentiallyReadNullableStringSegment) {
  auto value_segment = create_string_with_null_value_segment();
  auto base_encoded_segment = this->encode_segment(value_segment, DataType::String);

  EXPECT_EQ(value_segment->size(), base_encoded_segment->size());

  resolve_encoded_segment_type<pmr_string>(*base_encoded_segment, [&](const auto& encoded_segment) {
    auto value_segment_iterable = create_iterable_from_segment(*value_segment);
    auto encoded_segment_iterable = create_iterable_from_segment(encoded_segment);

    value_segment_iterable.with_iterators([&](auto value_segment_it, auto value_segment_end) {
      encoded_segment_iterable.with_iterators([&](auto encoded_segment_it, auto encoded_segment_end) {
        auto row_idx = 0u;
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

TEST_P(EncodedStringSegmentTest, SequentiallyReadNullableStringSegmentWithChunkOffsetsList) {
  auto value_segment = create_string_with_null_value_segment();
  auto base_encoded_segment = this->encode_segment(value_segment, DataType::String);

  EXPECT_EQ(value_segment->size(), base_encoded_segment->size());

  auto position_filter = create_sequential_position_filter();

  resolve_encoded_segment_type<pmr_string>(*base_encoded_segment, [&](const auto& encoded_segment) {
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

TEST_P(EncodedStringSegmentTest, SequentiallyReadNullableStringSegmentWithShuffledChunkOffsetsList) {
  auto value_segment = create_string_with_null_value_segment();
  auto base_encoded_segment = this->encode_segment(value_segment, DataType::String);

  EXPECT_EQ(value_segment->size(), base_encoded_segment->size());

  auto position_filter = create_random_access_position_filter();

  resolve_encoded_segment_type<pmr_string>(*base_encoded_segment, [&](const auto& encoded_segment) {
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

TEST_F(EncodedStringSegmentTest, SegmentReencoding) {
  auto value_segment = create_string_with_null_value_segment();

  auto encoded_segment =
      this->encode_segment(value_segment, DataType::String,
                           SegmentEncodingSpec{EncodingType::Dictionary, VectorCompressionType::FixedSizeByteAligned});
  EXPECT_SEGMENT_EQ_ORDERED(value_segment, encoded_segment);

  encoded_segment = this->encode_segment(value_segment, DataType::String, SegmentEncodingSpec{EncodingType::RunLength});
  EXPECT_SEGMENT_EQ_ORDERED(value_segment, encoded_segment);

  encoded_segment =
      this->encode_segment(value_segment, DataType::String,
                           SegmentEncodingSpec{EncodingType::FixedStringDictionary, VectorCompressionType::SimdBp128});
  EXPECT_SEGMENT_EQ_ORDERED(value_segment, encoded_segment);

  encoded_segment =
      this->encode_segment(value_segment, DataType::String,
                           SegmentEncodingSpec{EncodingType::LZ4, VectorCompressionType::FixedSizeByteAligned});
  EXPECT_SEGMENT_EQ_ORDERED(value_segment, encoded_segment);

  encoded_segment = this->encode_segment(
      value_segment, DataType::String, SegmentEncodingSpec{EncodingType::Dictionary, VectorCompressionType::SimdBp128});
  EXPECT_SEGMENT_EQ_ORDERED(value_segment, encoded_segment);

  encoded_segment = this->encode_segment(
      value_segment, DataType::String,
      SegmentEncodingSpec{EncodingType::FixedStringDictionary, VectorCompressionType::FixedSizeByteAligned});
  EXPECT_SEGMENT_EQ_ORDERED(value_segment, encoded_segment);

  encoded_segment = this->encode_segment(value_segment, DataType::String,
                                         SegmentEncodingSpec{EncodingType::LZ4, VectorCompressionType::SimdBp128});
  EXPECT_SEGMENT_EQ_ORDERED(value_segment, encoded_segment);
}

}  // namespace opossum
