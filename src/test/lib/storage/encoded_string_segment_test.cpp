#include <algorithm>
#include <cctype>
#include <memory>
#include <sstream>

#include "encoding_test.hpp"

#include "constant_mappings.hpp"
#include "storage/create_iterable_from_segment.hpp"
#include "storage/encoding_type.hpp"
#include "storage/resolve_encoded_segment_type.hpp"
#include "storage/segment_encoding_utils.hpp"
#include "storage/value_segment.hpp"

#include "types.hpp"

namespace opossum {

class EncodedStringSegmentTest : public BaseTestWithParam<SegmentEncodingSpec> {
 protected:
  static constexpr auto _row_count = size_t{1u} << 10;

  std::shared_ptr<ValueSegment<pmr_string>> _create_empty_string_value_segment() {
    auto values = pmr_vector<pmr_string>(_row_count);
    return std::make_shared<ValueSegment<pmr_string>>(std::move(values));
  }

  std::shared_ptr<ValueSegment<pmr_string>> _create_empty_string_with_null_value_segment() {
    auto values = pmr_vector<pmr_string>(_row_count);
    auto null_values = pmr_vector<bool>(_row_count);

    for (auto index = size_t{0u}; index < _row_count; ++index) {
      null_values[index] = index % 4 == 0;
    }

    return std::make_shared<ValueSegment<pmr_string>>(std::move(values), std::move(null_values));
  }

  std::shared_ptr<ValueSegment<pmr_string>> _create_string_value_segment() {
    auto values = pmr_vector<pmr_string>(_row_count);

    for (auto index = size_t{0u}; index < _row_count; ++index) {
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

  std::shared_ptr<ValueSegment<pmr_string>> _create_string_with_null_value_segment() {
    auto values = pmr_vector<pmr_string>(_row_count);
    auto null_values = pmr_vector<bool>(_row_count);

    for (auto index = 0u; index < _row_count; ++index) {
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

  std::shared_ptr<RowIDPosList> _create_sequential_position_filter() {
    auto list = std::make_shared<RowIDPosList>();
    list->guarantee_single_chunk();

    for (auto offset_in_referenced_chunk = 0u; offset_in_referenced_chunk < _row_count; ++offset_in_referenced_chunk) {
      if (offset_in_referenced_chunk % 2) {
        list->push_back(RowID{ChunkID{0}, offset_in_referenced_chunk});
      }
    }

    return list;
  }

  std::shared_ptr<RowIDPosList> _create_random_access_position_filter() {
    auto list = _create_sequential_position_filter();

    auto skewed_list = std::make_shared<RowIDPosList>();
    skewed_list->guarantee_single_chunk();
    skewed_list->reserve(list->size());
    // Let one iterator run from the beginning and one from the end of the list in each other's direction. Add the
    // iterators' elements alternately to the skewed list.
    auto front_iter = list->cbegin();
    auto back_iter = list->cend() - 1;
    const auto half_list_size = list->size() / 2;
    for (auto counter = 0u; counter < half_list_size; ++counter) {
      skewed_list->emplace_back(std::move(*front_iter));
      skewed_list->emplace_back(std::move(*back_iter));
      ++front_iter;
      --back_iter;
    }
    if (front_iter == back_iter) {  // odd number of list elements
      skewed_list->emplace_back(std::move(*front_iter));
    }

    return skewed_list;
  }

  std::shared_ptr<AbstractEncodedSegment> _encode_segment(const std::shared_ptr<AbstractSegment>& abstract_segment,
                                                          const DataType data_type) {
    auto segment_encoding_spec = GetParam();
    return this->_encode_segment(abstract_segment, data_type, segment_encoding_spec);
  }

  std::shared_ptr<AbstractEncodedSegment> _encode_segment(const std::shared_ptr<AbstractSegment>& abstract_segment,
                                                          const DataType data_type,
                                                          const SegmentEncodingSpec& segment_encoding_spec) {
    return std::dynamic_pointer_cast<AbstractEncodedSegment>(
        ChunkEncoder::encode_segment(abstract_segment, data_type, segment_encoding_spec));
  }
};

auto encoded_string_segment_test_formatter = [](const ::testing::TestParamInfo<SegmentEncodingSpec> info) {
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
                         ::testing::ValuesIn(get_supporting_segment_encodings_specs(DataType::String, false)),
                         encoded_string_segment_test_formatter);

TEST_P(EncodedStringSegmentTest, SequentiallyReadNotNullableEmptyStringSegment) {
  auto value_segment = _create_empty_string_value_segment();
  auto abstract_encoded_segment = this->_encode_segment(value_segment, DataType::String);

  EXPECT_EQ(value_segment->size(), abstract_encoded_segment->size());

  resolve_encoded_segment_type<pmr_string>(*abstract_encoded_segment, [&](const auto& encoded_segment) {
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
  auto value_segment = _create_empty_string_with_null_value_segment();
  auto abstract_encoded_segment = this->_encode_segment(value_segment, DataType::String);

  EXPECT_EQ(value_segment->size(), abstract_encoded_segment->size());

  resolve_encoded_segment_type<pmr_string>(*abstract_encoded_segment, [&](const auto& encoded_segment) {
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
  auto value_segment = _create_string_value_segment();
  auto abstract_encoded_segment = this->_encode_segment(value_segment, DataType::String);

  EXPECT_EQ(value_segment->size(), abstract_encoded_segment->size());

  resolve_encoded_segment_type<pmr_string>(*abstract_encoded_segment, [&](const auto& encoded_segment) {
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
  auto value_segment = _create_string_with_null_value_segment();
  auto abstract_encoded_segment = this->_encode_segment(value_segment, DataType::String);

  EXPECT_EQ(value_segment->size(), abstract_encoded_segment->size());

  resolve_encoded_segment_type<pmr_string>(*abstract_encoded_segment, [&](const auto& encoded_segment) {
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
  auto value_segment = _create_string_with_null_value_segment();
  auto abstract_encoded_segment = this->_encode_segment(value_segment, DataType::String);

  EXPECT_EQ(value_segment->size(), abstract_encoded_segment->size());

  auto position_filter = _create_sequential_position_filter();

  resolve_encoded_segment_type<pmr_string>(*abstract_encoded_segment, [&](const auto& encoded_segment) {
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
  auto value_segment = _create_string_with_null_value_segment();
  auto abstract_encoded_segment = this->_encode_segment(value_segment, DataType::String);

  EXPECT_EQ(value_segment->size(), abstract_encoded_segment->size());

  auto position_filter = _create_random_access_position_filter();

  resolve_encoded_segment_type<pmr_string>(*abstract_encoded_segment, [&](const auto& encoded_segment) {
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
  auto value_segment = _create_string_with_null_value_segment();

  auto encoded_segment =
      this->_encode_segment(value_segment, DataType::String,
                            SegmentEncodingSpec{EncodingType::Dictionary, VectorCompressionType::FixedSizeByteAligned});
  EXPECT_SEGMENT_EQ_ORDERED(value_segment, encoded_segment);

  encoded_segment =
      this->_encode_segment(value_segment, DataType::String, SegmentEncodingSpec{EncodingType::RunLength});
  EXPECT_SEGMENT_EQ_ORDERED(value_segment, encoded_segment);

  encoded_segment =
      this->_encode_segment(value_segment, DataType::String,
                            SegmentEncodingSpec{EncodingType::FixedStringDictionary, VectorCompressionType::SimdBp128});
  EXPECT_SEGMENT_EQ_ORDERED(value_segment, encoded_segment);

  encoded_segment = this->_encode_segment(
      value_segment, DataType::String, SegmentEncodingSpec{EncodingType::Dictionary, VectorCompressionType::SimdBp128});
  EXPECT_SEGMENT_EQ_ORDERED(value_segment, encoded_segment);

  encoded_segment = this->_encode_segment(
      value_segment, DataType::String,
      SegmentEncodingSpec{EncodingType::FixedStringDictionary, VectorCompressionType::FixedSizeByteAligned});
  EXPECT_SEGMENT_EQ_ORDERED(value_segment, encoded_segment);

  encoded_segment = this->_encode_segment(value_segment, DataType::String,
                                          SegmentEncodingSpec{EncodingType::LZ4, VectorCompressionType::SimdBp128});
  EXPECT_SEGMENT_EQ_ORDERED(value_segment, encoded_segment);
}

}  // namespace opossum
