#include <boost/hana/at_key.hpp>

#include <cctype>
#include <memory>
#include <random>
#include <sstream>

#include "base_test.hpp"
#include "gtest/gtest.h"

#include "constant_mappings.hpp"
#include "storage/chunk_encoder.hpp"
#include "storage/column_encoding_utils.hpp"
#include "storage/create_iterable_from_column.hpp"
#include "storage/encoding_type.hpp"
#include "storage/resolve_encoded_column_type.hpp"
#include "storage/value_column.hpp"

#include "types.hpp"
#include "utils/enum_constant.hpp"

namespace opossum {

class EncodedColumnTest : public BaseTestWithParam<ColumnEncodingSpec> {
 protected:
  static constexpr auto max_value = 1'024;

 protected:
  size_t row_count() {
    static constexpr auto default_row_count = size_t{1u} << 14;

    const auto encoding_spec = GetParam();

    switch (encoding_spec.encoding_type) {
      case EncodingType::FrameOfReference:
        // fill three blocks and a bit more
        return FrameOfReferenceColumn<int32_t>::block_size * (3.3);
      default:
        return default_row_count;
    }
  }

  std::shared_ptr<ValueColumn<int32_t>> create_int_value_column() {
    auto values = pmr_concurrent_vector<int32_t>(row_count());

    std::default_random_engine engine{};
    std::uniform_int_distribution<int32_t> dist{0u, max_value};

    for (auto& elem : values) {
      elem = dist(engine);
    }

    return std::make_shared<ValueColumn<int32_t>>(std::move(values));
  }

  std::shared_ptr<ValueColumn<int32_t>> create_int_w_null_value_column() {
    auto values = pmr_concurrent_vector<int32_t>(row_count());
    auto null_values = pmr_concurrent_vector<bool>(row_count());

    std::default_random_engine engine{};
    std::uniform_int_distribution<int32_t> dist{0u, max_value};
    std::bernoulli_distribution bernoulli_dist{0.3};

    for (auto i = 0u; i < row_count(); ++i) {
      values[i] = dist(engine);
      null_values[i] = bernoulli_dist(engine);
    }

    return std::make_shared<ValueColumn<int32_t>>(std::move(values), std::move(null_values));
  }

  ChunkOffsetsList create_sequential_chunk_offsets_list() {
    auto list = ChunkOffsetsList{};

    std::default_random_engine engine{};
    std::bernoulli_distribution bernoulli_dist{0.5};

    for (auto into_referencing = 0u, into_referenced = 0u; into_referenced < row_count(); ++into_referenced) {
      if (bernoulli_dist(engine)) {
        list.push_back({into_referencing++, into_referenced});
      }
    }

    return list;
  }

  ChunkOffsetsList create_random_access_chunk_offsets_list() {
    auto list = create_sequential_chunk_offsets_list();

    auto random_device = std::random_device{};
    std::default_random_engine engine{random_device()};
    std::shuffle(list.begin(), list.end(), engine);

    return list;
  }

  template <typename T>
  std::shared_ptr<BaseEncodedColumn> encode_value_column(DataType data_type,
                                                         const std::shared_ptr<ValueColumn<T>>& value_column) {
    const auto column_encoding_spec = GetParam();
    return encode_column(column_encoding_spec.encoding_type, data_type, value_column,
                         column_encoding_spec.vector_compression_type);
  }
};

auto formatter = [](const ::testing::TestParamInfo<ColumnEncodingSpec> info) {
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
    ColumnEncodingSpecs, EncodedColumnTest,
    ::testing::Values(ColumnEncodingSpec{EncodingType::Dictionary, VectorCompressionType::SimdBp128},
                      ColumnEncodingSpec{EncodingType::Dictionary, VectorCompressionType::FixedSizeByteAligned},
                      ColumnEncodingSpec{EncodingType::FrameOfReference, VectorCompressionType::SimdBp128},
                      ColumnEncodingSpec{EncodingType::FrameOfReference, VectorCompressionType::FixedSizeByteAligned},
                      ColumnEncodingSpec{EncodingType::RunLength}),
    formatter);

TEST_P(EncodedColumnTest, SequentiallyReadNotNullableIntColumn) {
  auto value_column = this->create_int_value_column();
  auto base_encoded_column = this->encode_value_column(DataType::Int, value_column);

  EXPECT_EQ(value_column->size(), base_encoded_column->size());

  resolve_encoded_column_type<int32_t>(*base_encoded_column, [&](const auto& encoded_column) {
    auto value_column_iterable = create_iterable_from_column(*value_column);
    auto encoded_column_iterable = create_iterable_from_column(encoded_column);

    value_column_iterable.with_iterators([&](auto value_column_it, auto value_column_end) {
      encoded_column_iterable.with_iterators([&](auto encoded_column_it, auto encoded_column_end) {
        for (; encoded_column_it != encoded_column_end; ++encoded_column_it, ++value_column_it) {
          EXPECT_EQ(value_column_it->value(), encoded_column_it->value());
        }
      });
    });
  });
}

TEST_P(EncodedColumnTest, SequentiallyReadNullableIntColumn) {
  auto value_column = this->create_int_w_null_value_column();
  auto base_encoded_column = this->encode_value_column(DataType::Int, value_column);

  EXPECT_EQ(value_column->size(), base_encoded_column->size());

  resolve_encoded_column_type<int32_t>(*base_encoded_column, [&](const auto& encoded_column) {
    auto value_column_iterable = create_iterable_from_column(*value_column);
    auto encoded_column_iterable = create_iterable_from_column(encoded_column);

    value_column_iterable.with_iterators([&](auto value_column_it, auto value_column_end) {
      encoded_column_iterable.with_iterators([&](auto encoded_column_it, auto encoded_column_end) {
        auto row_idx = 0;
        for (; encoded_column_it != encoded_column_end; ++encoded_column_it, ++value_column_it, ++row_idx) {
          // This covers `EncodedColumn::operator[]`
          if (variant_is_null((*value_column)[row_idx])) {
            EXPECT_TRUE(variant_is_null(encoded_column[row_idx]));
          } else {
            EXPECT_EQ((*value_column)[row_idx], encoded_column[row_idx]);
          }

          // This covers the point access iterator
          EXPECT_EQ(value_column_it->is_null(), encoded_column_it->is_null());

          if (!value_column_it->is_null()) {
            EXPECT_EQ(value_column_it->value(), encoded_column_it->value());
          }
        }
      });
    });
  });
}

TEST_P(EncodedColumnTest, SequentiallyReadNullableIntColumnWithChunkOffsetsList) {
  auto value_column = this->create_int_w_null_value_column();
  auto base_encoded_column = this->encode_value_column(DataType::Int, value_column);

  EXPECT_EQ(value_column->size(), base_encoded_column->size());

  auto chunk_offsets_list = this->create_sequential_chunk_offsets_list();

  resolve_encoded_column_type<int32_t>(*base_encoded_column, [&](const auto& encoded_column) {
    auto value_column_iterable = create_iterable_from_column(*value_column);
    auto encoded_column_iterable = create_iterable_from_column(encoded_column);

    value_column_iterable.with_iterators(&chunk_offsets_list, [&](auto value_column_it, auto value_column_end) {
      encoded_column_iterable.with_iterators(&chunk_offsets_list, [&](auto encoded_column_it, auto encoded_column_end) {
        for (; encoded_column_it != encoded_column_end; ++encoded_column_it, ++value_column_it) {
          EXPECT_EQ(value_column_it->is_null(), encoded_column_it->is_null());

          if (!value_column_it->is_null()) {
            EXPECT_EQ(value_column_it->value(), encoded_column_it->value());
          }
        }
      });
    });
  });
}

TEST_P(EncodedColumnTest, SequentiallyReadNullableIntColumnWithShuffledChunkOffsetsList) {
  auto value_column = this->create_int_w_null_value_column();
  auto base_encoded_column = this->encode_value_column(DataType::Int, value_column);

  EXPECT_EQ(value_column->size(), base_encoded_column->size());

  auto chunk_offsets_list = this->create_random_access_chunk_offsets_list();

  resolve_encoded_column_type<int32_t>(*base_encoded_column, [&](const auto& encoded_column) {
    auto value_column_iterable = create_iterable_from_column(*value_column);
    auto encoded_column_iterable = create_iterable_from_column(encoded_column);

    value_column_iterable.with_iterators(&chunk_offsets_list, [&](auto value_column_it, auto value_column_end) {
      encoded_column_iterable.with_iterators(&chunk_offsets_list, [&](auto encoded_column_it, auto encoded_column_end) {
        for (; encoded_column_it != encoded_column_end; ++encoded_column_it, ++value_column_it) {
          EXPECT_EQ(value_column_it->is_null(), encoded_column_it->is_null());

          if (!value_column_it->is_null()) {
            EXPECT_EQ(value_column_it->value(), encoded_column_it->value());
          }
        }
      });
    });
  });
}

TEST_P(EncodedColumnTest, IsImmutable) {
  auto value_column = this->create_int_w_null_value_column();
  auto base_encoded_column = this->encode_value_column(DataType::Int, value_column);

  EXPECT_THROW(base_encoded_column->append(AllTypeVariant{}), std::logic_error);
}

}  // namespace opossum
