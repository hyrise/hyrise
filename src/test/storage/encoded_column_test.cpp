#include <boost/hana/at_key.hpp>

#include <memory>
#include <random>

#include "base_test.hpp"
#include "gtest/gtest.h"

#include "storage/column_encoding_utils.hpp"
#include "storage/create_iterable_from_column.hpp"
#include "storage/encoding_type.hpp"
#include "storage/resolve_encoded_column_type.hpp"
#include "storage/value_column.hpp"

#include "types.hpp"
#include "utils/enum_constant.hpp"

namespace opossum {

namespace hana = boost::hana;

using EncodingTypes = ::testing::Types<enum_constant<EncodingType, EncodingType::Dictionary>,
                                       enum_constant<EncodingType, EncodingType::DeprecatedDictionary>,
                                       enum_constant<EncodingType, EncodingType::RunLength>>;

template <typename EncodingTypeT>
class EncodedColumnTest : public BaseTest {
 protected:
  static constexpr auto encoding_type_t = EncodingTypeT{};
  static constexpr auto column_template_t = hana::at_key(encoded_column_for_type, encoding_type_t);

  using ColumnTemplateType = typename decltype(column_template_t)::type;

  template <typename T>
  using EncodedColumnType = typename ColumnTemplateType::template _template<T>;

 protected:
  static constexpr auto row_count = 200u;

 protected:
  std::shared_ptr<ValueColumn<int32_t>> create_int_value_column() {
    auto values = pmr_concurrent_vector<int32_t>(row_count);

    std::default_random_engine engine{};
    std::uniform_int_distribution<int32_t> dist{0u, 10u};

    for (auto& elem : values) {
      elem = dist(engine);
    }

    return std::make_shared<ValueColumn<int32_t>>(std::move(values));
  }

  std::shared_ptr<ValueColumn<int32_t>> create_int_w_null_value_column() {
    auto values = pmr_concurrent_vector<int32_t>(row_count);
    auto null_values = pmr_concurrent_vector<bool>(row_count);

    std::default_random_engine engine{};
    std::uniform_int_distribution<int32_t> dist{0u, 10u};
    std::bernoulli_distribution bernoulli_dist{0.3};

    for (auto i = 0u; i < row_count; ++i) {
      values[i] = dist(engine);
      null_values[i] = bernoulli_dist(engine);
    }

    return std::make_shared<ValueColumn<int32_t>>(std::move(values), std::move(null_values));
  }

  ChunkOffsetsList create_sequential_chunk_offsets_list() {
    auto list = ChunkOffsetsList{};

    std::default_random_engine engine{};
    std::bernoulli_distribution bernoulli_dist{0.5};

    for (auto into_referencing = 0u, into_referenced = 0u; into_referenced < row_count; ++into_referenced) {
      if (bernoulli_dist(engine)) {
        list.push_back({into_referencing++, into_referenced});
      }
    }

    return list;
  }

  ChunkOffsetsList create_random_access_chunk_offsets_list() {
    auto list = create_sequential_chunk_offsets_list();

    std::default_random_engine engine{};
    std::shuffle(list.begin(), list.end(), engine);

    return list;
  }

  template <typename T>
  std::shared_ptr<EncodedColumnType<T>> encode_value_column(DataType data_type,
                                                            const std::shared_ptr<ValueColumn<T>>& value_column) {
    return std::dynamic_pointer_cast<EncodedColumnType<T>>(encode_column(encoding_type_t(), data_type, value_column));
  }
};

TYPED_TEST_CASE(EncodedColumnTest, EncodingTypes);

TYPED_TEST(EncodedColumnTest, SequenciallyReadNotNullableIntColumn) {
  auto value_column = this->create_int_value_column();
  auto encoded_column = this->encode_value_column(DataType::Int, value_column);

  EXPECT_EQ(value_column->size(), encoded_column->size());

  auto value_column_iterable = create_iterable_from_column(*value_column);
  auto encoded_column_iterable = create_iterable_from_column(*encoded_column);

  value_column_iterable.with_iterators([&](auto value_column_it, auto value_column_end) {
    encoded_column_iterable.with_iterators([&](auto encoded_column_it, auto encoded_column_end) {
      for (; encoded_column_it != encoded_column_end; ++encoded_column_it, ++value_column_it) {
        EXPECT_EQ(value_column_it->value(), encoded_column_it->value());
      }
    });
  });
}

TYPED_TEST(EncodedColumnTest, SequenciallyReadNullableIntColumn) {
  auto value_column = this->create_int_w_null_value_column();
  auto encoded_column = this->encode_value_column(DataType::Int, value_column);

  EXPECT_EQ(value_column->size(), encoded_column->size());

  auto value_column_iterable = create_iterable_from_column(*value_column);
  auto encoded_column_iterable = create_iterable_from_column(*encoded_column);

  value_column_iterable.with_iterators([&](auto value_column_it, auto value_column_end) {
    encoded_column_iterable.with_iterators([&](auto encoded_column_it, auto encoded_column_end) {
      for (; encoded_column_it != encoded_column_end; ++encoded_column_it, ++value_column_it) {
        EXPECT_EQ(value_column_it->is_null(), encoded_column_it->is_null());

        if (!value_column_it->is_null()) {
          EXPECT_EQ(value_column_it->value(), encoded_column_it->value());
        }
      }
    });
  });
}

TYPED_TEST(EncodedColumnTest, SequanciallyReadNullableIntColumnWithChunkOffsetsList) {
  auto value_column = this->create_int_w_null_value_column();
  auto encoded_column = this->encode_value_column(DataType::Int, value_column);

  EXPECT_EQ(value_column->size(), encoded_column->size());

  auto value_column_iterable = create_iterable_from_column(*value_column);
  auto encoded_column_iterable = create_iterable_from_column(*encoded_column);

  auto chunk_offsets_list = this->create_sequential_chunk_offsets_list();

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
}

TYPED_TEST(EncodedColumnTest, SequanciallyReadNullableIntColumnWithShuffledChunkOffsetsList) {
  auto value_column = this->create_int_w_null_value_column();
  auto encoded_column = this->encode_value_column(DataType::Int, value_column);

  EXPECT_EQ(value_column->size(), encoded_column->size());

  auto value_column_iterable = create_iterable_from_column(*value_column);
  auto encoded_column_iterable = create_iterable_from_column(*encoded_column);

  auto chunk_offsets_list = this->create_random_access_chunk_offsets_list();

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
}

}  // namespace opossum
