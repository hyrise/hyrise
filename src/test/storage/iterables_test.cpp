#include <cstdint>
#include <limits>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "base_test.hpp"
#include "gtest/gtest.h"

#include "storage/chunk_encoder.hpp"
#include "storage/column_iterables/constant_value_iterable.hpp"
#include "storage/dictionary_column.hpp"
#include "storage/dictionary_column/dictionary_column_iterable.hpp"
#include "storage/reference_column/reference_column_iterable.hpp"
#include "storage/table.hpp"
#include "storage/value_column.hpp"
#include "storage/value_column/value_column_iterable.hpp"

namespace opossum {

struct SumUpWithIt {
  template <typename Iterator>
  void operator()(Iterator begin, Iterator end) const {
    _sum = 0u;

    for (; begin != end; ++begin) {
      if ((*begin).is_null()) continue;

      _sum += (*begin).value();
    }
  }

  uint32_t& _sum;
};

struct SumUp {
  template <typename T>
  void operator()(const T& value) const {
    if (value.is_null()) return;

    _sum += value.value();
  }

  uint32_t& _sum;
};

class IterablesTest : public BaseTest {
 protected:
  void SetUp() override {
    table = load_table("src/test/tables/int_float6.tbl", Chunk::MAX_SIZE);
    table_with_null = load_table("src/test/tables/int_float_with_null.tbl", Chunk::MAX_SIZE);
  }

  std::shared_ptr<Table> table;
  std::shared_ptr<Table> table_with_null;
};

TEST_F(IterablesTest, ValueColumnIteratorWithIterators) {
  auto chunk = table->get_chunk(ChunkID{0u});

  auto column = chunk->get_column(ColumnID{0u});
  auto int_column = std::dynamic_pointer_cast<const ValueColumn<int>>(column);

  auto iterable = ValueColumnIterable<int>{*int_column};

  auto sum = uint32_t{0};
  iterable.with_iterators(SumUpWithIt{sum});

  EXPECT_EQ(sum, 24'825u);
}

TEST_F(IterablesTest, ValueColumnReferencedIteratorWithIterators) {
  auto chunk = table->get_chunk(ChunkID{0u});

  auto column = chunk->get_column(ColumnID{0u});
  auto int_column = std::dynamic_pointer_cast<const ValueColumn<int>>(column);

  auto chunk_offsets = std::vector<ChunkOffsetMapping>{{0u, 0u}, {1u, 2u}, {2u, 3u}};

  auto iterable = ValueColumnIterable<int>{*int_column};

  auto sum = uint32_t{0};
  iterable.with_iterators(&chunk_offsets, SumUpWithIt{sum});

  EXPECT_EQ(sum, 12'480u);
}

TEST_F(IterablesTest, ValueColumnNullableIteratorWithIterators) {
  auto chunk = table_with_null->get_chunk(ChunkID{0u});

  auto column = chunk->get_column(ColumnID{0u});
  auto int_column = std::dynamic_pointer_cast<const ValueColumn<int>>(column);

  auto iterable = ValueColumnIterable<int>{*int_column};

  auto sum = uint32_t{0};
  iterable.with_iterators(SumUpWithIt{sum});

  EXPECT_EQ(sum, 13'702u);
}

TEST_F(IterablesTest, ValueColumnNullableReferencedIteratorWithIterators) {
  auto chunk = table_with_null->get_chunk(ChunkID{0u});

  auto column = chunk->get_column(ColumnID{0u});
  auto int_column = std::dynamic_pointer_cast<const ValueColumn<int>>(column);

  auto chunk_offsets = std::vector<ChunkOffsetMapping>{{0u, 0u}, {1u, 2u}, {2u, 3u}};

  auto iterable = ValueColumnIterable<int>{*int_column};

  auto sum = uint32_t{0};
  iterable.with_iterators(&chunk_offsets, SumUpWithIt{sum});

  EXPECT_EQ(sum, 13'579u);
}

TEST_F(IterablesTest, DictionaryColumnIteratorWithIterators) {
  ChunkEncoder::encode_all_chunks(table, EncodingType::Dictionary);

  auto chunk = table->get_chunk(ChunkID{0u});

  auto column = chunk->get_column(ColumnID{0u});
  auto dict_column = std::dynamic_pointer_cast<const DictionaryColumn<int>>(column);

  auto iterable = DictionaryColumnIterable<int>{*dict_column};

  auto sum = uint32_t{0};
  iterable.with_iterators(SumUpWithIt{sum});

  EXPECT_EQ(sum, 24'825u);
}

TEST_F(IterablesTest, DictionaryColumnReferencedIteratorWithIterators) {
  ChunkEncoder::encode_all_chunks(table, EncodingType::Dictionary);

  auto chunk = table->get_chunk(ChunkID{0u});

  auto column = chunk->get_column(ColumnID{0u});
  auto dict_column = std::dynamic_pointer_cast<const DictionaryColumn<int>>(column);

  auto chunk_offsets = std::vector<ChunkOffsetMapping>{{0u, 0u}, {1u, 2u}, {2u, 3u}};

  auto iterable = DictionaryColumnIterable<int>{*dict_column};

  auto sum = uint32_t{0};
  iterable.with_iterators(&chunk_offsets, SumUpWithIt{sum});

  EXPECT_EQ(sum, 12'480u);
}

TEST_F(IterablesTest, ReferenceColumnIteratorWithIterators) {
  auto pos_list =
      PosList{RowID{ChunkID{0u}, 0u}, RowID{ChunkID{0u}, 3u}, RowID{ChunkID{0u}, 1u}, RowID{ChunkID{0u}, 2u}};

  auto reference_column =
      std::make_unique<ReferenceColumn>(table, ColumnID{0u}, std::make_shared<PosList>(std::move(pos_list)));

  auto iterable = ReferenceColumnIterable<int>{*reference_column};

  auto sum = uint32_t{0};
  iterable.with_iterators(SumUpWithIt{sum});

  EXPECT_EQ(sum, 24'825u);
}

TEST_F(IterablesTest, ConstantValueIteratorWithIterators) {
  auto iterable = ConstantValueIterable<int>{2u};

  auto sum = 0u;
  iterable.with_iterators([&](auto it, auto end) {
    for (auto i = 0u; i < 10; ++i) sum += (*it).value();
  });

  EXPECT_EQ(sum, 20u);
}

TEST_F(IterablesTest, ValueColumnIteratorForEach) {
  auto chunk = table->get_chunk(ChunkID{0u});

  auto column = chunk->get_column(ColumnID{0u});
  auto int_column = std::dynamic_pointer_cast<const ValueColumn<int>>(column);

  auto iterable = ValueColumnIterable<int>{*int_column};

  auto sum = uint32_t{0};
  iterable.for_each(SumUp{sum});

  EXPECT_EQ(sum, 24'825u);
}

TEST_F(IterablesTest, ValueColumnNullableIteratorForEach) {
  auto chunk = table_with_null->get_chunk(ChunkID{0u});

  auto column = chunk->get_column(ColumnID{0u});
  auto int_column = std::dynamic_pointer_cast<const ValueColumn<int>>(column);

  auto iterable = ValueColumnIterable<int>{*int_column};

  auto sum = uint32_t{0};
  iterable.for_each(SumUp{sum});

  EXPECT_EQ(sum, 13'702u);
}

}  // namespace opossum
