#include <cstdint>
#include <limits>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "../lib/storage/dictionary_column.hpp"
#include "../lib/storage/dictionary_compression.hpp"
#include "../lib/storage/iterables/constant_value_iterable.hpp"
#include "../lib/storage/iterables/dictionary_column_iterable.hpp"
#include "../lib/storage/iterables/reference_column_iterable.hpp"
#include "../lib/storage/iterables/value_column_iterable.hpp"
#include "../lib/storage/table.hpp"
#include "../lib/storage/value_column.hpp"

namespace opossum {

struct SumUp {
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

class IterablesTest : public BaseTest {
 protected:
  void SetUp() override {
    table = load_table("src/test/tables/int_float4.tbl", 0u);
    table_with_null = load_table("src/test/tables/int_float_with_null.tbl", 0u);
  }

  std::shared_ptr<Table> table;
  std::shared_ptr<Table> table_with_null;
};

TEST_F(IterablesTest, ValueColumnIteratorExecuteForAll) {
  auto& chunk = table->get_chunk(ChunkID{0u});

  auto column = chunk.get_column(ColumnID{0u});
  auto int_column = std::dynamic_pointer_cast<ValueColumn<int>>(column);

  auto iterable = ValueColumnIterable<int>{*int_column};

  auto sum = uint32_t{0};
  iterable.get_iterators(SumUp{sum});

  EXPECT_EQ(sum, 24'825u);
}

TEST_F(IterablesTest, ValueColumnReferencedIteratorExecuteForAll) {
  auto& chunk = table->get_chunk(ChunkID{0u});

  auto column = chunk.get_column(ColumnID{0u});
  auto int_column = std::dynamic_pointer_cast<ValueColumn<int>>(column);

  auto chunk_offsets = std::vector<ChunkOffsetMapping>{{0u, 0u}, {1u, 2u}, {2u, 3u}};

  auto iterable = ValueColumnIterable<int>{*int_column, &chunk_offsets};

  auto sum = uint32_t{0};
  iterable.get_iterators(SumUp{sum});

  EXPECT_EQ(sum, 12'480u);
}

TEST_F(IterablesTest, ValueColumnNullableIteratorExecuteForAll) {
  auto& chunk = table_with_null->get_chunk(ChunkID{0u});

  auto column = chunk.get_column(ColumnID{0u});
  auto int_column = std::dynamic_pointer_cast<ValueColumn<int>>(column);

  auto iterable = ValueColumnIterable<int>{*int_column};

  auto sum = uint32_t{0};
  iterable.get_iterators(SumUp{sum});

  EXPECT_EQ(sum, 13'702u);
}

TEST_F(IterablesTest, ValueColumnNullableReferencedIteratorExecuteForAll) {
  auto& chunk = table_with_null->get_chunk(ChunkID{0u});

  auto column = chunk.get_column(ColumnID{0u});
  auto int_column = std::dynamic_pointer_cast<ValueColumn<int>>(column);

  auto chunk_offsets = std::vector<ChunkOffsetMapping>{{0u, 0u}, {1u, 2u}, {2u, 3u}};

  auto iterable = ValueColumnIterable<int>{*int_column, &chunk_offsets};

  auto sum = uint32_t{0};
  iterable.get_iterators(SumUp{sum});

  EXPECT_EQ(sum, 13'579u);
}

TEST_F(IterablesTest, DictionaryColumnIteratorExecuteForAll) {
  DictionaryCompression::compress_table(*table);

  auto& chunk = table->get_chunk(ChunkID{0u});

  auto column = chunk.get_column(ColumnID{0u});
  auto dict_column = std::dynamic_pointer_cast<DictionaryColumn<int>>(column);

  auto iterable = DictionaryColumnIterable<int>{*dict_column};

  auto sum = uint32_t{0};
  iterable.get_iterators(SumUp{sum});

  EXPECT_EQ(sum, 24'825u);
}

TEST_F(IterablesTest, DictionaryColumnDictReferencedIteratorExecuteForAll) {
  DictionaryCompression::compress_table(*table);

  auto& chunk = table->get_chunk(ChunkID{0u});

  auto column = chunk.get_column(ColumnID{0u});
  auto dict_column = std::dynamic_pointer_cast<DictionaryColumn<int>>(column);

  auto chunk_offsets = std::vector<ChunkOffsetMapping>{{0u, 0u}, {1u, 2u}, {2u, 3u}};

  auto iterable = DictionaryColumnIterable<int>{*dict_column, &chunk_offsets};

  auto sum = uint32_t{0};
  iterable.get_iterators(SumUp{sum});

  EXPECT_EQ(sum, 12'480u);
}

TEST_F(IterablesTest, ReferenceColumnIteratorExecuteForAll) {
  auto pos_list =
      PosList{RowID{ChunkID{0u}, 0u}, RowID{ChunkID{0u}, 3u}, RowID{ChunkID{0u}, 1u}, RowID{ChunkID{0u}, 2u}};

  auto reference_column =
      std::make_unique<ReferenceColumn>(table, ColumnID{0u}, std::make_shared<PosList>(std::move(pos_list)));

  auto iterable = ReferenceColumnIterable<int>{*reference_column};

  auto sum = uint32_t{0};
  iterable.get_iterators(SumUp{sum});

  EXPECT_EQ(sum, 24'825u);
}

TEST_F(IterablesTest, ConstantValueIteratorExecuteForAll) {
  auto iterable = ConstantValueIterable<int>{2u};

  auto sum = 0u;
  iterable.get_iterators([&](auto it, auto end) {
    for (auto i = 0u; i < 10; ++i) sum += (*it).value();
  });

  EXPECT_EQ(sum, 20u);
}

}  // namespace opossum
