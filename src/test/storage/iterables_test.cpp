#include <limits>
#include <string>
#include <cstdint>
#include <vector>

#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "../lib/storage/dictionary_column.hpp"
#include "../lib/storage/iterables/dictionary_column_iterable.hpp"
#include "../lib/storage/dictionary_compression.hpp"
#include "../lib/storage/value_column.hpp"
#include "../lib/storage/iterables/value_column_iterable.hpp"
#include "../lib/storage/table.hpp"

namespace opossum {

struct SumUp
{
  template <typename Iterator>
  uint32_t operator()(Iterator begin, Iterator end) const {
    auto sum = uint32_t{0u};

    for (; begin != end; ++begin) {
      if ((*begin).is_null()) continue;

      sum += (*begin).value();
    }

    return sum;
  }
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

TEST_F(IterablesTest, IteratorExecuteForAll) {
  auto & chunk = table->get_chunk(ChunkID{0u});

  auto column = chunk.get_column(ColumnID{0u});
  auto int_column = std::dynamic_pointer_cast<ValueColumn<int>>(column);

  auto iterable = ValueColumnIterable<int>{*int_column};

  EXPECT_EQ(iterable.type(), ValueColumnIterableType::Simple);

  const auto sum = iterable.execute_for_all(SumUp{});

  EXPECT_EQ(sum, 24'825);
}

TEST_F(IterablesTest, ReferencedIteratorExecuteForAll) {
  auto & chunk = table->get_chunk(ChunkID{0u});

  auto column = chunk.get_column(ColumnID{0u});
  auto int_column = std::dynamic_pointer_cast<ValueColumn<int>>(column);

  auto chunk_offsets = std::vector<ChunkOffset>{0u, 2u, 3u};
  auto chunk_offsets_ptr = std::make_shared<std::vector<ChunkOffset>>(std::move(chunk_offsets));

  auto iterable = ValueColumnIterable<int>{*int_column, chunk_offsets_ptr};

  EXPECT_EQ(iterable.type(), ValueColumnIterableType::Referenced);

  const auto sum = iterable.execute_for_all(SumUp{});

  EXPECT_EQ(sum, 12'480);
}

TEST_F(IterablesTest, NullableIteratorExecuteForAll) {
  auto & chunk = table_with_null->get_chunk(ChunkID{0u});

  auto column = chunk.get_column(ColumnID{0u});
  auto int_column = std::dynamic_pointer_cast<ValueColumn<int>>(column);

  auto iterable = ValueColumnIterable<int>{*int_column};

  EXPECT_EQ(iterable.type(), ValueColumnIterableType::Nullable);

  const auto sum = iterable.execute_for_all(SumUp{});

  EXPECT_EQ(sum, 13'702);
}

TEST_F(IterablesTest, NullableReferencedIteratorExecuteForAll) {
  auto & chunk = table_with_null->get_chunk(ChunkID{0u});

  auto column = chunk.get_column(ColumnID{0u});
  auto int_column = std::dynamic_pointer_cast<ValueColumn<int>>(column);

  auto chunk_offsets = std::vector<ChunkOffset>{0u, 2u, 3u};
  auto chunk_offsets_ptr = std::make_shared<std::vector<ChunkOffset>>(std::move(chunk_offsets));

  auto iterable = ValueColumnIterable<int>{*int_column, chunk_offsets_ptr};

  EXPECT_EQ(iterable.type(), ValueColumnIterableType::NullableReferenced);

  const auto sum = iterable.execute_for_all(SumUp{});

  EXPECT_EQ(sum, 13'579);
}

TEST_F(IterablesTest, DictIteratorExecuteForAll) {
  DictionaryCompression::compress_table(*table);

  auto & chunk = table->get_chunk(ChunkID{0u});

  auto column = chunk.get_column(ColumnID{0u});
  auto int_column = std::dynamic_pointer_cast<DictionaryColumn<int>>(column);

  auto iterable = DictionaryColumnIterable<int>{*int_column};

  EXPECT_EQ(iterable.type(), DictionaryColumnIterableType::Simple);

  const auto sum = iterable.execute_for_all(SumUp{});

  EXPECT_EQ(sum, 24'825);
}

TEST_F(IterablesTest, DictReferencedIteratorExecuteForAll) {
  DictionaryCompression::compress_table(*table);

  auto & chunk = table->get_chunk(ChunkID{0u});

  auto column = chunk.get_column(ColumnID{0u});
  auto int_column = std::dynamic_pointer_cast<DictionaryColumn<int>>(column);

  auto chunk_offsets = std::vector<ChunkOffset>{0u, 2u, 3u};
  auto chunk_offsets_ptr = std::make_shared<std::vector<ChunkOffset>>(std::move(chunk_offsets));

  auto iterable = DictionaryColumnIterable<int>{*int_column, chunk_offsets_ptr};

  EXPECT_EQ(iterable.type(), DictionaryColumnIterableType::Referenced);

  const auto sum = iterable.execute_for_all(SumUp{});

  EXPECT_EQ(sum, 12'480);
}

}  // namespace opossum
