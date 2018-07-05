#include <cstdint>
#include <limits>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "base_test.hpp"
#include "gtest/gtest.h"

#include "resolve_type.hpp"
#include "storage/chunk_encoder.hpp"
#include "storage/column_iterables/constant_value_iterable.hpp"
#include "storage/dictionary_column.hpp"
#include "storage/dictionary_column/dictionary_column_iterable.hpp"
#include "storage/fixed_string_dictionary_column.hpp"
#include "storage/reference_column/reference_column_iterable.hpp"
#include "storage/table.hpp"
#include "storage/value_column.hpp"
#include "storage/value_column/value_column_iterable.hpp"

namespace opossum {

struct SumUpWithIterator {
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

struct AppendWithIterator {
  template <typename Iterator>
  void operator()(Iterator begin, Iterator end) const {
    _concatenate = "";

    for (; begin != end; ++begin) {
      if ((*begin).is_null()) continue;

      _concatenate += (*begin).value();
    }
  }

  std::string& _concatenate;
};

class IterablesTest : public BaseTest {
 protected:
  void SetUp() override {
    table = load_table("src/test/tables/int_float6.tbl", Chunk::MAX_SIZE);
    table_with_null = load_table("src/test/tables/int_float_with_null.tbl", Chunk::MAX_SIZE);
    table_strings = load_table("src/test/tables/string.tbl", Chunk::MAX_SIZE);
    table_two_chunks = load_table("src/test/tables/int_int3.tbl", 5);
    // encode one of the two chunks with dictionary compression
    ChunkEncoder::encode_chunk(table_two_chunks->get_chunk(ChunkID{0}), {DataType::Int, DataType::Int},
                               {{EncodingType::Dictionary}, {EncodingType::Dictionary}});
  }

  std::shared_ptr<Table> table;
  std::shared_ptr<Table> table_with_null;
  std::shared_ptr<Table> table_strings;
  std::shared_ptr<Table> table_two_chunks;
};

TEST_F(IterablesTest, ValueColumnIteratorWithIterators) {
  auto chunk = table->get_chunk(ChunkID{0u});

  auto column = chunk->get_column(ColumnID{0u});
  auto int_column = std::dynamic_pointer_cast<const ValueColumn<int>>(column);

  auto iterable = ValueColumnIterable<int>{*int_column};

  auto sum = uint32_t{0};
  iterable.with_iterators(SumUpWithIterator{sum});

  EXPECT_EQ(sum, 24'825u);
}

TEST_F(IterablesTest, ValueColumnReferencedIteratorWithIterators) {
  auto chunk = table->get_chunk(ChunkID{0u});

  auto column = chunk->get_column(ColumnID{0u});
  auto int_column = std::dynamic_pointer_cast<const ValueColumn<int>>(column);

  auto chunk_offsets = std::vector<ChunkOffsetMapping>{{0u, 0u}, {1u, 2u}, {2u, 3u}};

  auto iterable = ValueColumnIterable<int>{*int_column};

  auto sum = uint32_t{0};
  iterable.with_iterators(&chunk_offsets, SumUpWithIterator{sum});

  EXPECT_EQ(sum, 12'480u);
}

TEST_F(IterablesTest, ValueColumnNullableIteratorWithIterators) {
  auto chunk = table_with_null->get_chunk(ChunkID{0u});

  auto column = chunk->get_column(ColumnID{0u});
  auto int_column = std::dynamic_pointer_cast<const ValueColumn<int>>(column);

  auto iterable = ValueColumnIterable<int>{*int_column};

  auto sum = uint32_t{0};
  iterable.with_iterators(SumUpWithIterator{sum});

  EXPECT_EQ(sum, 13'702u);
}

TEST_F(IterablesTest, ValueColumnNullableReferencedIteratorWithIterators) {
  auto chunk = table_with_null->get_chunk(ChunkID{0u});

  auto column = chunk->get_column(ColumnID{0u});
  auto int_column = std::dynamic_pointer_cast<const ValueColumn<int>>(column);

  auto chunk_offsets = std::vector<ChunkOffsetMapping>{{0u, 0u}, {1u, 2u}, {2u, 3u}};

  auto iterable = ValueColumnIterable<int>{*int_column};

  auto sum = uint32_t{0};
  iterable.with_iterators(&chunk_offsets, SumUpWithIterator{sum});

  EXPECT_EQ(sum, 13'579u);
}

TEST_F(IterablesTest, DictionaryColumnIteratorWithIterators) {
  ChunkEncoder::encode_all_chunks(table, EncodingType::Dictionary);

  auto chunk = table->get_chunk(ChunkID{0u});

  auto column = chunk->get_column(ColumnID{0u});
  auto dict_column = std::dynamic_pointer_cast<const DictionaryColumn<int>>(column);

  auto iterable = DictionaryColumnIterable<int, pmr_vector<int>>{*dict_column};

  auto sum = uint32_t{0};
  iterable.with_iterators(SumUpWithIterator{sum});

  EXPECT_EQ(sum, 24'825u);
}

TEST_F(IterablesTest, DictionaryColumnReferencedIteratorWithIterators) {
  ChunkEncoder::encode_all_chunks(table, EncodingType::Dictionary);

  auto chunk = table->get_chunk(ChunkID{0u});

  auto column = chunk->get_column(ColumnID{0u});
  auto dict_column = std::dynamic_pointer_cast<const DictionaryColumn<int>>(column);

  auto chunk_offsets = std::vector<ChunkOffsetMapping>{{0u, 0u}, {1u, 2u}, {2u, 3u}};

  auto iterable = DictionaryColumnIterable<int, pmr_vector<int>>{*dict_column};

  auto sum = uint32_t{0};
  iterable.with_iterators(&chunk_offsets, SumUpWithIterator{sum});

  EXPECT_EQ(sum, 12'480u);
}

TEST_F(IterablesTest, FixedStringDictionaryColumnIteratorWithIterators) {
  ChunkEncoder::encode_all_chunks(table_strings, EncodingType::FixedStringDictionary);

  auto chunk = table_strings->get_chunk(ChunkID{0u});

  auto column = chunk->get_column(ColumnID{0u});
  auto dict_column = std::dynamic_pointer_cast<const FixedStringDictionaryColumn<std::string>>(column);

  auto iterable = DictionaryColumnIterable<std::string, FixedStringVector>{*dict_column};

  auto concatenate = std::string();
  iterable.with_iterators(AppendWithIterator{concatenate});

  EXPECT_EQ(concatenate, "xxxwwwyyyuuutttzzz");
}

TEST_F(IterablesTest, FixedStringDictionaryColumnReferencedIteratorWithIterators) {
  ChunkEncoder::encode_all_chunks(table_strings, EncodingType::FixedStringDictionary);

  auto chunk = table_strings->get_chunk(ChunkID{0u});

  auto column = chunk->get_column(ColumnID{0u});
  auto dict_column = std::dynamic_pointer_cast<const FixedStringDictionaryColumn<std::string>>(column);

  auto chunk_offsets = std::vector<ChunkOffsetMapping>{{0u, 0u}, {1u, 2u}, {2u, 3u}};

  auto iterable = DictionaryColumnIterable<std::string, FixedStringVector>{*dict_column};

  auto concatenate = std::string();
  iterable.with_iterators(&chunk_offsets, AppendWithIterator{concatenate});

  EXPECT_EQ(concatenate, "xxxyyyuuu");
}

TEST_F(IterablesTest, ReferenceColumnIteratorWithIterators) {
  auto pos_list =
      PosList{RowID{ChunkID{0u}, 0u}, RowID{ChunkID{0u}, 3u}, RowID{ChunkID{0u}, 1u}, RowID{ChunkID{0u}, 2u}};

  auto reference_column =
      std::make_unique<ReferenceColumn>(table, ColumnID{0u}, std::make_shared<PosList>(std::move(pos_list)));

  auto iterable = ReferenceColumnIterable<int>{*reference_column};

  auto sum = uint32_t{0};
  iterable.with_iterators(SumUpWithIterator{sum});

  EXPECT_EQ(sum, 24'825u);
}

TEST_F(IterablesTest, ReferenceColumnIteratorWithIterators2) {
  auto pos_list =
      PosList{RowID{ChunkID{0u}, 0u}, RowID{ChunkID{0u}, 3u}, RowID{ChunkID{0u}, 1u}, RowID{ChunkID{0u}, 2u}};

  auto reference_column =
      std::make_unique<ReferenceColumn>(table, ColumnID{0u}, std::make_shared<PosList>(std::move(pos_list)));

  auto vec = std::vector<const BaseColumnT<int>*>();

  auto pos_list2 = PosList{RowID{ChunkID{1u}, 2u}, RowID{ChunkID{0u}, 3u}, RowID{ChunkID{0u}, 2u},
                           RowID{ChunkID{0u}, 0u}, RowID{ChunkID{1u}, 0u}, RowID{ChunkID{1u}, 1u}};

  for (auto chunk_id = ChunkID{0}; chunk_id < table_two_chunks->chunk_count(); ++chunk_id) {
    auto chunk = table_two_chunks->get_chunk(chunk_id);
    auto base_column = chunk->get_column(ColumnID{0});

    resolve_column_type<int>(*base_column, [&](auto& typed_column) {
      using ColumnType = typename std::decay<decltype(typed_column)>::type;

      if constexpr (std::is_same<ColumnType, ValueColumn<int>>::value ||
                    std::is_same<ColumnType, DictionaryColumn<int>>::value) {
        vec.push_back(&typed_column);
      } else {
        std::cout << "not val col" << std::endl;
      }
    });
  }

  for (const auto& row_id : pos_list2) {
    std::cout << vec[row_id.chunk_id]->get_t(row_id.chunk_offset);
  }

  auto iterable = ReferenceColumnIterable<int>{*reference_column};

  auto sum = uint32_t{0};
  iterable.with_iterators(SumUpWithIterator{sum});

  EXPECT_EQ(sum, 24'825u);
}  // namespace opossum

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
