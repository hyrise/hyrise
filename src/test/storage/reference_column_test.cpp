#include <iostream>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "gtest/gtest.h"

#include "../../lib/operators/abstract_operator.hpp"
#include "../../lib/operators/get_table.hpp"
#include "../../lib/operators/print.hpp"
#include "../../lib/operators/table_scan.hpp"
#include "../../lib/storage/dictionary_compression.hpp"
#include "../../lib/storage/reference_column.hpp"
#include "../../lib/storage/storage_manager.hpp"
#include "../../lib/storage/table.hpp"
#include "../../lib/types.hpp"

namespace opossum {

class ReferenceColumnTest : public ::testing::Test {
  virtual void SetUp() {
    _test_table = std::make_shared<opossum::Table>(opossum::Table(3));
    _test_table->add_column("a", "int", true);
    _test_table->add_column("b", "float");
    _test_table->append({123, 456.7f});
    _test_table->append({1234, 457.7f});
    _test_table->append({12345, 458.7f});
    _test_table->append({NULL_VALUE, 458.7f});
    _test_table->append({12345, 458.7f});

    _test_table_dict = std::make_shared<opossum::Table>(5);
    _test_table_dict->add_column("a", "int");
    _test_table_dict->add_column("b", "int");
    for (int i = 0; i <= 24; i += 2) _test_table_dict->append({i, 100 + i});

    DictionaryCompression::compress_chunks(*_test_table_dict, {ChunkID{0}, ChunkID{1}});

    StorageManager::get().add_table("test_table_dict", _test_table_dict);
  }

 public:
  std::shared_ptr<opossum::Table> _test_table, _test_table_dict;
  std::shared_ptr<ReferenceColumn> _ref_column_1;
};

TEST_F(ReferenceColumnTest, IsImmutable) {
  auto pos_list =
      std::make_shared<PosList>(std::initializer_list<RowID>({{ChunkID{0}, 0}, {ChunkID{0}, 1}, {ChunkID{0}, 2}}));
  auto ref_column = ReferenceColumn(_test_table, ColumnID{0}, pos_list);

  EXPECT_THROW(ref_column.append(1), std::logic_error);
}

TEST_F(ReferenceColumnTest, RetrievesValues) {
  // PosList with (0, 0), (0, 1), (0, 2)
  auto pos_list = std::make_shared<PosList>(std::initializer_list<RowID>(
      {_test_table->calculate_row_id(ChunkID{0}, 0), _test_table->calculate_row_id(ChunkID{0}, 1),
       _test_table->calculate_row_id(ChunkID{0}, 2)}));
  auto ref_column = ReferenceColumn(_test_table, ColumnID{0}, pos_list);

  auto& column = *(_test_table->get_chunk(ChunkID{0}).get_column(ColumnID{0}));

  EXPECT_EQ(ref_column[0], column[0]);
  EXPECT_EQ(ref_column[1], column[1]);
  EXPECT_EQ(ref_column[2], column[2]);
}

TEST_F(ReferenceColumnTest, RetrievesValuesOutOfOrder) {
  // PosList with (0, 1), (0, 2), (0, 0)
  auto pos_list = std::make_shared<PosList>(std::initializer_list<RowID>(
      {_test_table->calculate_row_id(ChunkID{0}, 1), _test_table->calculate_row_id(ChunkID{0}, 2),
       _test_table->calculate_row_id(ChunkID{0}, 0)}));
  auto ref_column = ReferenceColumn(_test_table, ColumnID{0}, pos_list);

  auto& column = *(_test_table->get_chunk(ChunkID{0}).get_column(ColumnID{0}));

  EXPECT_EQ(ref_column[0], column[1]);
  EXPECT_EQ(ref_column[1], column[2]);
  EXPECT_EQ(ref_column[2], column[0]);
}

TEST_F(ReferenceColumnTest, RetrievesValuesFromChunks) {
  // PosList with (0, 2), (1, 0), (1, 1)
  auto pos_list = std::make_shared<PosList>(std::initializer_list<RowID>(
      {_test_table->calculate_row_id(ChunkID{0}, 2), _test_table->calculate_row_id(ChunkID{1}, 0),
       _test_table->calculate_row_id(ChunkID{1}, 1)}));
  auto ref_column = ReferenceColumn(_test_table, ColumnID{0}, pos_list);

  auto& column_1 = *(_test_table->get_chunk(ChunkID{0}).get_column(ColumnID{0}));
  auto& column_2 = *(_test_table->get_chunk(ChunkID{1}).get_column(ColumnID{0}));

  EXPECT_EQ(ref_column[0], column_1[2]);
  EXPECT_TRUE(is_null(ref_column[1]) && is_null(column_2[0]));
  EXPECT_EQ(ref_column[2], column_2[1]);
}

TEST_F(ReferenceColumnTest, RetrieveNullValueFromNullRowID) {
  // PosList with (0, 0), (0, 1), NULL_ROW_ID, (0, 2)
  auto pos_list = std::make_shared<PosList>(
      std::initializer_list<RowID>({_test_table->calculate_row_id(ChunkID{0u}, ChunkOffset{0u}),
                                    _test_table->calculate_row_id(ChunkID{0u}, ChunkOffset{1u}), NULL_ROW_ID,
                                    _test_table->calculate_row_id(ChunkID{0u}, ChunkOffset{2u})}));

  auto ref_column = ReferenceColumn(_test_table, ColumnID{0u}, pos_list);

  auto& column = *(_test_table->get_chunk(ChunkID{0u}).get_column(ColumnID{0u}));

  EXPECT_EQ(ref_column[0], column[0]);
  EXPECT_EQ(ref_column[1], column[1]);
  EXPECT_TRUE(is_null(ref_column[2]));
  EXPECT_EQ(ref_column[3], column[2]);
}

}  // namespace opossum
