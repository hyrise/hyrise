#include <iostream>
#include <memory>
#include <utility>

#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "operators/abstract_read_only_operator.hpp"
#include "operators/sort.hpp"
#include "operators/table_scan.hpp"
#include "operators/table_wrapper.hpp"
#include "storage/dictionary_compression.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"
#include "types.hpp"

namespace opossum {

class OperatorsSortTest : public BaseTest {
 protected:
  void SetUp() override {
    _table_wrapper = std::make_shared<TableWrapper>(load_table("src/test/tables/int_float.tbl", 2));
    _table_wrapper_null = std::make_shared<TableWrapper>(load_table("src/test/tables/int_float_with_null.tbl", 2));

    auto table = load_table("src/test/tables/int_float.tbl", 2);
    DictionaryCompression::compress_table(*table);

    auto table_dict = load_table("src/test/tables/int_float_with_null.tbl", 2);
    DictionaryCompression::compress_table(*table_dict);

    _table_wrapper_dict = std::make_shared<TableWrapper>(std::move(table));
    _table_wrapper_dict->execute();

    _table_wrapper_null_dict = std::make_shared<TableWrapper>(std::move(table_dict));
    _table_wrapper_null_dict->execute();

    _table_wrapper->execute();
    _table_wrapper_null->execute();
  }

  std::shared_ptr<TableWrapper> _table_wrapper, _table_wrapper_null, _table_wrapper_dict, _table_wrapper_null_dict;
};

TEST_F(OperatorsSortTest, AscendingSortOfOneColumn) {
  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_float_sorted.tbl", 2);

  auto sort = std::make_shared<Sort>(_table_wrapper, ColumnID{0}, OrderByMode::Ascending, 2u);
  sort->execute();

  EXPECT_TABLE_EQ(sort->get_output(), expected_result, true);
}

TEST_F(OperatorsSortTest, AscendingSortOfOneColumnWithoutChunkSize) {
  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_float_sorted.tbl", 2);

  auto sort = std::make_shared<Sort>(_table_wrapper, ColumnID{0}, OrderByMode::Ascending);
  sort->execute();

  EXPECT_TABLE_EQ(sort->get_output(), expected_result, true);
}

TEST_F(OperatorsSortTest, DoubleSortOfOneColumn) {
  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_float_sorted.tbl", 2);

  auto sort1 = std::make_shared<Sort>(_table_wrapper, ColumnID{0}, OrderByMode::Descending, 2u);
  sort1->execute();

  auto sort2 = std::make_shared<Sort>(sort1, ColumnID{0}, OrderByMode::Ascending, 2u);
  sort2->execute();

  EXPECT_TABLE_EQ(sort2->get_output(), expected_result, true);
}

TEST_F(OperatorsSortTest, DescendingSortOfOneColumn) {
  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_float_reverse.tbl", 2);

  auto sort = std::make_shared<Sort>(_table_wrapper, ColumnID{0}, OrderByMode::Descending, 2u);
  sort->execute();

  EXPECT_TABLE_EQ(sort->get_output(), expected_result, true);
}

TEST_F(OperatorsSortTest, MultipleColumnSortIsStable) {
  auto table_wrapper = std::make_shared<TableWrapper>(load_table("src/test/tables/int_float4.tbl", 2));
  table_wrapper->execute();

  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_float2_sorted.tbl", 2);

  // we want the output to be sorted after column a and in second place after column b.
  // So first we sort after column b and then after column a.
  auto sort_after_b = std::make_shared<Sort>(table_wrapper, ColumnID{1}, OrderByMode::Ascending, 2u);
  sort_after_b->execute();

  auto sort_after_a = std::make_shared<Sort>(sort_after_b, ColumnID{0}, OrderByMode::Ascending, 2u);
  sort_after_a->execute();

  EXPECT_TABLE_EQ(sort_after_a->get_output(), expected_result, true);
}

TEST_F(OperatorsSortTest, MultipleColumnSortIsStableMixedOrder) {
  auto table_wrapper = std::make_shared<TableWrapper>(load_table("src/test/tables/int_float4.tbl", 2));
  table_wrapper->execute();

  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_float2_sorted_mixed.tbl", 2);

  // we want the output to be sorted after column a and in second place after column b.
  // So first we sort after column b and then after column a.
  auto sort_after_b = std::make_shared<Sort>(table_wrapper, ColumnID{1}, OrderByMode::Descending, 2u);
  sort_after_b->execute();

  auto sort_after_a = std::make_shared<Sort>(sort_after_b, ColumnID{0}, OrderByMode::Ascending, 2u);
  sort_after_a->execute();

  EXPECT_TABLE_EQ(sort_after_a->get_output(), expected_result, true);
}

TEST_F(OperatorsSortTest, AscendingSortOfOneColumnWithNull) {
  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_float_null_sorted_asc.tbl", 2);

  auto sort = std::make_shared<Sort>(_table_wrapper_null, ColumnID{0}, OrderByMode::Ascending, 2u);
  sort->execute();

  EXPECT_TABLE_EQ(sort->get_output(), expected_result, true);
}

TEST_F(OperatorsSortTest, DescendingSortOfOneColumnWithNull) {
  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_float_null_sorted_desc.tbl", 2);

  auto sort = std::make_shared<Sort>(_table_wrapper_null, ColumnID{0}, OrderByMode::Descending, 2u);
  sort->execute();

  EXPECT_TABLE_EQ(sort->get_output(), expected_result, true);
}

TEST_F(OperatorsSortTest, AscendingSortOfOneColumnWithNullsLast) {
  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_float_null_sorted_asc_nulls_last.tbl", 2);

  auto sort = std::make_shared<Sort>(_table_wrapper_null, ColumnID{0}, OrderByMode::AscendingNullsLast, 2u);
  sort->execute();

  EXPECT_TABLE_EQ(sort->get_output(), expected_result, true);
}

TEST_F(OperatorsSortTest, DescendingSortOfOneColumnWithNullsLast) {
  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_float_null_sorted_desc_nulls_last.tbl", 2);

  auto sort = std::make_shared<Sort>(_table_wrapper_null, ColumnID{0}, OrderByMode::DescendingNullsLast, 2u);
  sort->execute();

  EXPECT_TABLE_EQ(sort->get_output(), expected_result, true);
}

TEST_F(OperatorsSortTest, AscendingSortOfOneDictColumnWithNull) {
  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_float_null_sorted_asc.tbl", 2);

  auto sort = std::make_shared<Sort>(_table_wrapper_null_dict, ColumnID{0}, OrderByMode::Ascending, 2u);
  sort->execute();

  EXPECT_TABLE_EQ(sort->get_output(), expected_result, true);
}

TEST_F(OperatorsSortTest, DescendingSortOfOneDictColumnWithNull) {
  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_float_null_sorted_desc.tbl", 2);

  auto sort = std::make_shared<Sort>(_table_wrapper_null_dict, ColumnID{0}, OrderByMode::Descending, 2u);
  sort->execute();

  EXPECT_TABLE_EQ(sort->get_output(), expected_result, true);
}

TEST_F(OperatorsSortTest, AscendingSortOfOneDictColumn) {
  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_float_sorted.tbl", 2);

  auto sort = std::make_shared<Sort>(_table_wrapper_dict, ColumnID{0}, OrderByMode::Ascending, 2u);
  sort->execute();

  EXPECT_TABLE_EQ(sort->get_output(), expected_result, true);
}

TEST_F(OperatorsSortTest, DescendingSortOfOneDictColumn) {
  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_float_reverse.tbl", 2);

  auto sort = std::make_shared<Sort>(_table_wrapper_dict, ColumnID{0}, OrderByMode::Descending, 2u);
  sort->execute();

  EXPECT_TABLE_EQ(sort->get_output(), expected_result, true);
}

}  // namespace opossum
