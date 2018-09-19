#include <iostream>
#include <memory>
#include <utility>

#include "base_test.hpp"
#include "gtest/gtest.h"

#include "operators/abstract_read_only_operator.hpp"
#include "operators/sort.hpp"
#include "operators/table_scan.hpp"
#include "operators/table_wrapper.hpp"
#include "operators/union_all.hpp"
#include "storage/chunk_encoder.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"
#include "types.hpp"

namespace opossum {

class OperatorsSortTest : public BaseTest, public ::testing::WithParamInterface<EncodingType> {
 protected:
  void SetUp() override {
    _encoding_type = GetParam();

    _table_wrapper = std::make_shared<TableWrapper>(load_table("src/test/tables/int_float.tbl", 2));
    _table_wrapper_null = std::make_shared<TableWrapper>(load_table("src/test/tables/int_float_with_null.tbl", 2));

    auto table = load_table("src/test/tables/int_float.tbl", 2);
    ChunkEncoder::encode_all_chunks(table, _encoding_type);

    auto table_dict = load_table("src/test/tables/int_float_with_null.tbl", 2);
    ChunkEncoder::encode_all_chunks(table_dict, _encoding_type);

    _table_wrapper_dict = std::make_shared<TableWrapper>(std::move(table));
    _table_wrapper_dict->execute();

    _table_wrapper_null_dict = std::make_shared<TableWrapper>(std::move(table_dict));
    _table_wrapper_null_dict->execute();

    _table_wrapper->execute();
    _table_wrapper_null->execute();
  }

 protected:
  std::shared_ptr<TableWrapper> _table_wrapper, _table_wrapper_null, _table_wrapper_dict, _table_wrapper_null_dict;
  EncodingType _encoding_type;
};

auto formatter = [](const ::testing::TestParamInfo<EncodingType> info) {
  return std::to_string(static_cast<uint32_t>(info.param));
};

// As long as two implementation of dictionary encoding exist, this ensure to run the tests for both.
INSTANTIATE_TEST_CASE_P(DictionaryEncodingTypes, OperatorsSortTest, ::testing::Values(EncodingType::Dictionary),
                        formatter);

TEST_P(OperatorsSortTest, AscendingSortOfOneColumn) {
  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_float_sorted.tbl", 2);

  auto sort = std::make_shared<Sort>(_table_wrapper, ColumnID{0}, OrderByMode::Ascending, 2u);
  sort->execute();

  EXPECT_TABLE_EQ_ORDERED(sort->get_output(), expected_result);
}

TEST_P(OperatorsSortTest, AscendingSortOFilteredColumn) {
  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_float_filtered_sorted.tbl", 2);

  auto input = std::make_shared<TableWrapper>(load_table("src/test/tables/int_float.tbl", 1));
  input->execute();

  auto scan =
      std::make_shared<TableScan>(input, OperatorScanPredicate{ColumnID{0}, PredicateCondition::NotEquals, 123});
  scan->execute();

  auto sort = std::make_shared<Sort>(scan, ColumnID{0}, OrderByMode::Ascending, 2u);
  sort->execute();

  EXPECT_TABLE_EQ_ORDERED(sort->get_output(), expected_result);
}

TEST_P(OperatorsSortTest, AscendingSortOfOneColumnWithoutChunkSize) {
  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_float_sorted.tbl", 2);

  auto sort = std::make_shared<Sort>(_table_wrapper, ColumnID{0}, OrderByMode::Ascending);
  sort->execute();

  EXPECT_TABLE_EQ_ORDERED(sort->get_output(), expected_result);
}

TEST_P(OperatorsSortTest, DoubleSortOfOneColumn) {
  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_float_sorted.tbl", 2);

  auto sort1 = std::make_shared<Sort>(_table_wrapper, ColumnID{0}, OrderByMode::Descending, 2u);
  sort1->execute();

  auto sort2 = std::make_shared<Sort>(sort1, ColumnID{0}, OrderByMode::Ascending, 2u);
  sort2->execute();

  EXPECT_TABLE_EQ_ORDERED(sort2->get_output(), expected_result);
}

TEST_P(OperatorsSortTest, DescendingSortOfOneColumn) {
  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_float_reverse.tbl", 2);

  auto sort = std::make_shared<Sort>(_table_wrapper, ColumnID{0}, OrderByMode::Descending, 2u);
  sort->execute();

  EXPECT_TABLE_EQ_ORDERED(sort->get_output(), expected_result);
}

TEST_P(OperatorsSortTest, MultipleColumnSortIsStable) {
  auto table_wrapper = std::make_shared<TableWrapper>(load_table("src/test/tables/int_float4.tbl", 2));
  table_wrapper->execute();

  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_float2_sorted.tbl", 2);

  // we want the output to be sorted after column a and in second place after column b.
  // So first we sort after column b and then after column a.
  auto sort_after_b = std::make_shared<Sort>(table_wrapper, ColumnID{1}, OrderByMode::Ascending, 2u);
  sort_after_b->execute();

  auto sort_after_a = std::make_shared<Sort>(sort_after_b, ColumnID{0}, OrderByMode::Ascending, 2u);
  sort_after_a->execute();

  EXPECT_TABLE_EQ_ORDERED(sort_after_a->get_output(), expected_result);
}

TEST_P(OperatorsSortTest, MultipleColumnSortIsStableMixedOrder) {
  auto table_wrapper = std::make_shared<TableWrapper>(load_table("src/test/tables/int_float4.tbl", 2));
  table_wrapper->execute();

  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_float2_sorted_mixed.tbl", 2);

  // we want the output to be sorted after column a and in second place after column b.
  // So first we sort after column b and then after column a.
  auto sort_after_b = std::make_shared<Sort>(table_wrapper, ColumnID{1}, OrderByMode::Descending, 2u);
  sort_after_b->execute();

  auto sort_after_a = std::make_shared<Sort>(sort_after_b, ColumnID{0}, OrderByMode::Ascending, 2u);
  sort_after_a->execute();

  EXPECT_TABLE_EQ_ORDERED(sort_after_a->get_output(), expected_result);
}

TEST_P(OperatorsSortTest, AscendingSortOfOneColumnWithNull) {
  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_float_null_sorted_asc.tbl", 2);

  auto sort = std::make_shared<Sort>(_table_wrapper_null, ColumnID{0}, OrderByMode::Ascending, 2u);
  sort->execute();

  EXPECT_TABLE_EQ_ORDERED(sort->get_output(), expected_result);
}

TEST_P(OperatorsSortTest, DescendingSortOfOneColumnWithNull) {
  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_float_null_sorted_desc.tbl", 2);

  auto sort = std::make_shared<Sort>(_table_wrapper_null, ColumnID{0}, OrderByMode::Descending, 2u);
  sort->execute();

  EXPECT_TABLE_EQ_ORDERED(sort->get_output(), expected_result);
}

TEST_P(OperatorsSortTest, AscendingSortOfOneColumnWithNullsLast) {
  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_float_null_sorted_asc_nulls_last.tbl", 2);

  auto sort = std::make_shared<Sort>(_table_wrapper_null, ColumnID{0}, OrderByMode::AscendingNullsLast, 2u);
  sort->execute();

  EXPECT_TABLE_EQ_ORDERED(sort->get_output(), expected_result);
}

TEST_P(OperatorsSortTest, DescendingSortOfOneColumnWithNullsLast) {
  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_float_null_sorted_desc_nulls_last.tbl", 2);

  auto sort = std::make_shared<Sort>(_table_wrapper_null, ColumnID{0}, OrderByMode::DescendingNullsLast, 2u);
  sort->execute();

  EXPECT_TABLE_EQ_ORDERED(sort->get_output(), expected_result);
}

TEST_P(OperatorsSortTest, AscendingSortOfOneDictSegmentWithNull) {
  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_float_null_sorted_asc.tbl", 2);

  auto sort = std::make_shared<Sort>(_table_wrapper_null_dict, ColumnID{0}, OrderByMode::Ascending, 2u);
  sort->execute();

  EXPECT_TABLE_EQ_ORDERED(sort->get_output(), expected_result);
}

TEST_P(OperatorsSortTest, DescendingSortOfOneDictSegmentWithNull) {
  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_float_null_sorted_desc.tbl", 2);

  auto sort = std::make_shared<Sort>(_table_wrapper_null_dict, ColumnID{0}, OrderByMode::Descending, 2u);
  sort->execute();

  EXPECT_TABLE_EQ_ORDERED(sort->get_output(), expected_result);
}

TEST_P(OperatorsSortTest, AscendingSortOfOneDictSegment) {
  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_float_sorted.tbl", 2);

  auto sort = std::make_shared<Sort>(_table_wrapper_dict, ColumnID{0}, OrderByMode::Ascending, 2u);
  sort->execute();

  EXPECT_TABLE_EQ_ORDERED(sort->get_output(), expected_result);
}

TEST_P(OperatorsSortTest, DescendingSortOfOneDictSegment) {
  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_float_reverse.tbl", 2);

  auto sort = std::make_shared<Sort>(_table_wrapper_dict, ColumnID{0}, OrderByMode::Descending, 2u);
  sort->execute();

  EXPECT_TABLE_EQ_ORDERED(sort->get_output(), expected_result);
}

}  // namespace opossum
