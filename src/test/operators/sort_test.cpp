#include <iostream>
#include <memory>
#include <utility>

#include "base_test.hpp"

#include "operators/abstract_read_only_operator.hpp"
#include "operators/join_nested_loop.hpp"
#include "operators/print.hpp"
#include "operators/sort.hpp"
#include "operators/table_scan.hpp"
#include "operators/table_wrapper.hpp"
#include "operators/union_all.hpp"
#include "storage/chunk_encoder.hpp"
#include "storage/table.hpp"
#include "types.hpp"

namespace opossum {

class OperatorsSortTest : public BaseTestWithParam<EncodingType> {
 protected:
  void SetUp() override {
    _table_wrapper = std::make_shared<TableWrapper>(load_table("resources/test_data/tbl/int_float.tbl", 2));
    _table_wrapper_null =
        std::make_shared<TableWrapper>(load_table("resources/test_data/tbl/int_float_with_null.tbl", 2));

    auto table_dict = load_table("resources/test_data/tbl/int_float.tbl", 2);
    ChunkEncoder::encode_all_chunks(table_dict, EncodingType::Dictionary);

    auto table_null_dict = load_table("resources/test_data/tbl/int_float_with_null.tbl", 2);
    ChunkEncoder::encode_all_chunks(table_null_dict, EncodingType::Dictionary);

    _table_wrapper_dict = std::make_shared<TableWrapper>(std::move(table_dict));
    _table_wrapper_dict->execute();

    _table_wrapper_null_dict = std::make_shared<TableWrapper>(std::move(table_null_dict));
    _table_wrapper_null_dict->execute();

    _table_wrapper_outer_join = std::make_shared<TableWrapper>(load_table("resources/test_data/tbl/int_float2.tbl", 2));
    _table_wrapper_outer_join->execute();

    _table_wrapper->execute();
    _table_wrapper_null->execute();
  }

 protected:
  std::shared_ptr<TableWrapper> _table_wrapper, _table_wrapper_null, _table_wrapper_dict, _table_wrapper_null_dict,
      _table_wrapper_outer_join;
};

auto sort_test_formatter = [](const ::testing::TestParamInfo<EncodingType> info) {
  return std::to_string(static_cast<uint32_t>(info.param));
};

TEST_F(OperatorsSortTest, AscendingSortOfOneColumn) {
  std::shared_ptr<Table> expected_result = load_table("resources/test_data/tbl/int_float_sorted.tbl", 2);

  auto sort = std::make_shared<Sort>(
      _table_wrapper, std::vector<SortColumnDefinition>{SortColumnDefinition{ColumnID{0}, OrderByMode::Ascending}}, 2u);
  sort->execute();

  EXPECT_TABLE_EQ_ORDERED(sort->get_output(), expected_result);
}

TEST_F(OperatorsSortTest, AscendingSortOfEmptyColumn) {
  std::shared_ptr<Table> expected_result = load_table("resources/test_data/tbl/int_empty.tbl", 3);

  auto input = std::make_shared<TableWrapper>(load_table("resources/test_data/tbl/int_empty.tbl", 3));
  input->execute();

  auto sort = std::make_shared<Sort>(
      input, std::vector<SortColumnDefinition>{SortColumnDefinition{ColumnID{0}, OrderByMode::Ascending}}, 2u);
  sort->execute();

  EXPECT_TABLE_EQ_ORDERED(sort->get_output(), expected_result);
}

TEST_F(OperatorsSortTest, AscendingSortOfFilteredColumn) {
  std::shared_ptr<Table> expected_result = load_table("resources/test_data/tbl/int_float_filtered_sorted.tbl", 2);

  auto input = std::make_shared<TableWrapper>(load_table("resources/test_data/tbl/int_float.tbl", 1));
  input->execute();

  auto scan = create_table_scan(input, ColumnID{0}, PredicateCondition::NotEquals, 123);
  scan->execute();

  auto sort = std::make_shared<Sort>(
      scan, std::vector<SortColumnDefinition>{SortColumnDefinition{ColumnID{0}, OrderByMode::Ascending}}, 2u);
  sort->execute();

  EXPECT_TABLE_EQ_ORDERED(sort->get_output(), expected_result);
}

TEST_F(OperatorsSortTest, AscendingSortOfEmptyFilteredColumn) {
  std::shared_ptr<Table> expected_result = load_table("resources/test_data/tbl/int_empty.tbl", 2);

  auto input = std::make_shared<TableWrapper>(load_table("resources/test_data/tbl/int.tbl", 1));
  input->execute();

  auto scan = create_table_scan(input, ColumnID{0}, PredicateCondition::Equals, 17);
  scan->execute();

  auto sort = std::make_shared<Sort>(
      scan, std::vector<SortColumnDefinition>{SortColumnDefinition{ColumnID{0}, OrderByMode::Ascending}}, 2u);
  sort->execute();

  EXPECT_TABLE_EQ_ORDERED(sort->get_output(), expected_result);
}

TEST_F(OperatorsSortTest, AscendingSortOfOneColumnWithoutChunkSize) {
  std::shared_ptr<Table> expected_result = load_table("resources/test_data/tbl/int_float_sorted.tbl", 2);

  auto sort = std::make_shared<Sort>(
      _table_wrapper, std::vector<SortColumnDefinition>{SortColumnDefinition{ColumnID{0}, OrderByMode::Ascending}});
  sort->execute();

  EXPECT_TABLE_EQ_ORDERED(sort->get_output(), expected_result);
}

TEST_F(OperatorsSortTest, DoubleSortOfOneColumn) {
  std::shared_ptr<Table> expected_result = load_table("resources/test_data/tbl/int_float_sorted.tbl", 2);

  auto sort1 = std::make_shared<Sort>(
      _table_wrapper, std::vector<SortColumnDefinition>{SortColumnDefinition{ColumnID{0}, OrderByMode::Descending}},
      2u);
  sort1->execute();

  auto sort2 = std::make_shared<Sort>(
      sort1, std::vector<SortColumnDefinition>{SortColumnDefinition{ColumnID{0}, OrderByMode::Ascending}}, 2u);
  sort2->execute();

  EXPECT_TABLE_EQ_ORDERED(sort2->get_output(), expected_result);
}

TEST_F(OperatorsSortTest, DescendingSortOfOneColumn) {
  std::shared_ptr<Table> expected_result = load_table("resources/test_data/tbl/int_float_reverse.tbl", 2);

  auto sort = std::make_shared<Sort>(
      _table_wrapper, std::vector<SortColumnDefinition>{SortColumnDefinition{ColumnID{0}, OrderByMode::Descending}},
      2u);
  sort->execute();

  EXPECT_TABLE_EQ_ORDERED(sort->get_output(), expected_result);
}

TEST_F(OperatorsSortTest, MultipleColumnSortIsStable) {
  auto table_wrapper = std::make_shared<TableWrapper>(load_table("resources/test_data/tbl/int_float4.tbl", 2));
  table_wrapper->execute();

  std::shared_ptr<Table> expected_result = load_table("resources/test_data/tbl/int_float2_sorted.tbl", 2);

  auto sort_definitions =
      std::vector<SortColumnDefinition>{{SortColumnDefinition{ColumnID{0}, OrderByMode::Ascending},
                                         SortColumnDefinition{ColumnID{1}, OrderByMode::Ascending}}};
  auto sort = std::make_shared<Sort>(table_wrapper, sort_definitions, 2u);
  sort->execute();

  EXPECT_TABLE_EQ_ORDERED(sort->get_output(), expected_result);
}

TEST_F(OperatorsSortTest, MultipleColumnSortIsStableMixedOrder) {
  auto table_wrapper = std::make_shared<TableWrapper>(load_table("resources/test_data/tbl/int_float4.tbl", 2));
  table_wrapper->execute();

  std::shared_ptr<Table> expected_result = load_table("resources/test_data/tbl/int_float2_sorted_mixed.tbl", 2);

  auto sort_definitions =
      std::vector<SortColumnDefinition>{{SortColumnDefinition{ColumnID{0}, OrderByMode::Ascending},
                                         SortColumnDefinition{ColumnID{1}, OrderByMode::Descending}}};
  auto sort = std::make_shared<Sort>(table_wrapper, sort_definitions, 2u);
  sort->execute();

  EXPECT_TABLE_EQ_ORDERED(sort->get_output(), expected_result);
}

TEST_F(OperatorsSortTest, AscendingSortOfOneColumnWithNull) {
  std::shared_ptr<Table> expected_result = load_table("resources/test_data/tbl/int_float_null_sorted_asc.tbl", 2);

  auto sort = std::make_shared<Sort>(
      _table_wrapper_null, std::vector<SortColumnDefinition>{SortColumnDefinition{ColumnID{0}, OrderByMode::Ascending}},
      2u);
  sort->execute();

  EXPECT_TABLE_EQ_ORDERED(sort->get_output(), expected_result);
}

TEST_F(OperatorsSortTest, DescendingSortOfOneColumnWithNull) {
  std::shared_ptr<Table> expected_result = load_table("resources/test_data/tbl/int_float_null_sorted_desc.tbl", 2);

  auto sort = std::make_shared<Sort>(
      _table_wrapper_null,
      std::vector<SortColumnDefinition>{SortColumnDefinition{ColumnID{0}, OrderByMode::Descending}}, 2u);
  sort->execute();

  EXPECT_TABLE_EQ_ORDERED(sort->get_output(), expected_result);
}

TEST_F(OperatorsSortTest, AscendingSortOfOneColumnWithNullsLast) {
  std::shared_ptr<Table> expected_result =
      load_table("resources/test_data/tbl/int_float_null_sorted_asc_nulls_last.tbl", 2);

  auto sort = std::make_shared<Sort>(
      _table_wrapper_null,
      std::vector<SortColumnDefinition>{SortColumnDefinition{ColumnID{0}, OrderByMode::AscendingNullsLast}}, 2u);
  sort->execute();

  EXPECT_TABLE_EQ_ORDERED(sort->get_output(), expected_result);
}

TEST_F(OperatorsSortTest, DescendingSortOfOneColumnWithNullsLast) {
  std::shared_ptr<Table> expected_result =
      load_table("resources/test_data/tbl/int_float_null_sorted_desc_nulls_last.tbl", 2);

  auto sort = std::make_shared<Sort>(
      _table_wrapper_null,
      std::vector<SortColumnDefinition>{SortColumnDefinition{ColumnID{0}, OrderByMode::DescendingNullsLast}}, 2u);
  sort->execute();

  EXPECT_TABLE_EQ_ORDERED(sort->get_output(), expected_result);
}

TEST_F(OperatorsSortTest, AscendingSortOfOneDictSegmentWithNull) {
  std::shared_ptr<Table> expected_result = load_table("resources/test_data/tbl/int_float_null_sorted_asc.tbl", 2);

  auto sort = std::make_shared<Sort>(
      _table_wrapper_null_dict,
      std::vector<SortColumnDefinition>{SortColumnDefinition{ColumnID{0}, OrderByMode::Ascending}}, 2u);
  sort->execute();

  EXPECT_TABLE_EQ_ORDERED(sort->get_output(), expected_result);
}

TEST_F(OperatorsSortTest, DescendingSortOfOneDictSegmentWithNull) {
  std::shared_ptr<Table> expected_result = load_table("resources/test_data/tbl/int_float_null_sorted_desc.tbl", 2);

  auto sort = std::make_shared<Sort>(
      _table_wrapper_null_dict,
      std::vector<SortColumnDefinition>{SortColumnDefinition{ColumnID{0}, OrderByMode::Descending}}, 2u);
  sort->execute();

  EXPECT_TABLE_EQ_ORDERED(sort->get_output(), expected_result);
}

TEST_F(OperatorsSortTest, AscendingSortOfOneDictSegment) {
  std::shared_ptr<Table> expected_result = load_table("resources/test_data/tbl/int_float_sorted.tbl", 2);

  auto sort = std::make_shared<Sort>(
      _table_wrapper_dict, std::vector<SortColumnDefinition>{SortColumnDefinition{ColumnID{0}, OrderByMode::Ascending}},
      2u);
  sort->execute();

  EXPECT_TABLE_EQ_ORDERED(sort->get_output(), expected_result);
}

TEST_F(OperatorsSortTest, DescendingSortOfOneDictSegment) {
  std::shared_ptr<Table> expected_result = load_table("resources/test_data/tbl/int_float_reverse.tbl", 2);

  auto sort = std::make_shared<Sort>(
      _table_wrapper_dict,
      std::vector<SortColumnDefinition>{SortColumnDefinition{ColumnID{0}, OrderByMode::Descending}}, 2u);
  sort->execute();

  EXPECT_TABLE_EQ_ORDERED(sort->get_output(), expected_result);
}

TEST_F(OperatorsSortTest, SortAfterOuterJoin) {
  auto join =
      std::make_shared<JoinNestedLoop>(_table_wrapper, _table_wrapper_outer_join, JoinMode::FullOuter,
                                       OperatorJoinPredicate{{ColumnID{0}, ColumnID{0}}, PredicateCondition::Equals});
  join->execute();

  auto sort = std::make_shared<Sort>(
      join, std::vector<SortColumnDefinition>{SortColumnDefinition{ColumnID{0}, OrderByMode::Ascending}});
  sort->execute();

  std::shared_ptr<Table> expected_result =
      load_table("resources/test_data/tbl/join_operators/int_outer_join_sorted_asc.tbl", 2);
  EXPECT_TABLE_EQ_ORDERED(sort->get_output(), expected_result);
}

// TODO sort large table spanning multiple chunks (both reference and non-reference)
// TODO force materialize
// TODO input column referencing (a) multiple tables, (b) different columns within the same table
// TODO test output chunk size

}  // namespace opossum
