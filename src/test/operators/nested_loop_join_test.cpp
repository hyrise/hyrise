#include <iostream>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <utility>

#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "../../lib/operators/abstract_join_operator.hpp"
#include "../../lib/operators/get_table.hpp"
#include "../../lib/operators/nested_loop_join.hpp"
#include "../../lib/operators/print.hpp"
#include "../../lib/operators/table_scan.hpp"
#include "../../lib/storage/storage_manager.hpp"
#include "../../lib/storage/table.hpp"
#include "../../lib/types.hpp"

namespace opossum {

class OperatorsNestedLoopJoinTest : public BaseTest {
 protected:
  void SetUp() override {
    std::shared_ptr<Table> test_table = load_table("src/test/tables/int_float.tbl", 2);
    StorageManager::get().add_table("table_a", std::move(test_table));
    _gt_a = std::make_shared<GetTable>("table_a");

    std::shared_ptr<Table> test_table_2 = load_table("src/test/tables/int_float2.tbl", 2);
    StorageManager::get().add_table("table_b", std::move(test_table_2));
    _gt_b = std::make_shared<GetTable>("table_b");

    test_table = load_table("src/test/tables/int_string.tbl", 4);
    StorageManager::get().add_table("table_c", std::move(test_table));
    _gt_c = std::make_shared<GetTable>("table_c");

    test_table_2 = load_table("src/test/tables/string_int.tbl", 3);
    StorageManager::get().add_table("table_d", std::move(test_table_2));
    _gt_d = std::make_shared<GetTable>("table_d");

    std::shared_ptr<Table> test_table_dict = load_table("src/test/tables/int_float.tbl", 2);
    test_table_dict->compress_chunk(0);
    test_table_dict->compress_chunk(1);
    StorageManager::get().add_table("table_a_dict", std::move(test_table_dict));
    _gt_a_dict = std::make_shared<GetTable>("table_a_dict");

    std::shared_ptr<Table> test_table_2_dict = load_table("src/test/tables/int_float2.tbl", 2);
    test_table_2_dict->compress_chunk(0);
    test_table_2_dict->compress_chunk(1);
    StorageManager::get().add_table("table_b_dict", std::move(test_table_2_dict));
    _gt_b_dict = std::make_shared<GetTable>("table_b_dict");

    std::shared_ptr<Table> test_table_3_dict = load_table("src/test/tables/int_float.tbl", 2);
    test_table_3_dict->compress_chunk(0);
    StorageManager::get().add_table("table_c_dict", std::move(test_table_3_dict));
    _gt_c_dict = std::make_shared<GetTable>("table_c_dict");

    _gt_a->execute();
    _gt_b->execute();
    _gt_c->execute();
    _gt_d->execute();
    _gt_a_dict->execute();
    _gt_b_dict->execute();
    _gt_c_dict->execute();
  }

  std::shared_ptr<GetTable> _gt_a, _gt_b, _gt_c, _gt_d, _gt_a_dict, _gt_b_dict, _gt_c_dict;
};

TEST_F(OperatorsNestedLoopJoinTest, CrossJoin) {
  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_cross_join.tbl", 1);

  auto join =
      std::make_shared<NestedLoopJoin>(_gt_a, _gt_b, optional<std::pair<const std::string &, const std::string &>>(),
                                       "=", Cross, std::string("left"), std::string("right"));
  join->execute();

  EXPECT_TABLE_EQ(join->get_output(), expected_result);
}

TEST_F(OperatorsNestedLoopJoinTest, MissingJoinColumns) {
  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_left_join.tbl", 1);

  EXPECT_THROW(
      std::make_shared<NestedLoopJoin>(_gt_a, _gt_b, optional<std::pair<const std::string &, const std::string &>>(),
                                       "=", Left, std::string("left"), std::string("right")),
      std::runtime_error);
}

TEST_F(OperatorsNestedLoopJoinTest, LeftJoin) {
  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_left_join.tbl", 1);

  auto column_names = std::make_pair(std::string("a"), std::string("a"));
  auto join_columns = optional<std::pair<const std::string &, const std::string &>>(column_names);

  auto join = std::make_shared<NestedLoopJoin>(_gt_a, _gt_b, join_columns, "=", Left, std::string("left"),
                                               std::string("right"));
  join->execute();

  EXPECT_TABLE_EQ(join->get_output(), expected_result);
}

TEST_F(OperatorsNestedLoopJoinTest, LeftJoinOnString) {
  std::shared_ptr<Table> expected_result = load_table("src/test/tables/string_left_join.tbl", 1);

  auto column_names = std::make_pair(std::string("b"), std::string("b"));
  auto join_columns = optional<std::pair<const std::string &, const std::string &>>(column_names);

  auto join = std::make_shared<NestedLoopJoin>(_gt_c, _gt_d, join_columns, "=", Left, std::string("left"),
                                               std::string("right"));
  join->execute();

  EXPECT_TABLE_EQ(join->get_output(), expected_result);
}

TEST_F(OperatorsNestedLoopJoinTest, RightJoin) {
  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_right_join.tbl", 1);

  auto column_names = std::make_pair(std::string("a"), std::string("a"));
  auto join_columns = optional<std::pair<const std::string &, const std::string &>>(column_names);

  auto join = std::make_shared<NestedLoopJoin>(_gt_a, _gt_b, join_columns, "=", Right, std::string("left"),
                                               std::string("right"));
  join->execute();

  EXPECT_TABLE_EQ(join->get_output(), expected_result);
}

TEST_F(OperatorsNestedLoopJoinTest, InnerJoin) {
  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_inner_join.tbl", 1);

  auto column_names = std::make_pair(std::string("a"), std::string("a"));
  auto join_columns = optional<std::pair<const std::string &, const std::string &>>(column_names);

  auto join = std::make_shared<NestedLoopJoin>(_gt_a, _gt_b, join_columns, "=", Inner, std::string("left"),
                                               std::string("right"));
  join->execute();

  EXPECT_TABLE_EQ(join->get_output(), expected_result);
}

TEST_F(OperatorsNestedLoopJoinTest, InnerJoinOnString) {
  std::shared_ptr<Table> expected_result = load_table("src/test/tables/string_inner_join.tbl", 1);

  auto column_names = std::make_pair(std::string("b"), std::string("b"));
  auto join_columns = optional<std::pair<const std::string &, const std::string &>>(column_names);

  auto join = std::make_shared<NestedLoopJoin>(_gt_c, _gt_d, join_columns, "=", Inner, std::string("left"),
                                               std::string("right"));
  join->execute();

  EXPECT_TABLE_EQ(join->get_output(), expected_result);
}

TEST_F(OperatorsNestedLoopJoinTest, InnerRefJoin) {
  _gt_a->execute();
  _gt_b->execute();

  // scan that returns all rows
  auto scan_a = std::make_shared<TableScan>(_gt_a, "a", ">=", 0);
  scan_a->execute();
  auto scan_b = std::make_shared<TableScan>(_gt_b, "a", ">=", 0);
  scan_b->execute();

  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_inner_join.tbl", 1);

  auto column_names = std::make_pair(std::string("a"), std::string("a"));
  auto join_columns = optional<std::pair<const std::string &, const std::string &>>(column_names);

  auto join = std::make_shared<NestedLoopJoin>(scan_a, scan_b, join_columns, "=", Inner, std::string("left"),
                                               std::string("right"));
  join->execute();

  EXPECT_TABLE_EQ(join->get_output(), expected_result);
}

TEST_F(OperatorsNestedLoopJoinTest, InnerValueDictJoin) {
  _gt_a->execute();
  _gt_b_dict->execute();

  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_inner_join.tbl", 1);

  auto column_names = std::make_pair(std::string("a"), std::string("a"));
  auto join_columns = optional<std::pair<const std::string &, const std::string &>>(column_names);

  auto join = std::make_shared<NestedLoopJoin>(_gt_a, _gt_b_dict, join_columns, "=", Inner, std::string("left"),
                                               std::string("right"));
  join->execute();

  EXPECT_TABLE_EQ(join->get_output(), expected_result);
}

TEST_F(OperatorsNestedLoopJoinTest, InnerDictValueJoin) {
  _gt_a_dict->execute();
  _gt_b->execute();

  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_inner_join.tbl", 1);

  auto column_names = std::make_pair(std::string("a"), std::string("a"));
  auto join_columns = optional<std::pair<const std::string &, const std::string &>>(column_names);

  auto join = std::make_shared<NestedLoopJoin>(_gt_a_dict, _gt_b, join_columns, "=", Inner, std::string("left"),
                                               std::string("right"));
  join->execute();

  EXPECT_TABLE_EQ(join->get_output(), expected_result);
}

TEST_F(OperatorsNestedLoopJoinTest, InnerValueDictRefJoin) {
  _gt_a->execute();
  _gt_b_dict->execute();

  // scan that returns all rows
  auto scan_a = std::make_shared<TableScan>(_gt_a, "a", ">=", 0);
  scan_a->execute();
  auto scan_b = std::make_shared<TableScan>(_gt_b_dict, "a", ">=", 0);
  scan_b->execute();

  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_inner_join.tbl", 1);

  auto column_names = std::make_pair(std::string("a"), std::string("a"));
  auto join_columns = optional<std::pair<const std::string &, const std::string &>>(column_names);

  auto join = std::make_shared<NestedLoopJoin>(scan_a, scan_b, join_columns, "=", Inner, std::string("left"),
                                               std::string("right"));
  join->execute();

  EXPECT_TABLE_EQ(join->get_output(), expected_result);
}

TEST_F(OperatorsNestedLoopJoinTest, InnerDictValueRefJoin) {
  _gt_a_dict->execute();
  _gt_b->execute();

  // scan that returns all rows
  auto scan_a = std::make_shared<TableScan>(_gt_a_dict, "a", ">=", 0);
  scan_a->execute();
  auto scan_b = std::make_shared<TableScan>(_gt_b, "a", ">=", 0);
  scan_b->execute();

  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_inner_join.tbl", 1);

  auto column_names = std::make_pair(std::string("a"), std::string("a"));
  auto join_columns = optional<std::pair<const std::string &, const std::string &>>(column_names);

  auto join = std::make_shared<NestedLoopJoin>(scan_a, scan_b, join_columns, "=", Inner, std::string("left"),
                                               std::string("right"));
  join->execute();

  EXPECT_TABLE_EQ(join->get_output(), expected_result);
}

TEST_F(OperatorsNestedLoopJoinTest, InnerRefJoinFiltered) {
  _gt_a->execute();
  _gt_b->execute();

  auto scan_a = std::make_shared<TableScan>(_gt_a, "a", ">", 1000);
  scan_a->execute();
  auto scan_b = std::make_shared<TableScan>(_gt_b, "a", ">=", 0);
  scan_b->execute();

  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_inner_join_filtered.tbl", 1);

  auto column_names = std::make_pair(std::string("a"), std::string("a"));
  auto join_columns = optional<std::pair<const std::string &, const std::string &>>(column_names);

  auto join = std::make_shared<NestedLoopJoin>(scan_a, scan_b, join_columns, "=", Inner, std::string("left"),
                                               std::string("right"));
  join->execute();

  EXPECT_TABLE_EQ(join->get_output(), expected_result);
}

TEST_F(OperatorsNestedLoopJoinTest, InnerDictJoin) {
  _gt_a_dict->execute();
  _gt_b_dict->execute();

  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_inner_join.tbl", 1);

  auto column_names = std::make_pair(std::string("a"), std::string("a"));
  auto join_columns = optional<std::pair<const std::string &, const std::string &>>(column_names);

  auto join = std::make_shared<NestedLoopJoin>(_gt_a_dict, _gt_b_dict, join_columns, "=", Inner, std::string("left"),
                                               std::string("right"));
  join->execute();

  EXPECT_TABLE_EQ(join->get_output(), expected_result);
}

TEST_F(OperatorsNestedLoopJoinTest, InnerRefDictJoin) {
  _gt_a_dict->execute();
  _gt_b_dict->execute();

  // scan that returns all rows
  auto scan_a = std::make_shared<TableScan>(_gt_a_dict, "a", ">=", 0);
  scan_a->execute();
  auto scan_b = std::make_shared<TableScan>(_gt_b_dict, "a", ">=", 0);
  scan_b->execute();

  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_inner_join.tbl", 1);

  auto column_names = std::make_pair(std::string("a"), std::string("a"));
  auto join_columns = optional<std::pair<const std::string &, const std::string &>>(column_names);

  auto join = std::make_shared<NestedLoopJoin>(scan_a, scan_b, join_columns, "=", Inner, std::string("left"),
                                               std::string("right"));
  join->execute();

  EXPECT_TABLE_EQ(join->get_output(), expected_result);
}

TEST_F(OperatorsNestedLoopJoinTest, InnerRefDictJoinFiltered) {
  _gt_a_dict->execute();
  _gt_b_dict->execute();

  auto scan_a = std::make_shared<TableScan>(_gt_a_dict, "a", ">", 1000);
  scan_a->execute();
  auto scan_b = std::make_shared<TableScan>(_gt_b_dict, "a", ">=", 0);
  scan_b->execute();

  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_inner_join_filtered.tbl", 1);

  auto column_names = std::make_pair(std::string("a"), std::string("a"));
  auto join_columns = optional<std::pair<const std::string &, const std::string &>>(column_names);

  auto join = std::make_shared<NestedLoopJoin>(scan_a, scan_b, join_columns, "=", Inner, std::string("left"),
                                               std::string("right"));
  join->execute();

  EXPECT_TABLE_EQ(join->get_output(), expected_result);
}

TEST_F(OperatorsNestedLoopJoinTest, InnerJoinBig) {
  _gt_c->execute();
  _gt_d->execute();

  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_string_inner_join.tbl", 1);

  auto column_names = std::make_pair(std::string("a"), std::string("a"));
  auto join_columns = optional<std::pair<const std::string &, const std::string &>>(column_names);

  auto join = std::make_shared<NestedLoopJoin>(_gt_c, _gt_d, join_columns, "=", Inner, std::string("left"),
                                               std::string("right"));
  join->execute();

  EXPECT_TABLE_EQ(join->get_output(), expected_result);
}

TEST_F(OperatorsNestedLoopJoinTest, InnerRefJoinFilteredBig) {
  _gt_c->execute();
  _gt_d->execute();

  auto scan_c = std::make_shared<TableScan>(_gt_c, "a", ">=", 0);
  scan_c->execute();
  auto scan_d = std::make_shared<TableScan>(_gt_d, "a", ">=", 6);
  scan_d->execute();

  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_string_inner_join_filtered.tbl", 1);

  auto column_names = std::make_pair(std::string("a"), std::string("a"));
  auto join_columns = optional<std::pair<const std::string &, const std::string &>>(column_names);

  auto join = std::make_shared<NestedLoopJoin>(scan_c, scan_d, join_columns, "=", Inner, std::string("left"),
                                               std::string("right"));
  join->execute();

  EXPECT_TABLE_EQ(join->get_output(), expected_result);
}

/*
We are not going to support this for NestedLoopJoins due to complexity of writing the output.
The optimizer could rebuild the functionality by using a Left Outer Join and an additional Anti Join.
*/
TEST_F(OperatorsNestedLoopJoinTest, DISABLED_OuterJoin) {
  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_outer_join.tbl", 1);

  auto column_names = std::make_pair(std::string("a"), std::string("a"));
  auto join_columns = optional<std::pair<const std::string &, const std::string &>>(column_names);

  auto join = std::make_shared<NestedLoopJoin>(_gt_a, _gt_b, join_columns, "=", Outer, std::string("left"),
                                               std::string("right"));
  join->execute();

  EXPECT_TABLE_EQ(join->get_output(), expected_result);
}

// This is not implemented yet.
TEST_F(OperatorsNestedLoopJoinTest, DISABLED_NaturalJoin) {
  _gt_a->execute();
  _gt_b->execute();

  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_natural_join.tbl", 1);

  auto column_names = std::make_pair(std::string("a"), std::string("a"));
  auto join_columns = optional<std::pair<const std::string &, const std::string &>>(column_names);

  auto join = std::make_shared<NestedLoopJoin>(_gt_a, _gt_b, join_columns, "=", Natural, std::string("left"),
                                               std::string("right"));
  join->execute();

  EXPECT_TABLE_EQ(join->get_output(), expected_result);
}

TEST_F(OperatorsNestedLoopJoinTest, SelfJoin) {
  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_self_join.tbl", 1);

  auto column_names = std::make_pair(std::string("a"), std::string("a"));
  auto join_columns = optional<std::pair<const std::string &, const std::string &>>(column_names);

  auto join = std::make_shared<NestedLoopJoin>(_gt_a, _gt_a, join_columns, "=", Self, std::string("left"),
                                               std::string("right"));
  join->execute();

  EXPECT_TABLE_EQ(join->get_output(), expected_result);
}

TEST_F(OperatorsNestedLoopJoinTest, SmallerInnerJoin) {
  std::shared_ptr<Table> expected_int_result =
      load_table("src/test/tables/joinoperators/int_smaller_inner_join.tbl", 1);
  std::shared_ptr<Table> expected_float_result =
      load_table("src/test/tables/joinoperators/float_smaller_inner_join.tbl", 1);

  auto column_names = std::make_pair(std::string("a"), std::string("a"));
  auto join_columns = optional<std::pair<const std::string &, const std::string &>>(column_names);

  // Joining two Integer Columns
  auto join = std::make_shared<NestedLoopJoin>(_gt_a, _gt_b, join_columns, "<", Inner, std::string("left"),
                                               std::string("right"));
  join->execute();
  EXPECT_TABLE_EQ(join->get_output(), expected_int_result);

  auto column_names2 = std::make_pair(std::string("b"), std::string("b"));
  auto join_columns2 = optional<std::pair<const std::string &, const std::string &>>(column_names2);
  // Joining two Float Columns
  join = std::make_shared<NestedLoopJoin>(_gt_a, _gt_b, join_columns2, "<", Inner, std::string("left"),
                                          std::string("right"));
  join->execute();

  EXPECT_TABLE_EQ(join->get_output(), expected_float_result);
}

TEST_F(OperatorsNestedLoopJoinTest, SmallerEqualInnerJoin) {
  std::shared_ptr<Table> expected_int_result =
      load_table("src/test/tables/joinoperators/int_smallerequal_inner_join.tbl", 1);
  std::shared_ptr<Table> expected_float_result =
      load_table("src/test/tables/joinoperators/float_smallerequal_inner_join.tbl", 1);

  auto column_names = std::make_pair(std::string("a"), std::string("a"));
  auto join_columns = optional<std::pair<const std::string &, const std::string &>>(column_names);

  // Joining two Integer Columns
  auto join = std::make_shared<NestedLoopJoin>(_gt_a, _gt_b, join_columns, "<=", Inner, std::string("left"),
                                               std::string("right"));
  join->execute();
  EXPECT_TABLE_EQ(join->get_output(), expected_int_result);

  auto column_names2 = std::make_pair(std::string("b"), std::string("b"));
  auto join_columns2 = optional<std::pair<const std::string &, const std::string &>>(column_names2);

  // Joining two Float Columns
  join = std::make_shared<NestedLoopJoin>(_gt_a, _gt_b, join_columns2, "<=", Inner, std::string("left"),
                                          std::string("right"));
  join->execute();

  EXPECT_TABLE_EQ(join->get_output(), expected_float_result);
}

TEST_F(OperatorsNestedLoopJoinTest, GreaterInnerJoin) {
  std::shared_ptr<Table> expected_int_result =
      load_table("src/test/tables/joinoperators/int_greater_inner_join.tbl", 1);
  std::shared_ptr<Table> expected_float_result =
      load_table("src/test/tables/joinoperators/float_greater_inner_join.tbl", 1);

  auto column_names = std::make_pair(std::string("a"), std::string("a"));
  auto join_columns = optional<std::pair<const std::string &, const std::string &>>(column_names);

  // Joining two Integer Column
  auto join = std::make_shared<NestedLoopJoin>(_gt_a, _gt_b, join_columns, ">", Inner, std::string("left"),
                                               std::string("right"));
  join->execute();
  EXPECT_TABLE_EQ(join->get_output(), expected_int_result);

  auto column_names2 = std::make_pair(std::string("b"), std::string("b"));
  auto join_columns2 = optional<std::pair<const std::string &, const std::string &>>(column_names2);

  // Joining two Float Columns
  join = std::make_shared<NestedLoopJoin>(_gt_a, _gt_b, join_columns2, ">", Inner, std::string("left"),
                                          std::string("right"));
  join->execute();

  EXPECT_TABLE_EQ(join->get_output(), expected_float_result);
}

TEST_F(OperatorsNestedLoopJoinTest, GreaterEqualInnerJoin) {
  std::shared_ptr<Table> expected_int_result =
      load_table("src/test/tables/joinoperators/int_greaterequal_inner_join.tbl", 1);
  std::shared_ptr<Table> expected_float_result =
      load_table("src/test/tables/joinoperators/float_greaterequal_inner_join.tbl", 1);

  auto column_names = std::make_pair(std::string("a"), std::string("a"));
  auto join_columns = optional<std::pair<const std::string &, const std::string &>>(column_names);

  // Joining two Integer Columns
  auto join = std::make_shared<NestedLoopJoin>(_gt_a, _gt_b, join_columns, ">=", Inner, std::string("left"),
                                               std::string("right"));
  join->execute();
  EXPECT_TABLE_EQ(join->get_output(), expected_int_result);

  auto column_names2 = std::make_pair(std::string("b"), std::string("b"));
  auto join_columns2 = optional<std::pair<const std::string &, const std::string &>>(column_names2);

  // Joining two Float Columns
  join = std::make_shared<NestedLoopJoin>(_gt_a, _gt_b, join_columns2, ">=", Inner, std::string("left"),
                                          std::string("right"));
  join->execute();

  EXPECT_TABLE_EQ(join->get_output(), expected_float_result);
}

TEST_F(OperatorsNestedLoopJoinTest, NotEqualInnerJoin) {
  std::shared_ptr<Table> expected_int_result =
      load_table("src/test/tables/joinoperators/int_notequal_inner_join.tbl", 1);
  std::shared_ptr<Table> expected_float_result =
      load_table("src/test/tables/joinoperators/float_notequal_inner_join.tbl", 1);

  auto column_names = std::make_pair(std::string("a"), std::string("a"));
  auto join_columns = optional<std::pair<const std::string &, const std::string &>>(column_names);

  // Joining two Integer Columns
  auto join = std::make_shared<NestedLoopJoin>(_gt_a, _gt_b, join_columns, "!=", Inner, std::string("left"),
                                               std::string("right"));
  join->execute();
  EXPECT_TABLE_EQ(join->get_output(), expected_int_result);

  auto column_names2 = std::make_pair(std::string("b"), std::string("b"));
  auto join_columns2 = optional<std::pair<const std::string &, const std::string &>>(column_names2);

  // Joining two Float Columns
  join = std::make_shared<NestedLoopJoin>(_gt_a, _gt_b, join_columns2, "!=", Inner, std::string("left"),
                                          std::string("right"));
  join->execute();

  EXPECT_TABLE_EQ(join->get_output(), expected_float_result);
}

TEST_F(OperatorsNestedLoopJoinTest, JoinOnMixedValueAndDictionaryColumns) {
  std::shared_ptr<Table> expected_int_result = load_table("src/test/tables/int_inner_join.tbl", 1);

  auto column_names = std::make_pair(std::string("a"), std::string("a"));
  auto join_columns = optional<std::pair<const std::string &, const std::string &>>(column_names);

  auto join = std::make_shared<NestedLoopJoin>(_gt_c_dict, _gt_b, join_columns, "=", Inner, std::string("left"),
                                               std::string("right"));
  join->execute();
  EXPECT_TABLE_EQ(join->get_output(), expected_int_result);
}

TEST_F(OperatorsNestedLoopJoinTest, JoinOnMixedValueAndReferenceColumns) {
  std::shared_ptr<Table> expected_int_result = load_table("src/test/tables/int_inner_join.tbl", 1);

  // scan that returns all rows
  auto scan_a = std::make_shared<TableScan>(_gt_a, "a", ">=", 0);
  scan_a->execute();

  auto column_names = std::make_pair(std::string("a"), std::string("a"));
  auto join_columns = optional<std::pair<const std::string &, const std::string &>>(column_names);

  auto join = std::make_shared<NestedLoopJoin>(scan_a, _gt_b, join_columns, "=", Inner, std::string("left"),
                                               std::string("right"));
  join->execute();
  EXPECT_TABLE_EQ(join->get_output(), expected_int_result);
}

}  // namespace opossum
