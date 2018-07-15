#include <memory>
#include <vector>

#include "gtest/gtest.h"

#include "base_test.hpp"

#include "expression/expression_functional.hpp"
#include "logical_query_plan/lqp_utils.hpp"
#include "logical_query_plan/sort_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

class SortNodeTest : public ::testing::Test {
 protected:
  void SetUp() override {
    StorageManager::get().add_table("table_a", load_table("src/test/tables/int_float_double_string.tbl", 2));

    _table_node = StoredTableNode::make("table_a");

    _a_i = {_table_node, ColumnID{0}};
    _a_f = {_table_node, ColumnID{1}};
    _a_d = {_table_node, ColumnID{2}};

    _sort_node = SortNode::make(expression_vector(_a_i), std::vector<OrderByMode>{OrderByMode::Ascending}, _table_node);
  }

  void TearDown() override { StorageManager::reset(); }

  std::shared_ptr<StoredTableNode> _table_node;
  std::shared_ptr<SortNode> _sort_node;
  LQPColumnReference _a_i, _a_f, _a_d;
};

TEST_F(SortNodeTest, Descriptions) {
  EXPECT_EQ(_sort_node->description(), "[Sort] i (Ascending)");

  auto sort_b = SortNode::make(expression_vector(_a_i), std::vector<OrderByMode>{OrderByMode::Descending}, _table_node);
  EXPECT_EQ(sort_b->description(), "[Sort] i (Descending)");

  auto sort_c = SortNode::make(
      expression_vector(_a_d, _a_f, _a_i),
      std::vector<OrderByMode>{OrderByMode::Descending, OrderByMode::Ascending, OrderByMode::Descending});
  sort_c->set_left_input(_table_node);
  EXPECT_EQ(sort_c->description(), "[Sort] d (Descending), f (Ascending), i (Descending)");
}

TEST_F(SortNodeTest, Equals) {
  EXPECT_EQ(*_sort_node, *_sort_node);

  const auto sort_a =
      SortNode::make(expression_vector(_a_i), std::vector<OrderByMode>{OrderByMode::Descending}, _table_node);
  const auto sort_b = SortNode::make(
      expression_vector(_a_d, _a_f, _a_i),
      std::vector<OrderByMode>{OrderByMode::Descending, OrderByMode::Ascending, OrderByMode::Descending});
  const auto sort_c =
      SortNode::make(expression_vector(_a_i), std::vector<OrderByMode>{OrderByMode::Ascending}, _table_node);

  EXPECT_NE(*_sort_node, *sort_a);
  EXPECT_NE(*_sort_node, *sort_b);
  EXPECT_EQ(*_sort_node, *sort_c);
}

TEST_F(SortNodeTest, Copy) {
  EXPECT_EQ(*_sort_node->deep_copy(), *_sort_node);

  const auto sort_b = SortNode::make(
      expression_vector(_a_d, _a_f, _a_i),
      std::vector<OrderByMode>{OrderByMode::Descending, OrderByMode::Ascending, OrderByMode::Descending}, _table_node);
  EXPECT_EQ(*sort_b->deep_copy(), *sort_b);
}

}  // namespace opossum
