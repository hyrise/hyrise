#include <memory>

#include "gtest/gtest.h"

#include "base_test.hpp"

#include "expression/expression_functional.hpp"
#include "logical_query_plan/lqp_utils.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

class PredicateNodeTest : public ::testing::Test {
 protected:
  void SetUp() override {
    StorageManager::get().add_table("table_a", load_table("src/test/tables/int_float_double_string.tbl", 2));

    _table_node = StoredTableNode::make("table_a");
    _i = {_table_node, ColumnID{0}};
    _f = {_table_node, ColumnID{1}};

    _predicate_node = PredicateNode::make(equals_(_i, 5), _table_node);
  }

  void TearDown() override { StorageManager::reset(); }

  std::shared_ptr<StoredTableNode> _table_node;
  LQPColumnReference _i, _f;
  std::shared_ptr<PredicateNode> _predicate_node;
};

TEST_F(PredicateNodeTest, Descriptions) { EXPECT_EQ(_predicate_node->description(), "[Predicate] i = 5"); }

TEST_F(PredicateNodeTest, Equals) {
  EXPECT_EQ(*_predicate_node, *_predicate_node);

  const auto other_predicate_node_a = PredicateNode::make(equals_(_i, 5), _table_node);
  const auto other_predicate_node_b = PredicateNode::make(equals_(_f, 5), _table_node);
  const auto other_predicate_node_c = PredicateNode::make(not_equals_(_i, 5), _table_node);
  const auto other_predicate_node_d = PredicateNode::make(equals_(_i, 6), _table_node);

  EXPECT_EQ(*other_predicate_node_a, *_predicate_node);
  EXPECT_NE(*other_predicate_node_b, *_predicate_node);
  EXPECT_NE(*other_predicate_node_c, *_predicate_node);
  EXPECT_NE(*other_predicate_node_d, *_predicate_node);
}

TEST_F(PredicateNodeTest, Copy) { EXPECT_EQ(*_predicate_node->deep_copy(), *_predicate_node); }

}  // namespace opossum
