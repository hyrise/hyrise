#include <memory>

#include "gtest/gtest.h"

#include "base_test.hpp"

#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"

namespace opossum {

class PredicateNodeTest : public BaseTest {
 protected:
  void SetUp() override {
    StorageManager::get().add_table("table_a", load_table("src/test/tables/int_float_double_string.tbl", 2));

    _table_node = StoredTableNode::make("table_a");
    _predicate_node = PredicateNode::make(LQPColumnReference{_table_node, ColumnID{0}}, PredicateCondition::Equals, 5);
    _predicate_node->set_left_input(_table_node);
  }

  std::shared_ptr<StoredTableNode> _table_node;
  std::shared_ptr<PredicateNode> _predicate_node;
};

TEST_F(PredicateNodeTest, Descriptions) {
  EXPECT_EQ(_predicate_node->description(), "[Predicate] table_a.i = 5");

  auto predicate_b =
      PredicateNode::make(LQPColumnReference{_table_node, ColumnID{1}}, PredicateCondition::NotEquals, 2.5);
  predicate_b->set_left_input(_table_node);
  EXPECT_EQ(predicate_b->description(), "[Predicate] table_a.f != 2.5");

  auto predicate_c =
      PredicateNode::make(LQPColumnReference{_table_node, ColumnID{2}}, PredicateCondition::Between, 2.5, 10.0);
  predicate_c->set_left_input(_table_node);
  EXPECT_EQ(predicate_c->description(), "[Predicate] table_a.d BETWEEN 2.5 AND 10");

  auto predicate_d =
      PredicateNode::make(LQPColumnReference{_table_node, ColumnID{3}}, PredicateCondition::Equals, "test");
  predicate_d->set_left_input(_table_node);
  EXPECT_EQ(predicate_d->description(), "[Predicate] table_a.s = test");
}

TEST_F(PredicateNodeTest, ShallowEquals) {
  EXPECT_TRUE(_predicate_node->shallow_equals(*_predicate_node));

  const auto other_predicate_node_a =
      PredicateNode::make(LQPColumnReference{_table_node, ColumnID{0}}, PredicateCondition::Equals, 5, _table_node);
  const auto other_predicate_node_b =
      PredicateNode::make(LQPColumnReference{_table_node, ColumnID{1}}, PredicateCondition::Equals, 5, _table_node);
  const auto other_predicate_node_c =
      PredicateNode::make(LQPColumnReference{_table_node, ColumnID{0}}, PredicateCondition::NotLike, 5, _table_node);
  const auto other_predicate_node_d =
      PredicateNode::make(LQPColumnReference{_table_node, ColumnID{0}}, PredicateCondition::Equals, 6, _table_node);

  EXPECT_TRUE(_predicate_node->shallow_equals(*other_predicate_node_a));
  EXPECT_FALSE(_predicate_node->shallow_equals(*other_predicate_node_b));
  EXPECT_FALSE(_predicate_node->shallow_equals(*other_predicate_node_c));
  EXPECT_FALSE(_predicate_node->shallow_equals(*other_predicate_node_d));
}

}  // namespace opossum
