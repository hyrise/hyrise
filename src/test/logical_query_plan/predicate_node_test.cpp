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

    _table_node = std::make_shared<StoredTableNode>("table_a");
  }

  std::shared_ptr<StoredTableNode> _table_node;
};

TEST_F(PredicateNodeTest, Descriptions) {
  auto predicate_a =
      std::make_shared<PredicateNode>(LQPColumnReference{_table_node, ColumnID{0}}, PredicateCondition::Equals, 5);
  predicate_a->set_left_child(_table_node);
  EXPECT_EQ(predicate_a->description(), "[Predicate] table_a.i = 5");

  auto predicate_b =
      std::make_shared<PredicateNode>(LQPColumnReference{_table_node, ColumnID{1}}, PredicateCondition::NotEquals, 2.5);
  predicate_b->set_left_child(_table_node);
  EXPECT_EQ(predicate_b->description(), "[Predicate] table_a.f != 2.5");

  auto predicate_c = std::make_shared<PredicateNode>(LQPColumnReference{_table_node, ColumnID{2}},
                                                     PredicateCondition::Between, 2.5, 10.0);
  predicate_c->set_left_child(_table_node);
  EXPECT_EQ(predicate_c->description(), "[Predicate] table_a.d BETWEEN 2.5 AND 10");

  auto predicate_d =
      std::make_shared<PredicateNode>(LQPColumnReference{_table_node, ColumnID{3}}, PredicateCondition::Equals, "test");
  predicate_d->set_left_child(_table_node);
  EXPECT_EQ(predicate_d->description(), "[Predicate] table_a.s = test");
}

}  // namespace opossum
