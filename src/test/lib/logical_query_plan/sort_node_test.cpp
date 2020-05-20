#include <memory>
#include <vector>

#include "base_test.hpp"
#include "expression/expression_functional.hpp"
#include "logical_query_plan/lqp_utils.hpp"
#include "logical_query_plan/sort_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

class SortNodeTest : public BaseTest {
 protected:
  void SetUp() override {
    Hyrise::get().storage_manager.add_table("table_a",
                                            load_table("resources/test_data/tbl/int_float_double_string.tbl", 2));

    _table_node = StoredTableNode::make("table_a");

    _a_i = _table_node->get_column("i");
    _a_f = _table_node->get_column("f");
    _a_d = _table_node->get_column("d");

    _sort_node = SortNode::make(expression_vector(_a_i), std::vector<SortMode>{SortMode::Ascending}, _table_node);
  }

  std::shared_ptr<StoredTableNode> _table_node;
  std::shared_ptr<SortNode> _sort_node;
  std::shared_ptr<LQPColumnExpression> _a_i, _a_f, _a_d;
};

TEST_F(SortNodeTest, Descriptions) {
  EXPECT_EQ(_sort_node->description(), "[Sort] i (Ascending)");

  auto sort_b = SortNode::make(expression_vector(_a_i), std::vector<SortMode>{SortMode::Descending}, _table_node);
  EXPECT_EQ(sort_b->description(), "[Sort] i (Descending)");

  auto sort_c = SortNode::make(expression_vector(_a_d, _a_f, _a_i),
                               std::vector<SortMode>{SortMode::Descending, SortMode::Ascending, SortMode::Descending});
  sort_c->set_left_input(_table_node);
  EXPECT_EQ(sort_c->description(), "[Sort] d (Descending), f (Ascending), i (Descending)");
}

TEST_F(SortNodeTest, HashingAndEqualityCheck) {
  EXPECT_EQ(*_sort_node, *_sort_node);

  const auto sort_a = SortNode::make(expression_vector(_a_i), std::vector<SortMode>{SortMode::Descending}, _table_node);
  const auto sort_b =
      SortNode::make(expression_vector(_a_d, _a_f, _a_i),
                     std::vector<SortMode>{SortMode::Descending, SortMode::Ascending, SortMode::Descending});
  const auto sort_c = SortNode::make(expression_vector(_a_i), std::vector<SortMode>{SortMode::Ascending}, _table_node);

  EXPECT_NE(*_sort_node, *sort_a);
  EXPECT_NE(*_sort_node, *sort_b);
  EXPECT_EQ(*_sort_node, *sort_c);

  EXPECT_NE(_sort_node->hash(), sort_a->hash());
  EXPECT_NE(_sort_node->hash(), sort_b->hash());
  EXPECT_EQ(_sort_node->hash(), sort_c->hash());
}

TEST_F(SortNodeTest, Copy) {
  EXPECT_EQ(*_sort_node->deep_copy(), *_sort_node);

  const auto sort_b = SortNode::make(
      expression_vector(_a_d, _a_f, _a_i),
      std::vector<SortMode>{SortMode::Descending, SortMode::Ascending, SortMode::Descending}, _table_node);
  EXPECT_EQ(*sort_b->deep_copy(), *sort_b);
}

TEST_F(SortNodeTest, NodeExpressions) {
  ASSERT_EQ(_sort_node->node_expressions.size(), 1u);
  EXPECT_EQ(*_sort_node->node_expressions.at(0), *_a_i);
}

}  // namespace opossum
