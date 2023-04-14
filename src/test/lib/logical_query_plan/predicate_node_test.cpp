#include <memory>

#include "base_test.hpp"
#include "expression/expression_functional.hpp"
#include "logical_query_plan/lqp_utils.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"

namespace hyrise {

using namespace expression_functional;  // NOLINT(build/namespaces)

class PredicateNodeTest : public BaseTest {
 protected:
  void SetUp() override {
    _table_a = load_table("resources/test_data/tbl/int_float_double_string.tbl", ChunkOffset{2});
    Hyrise::get().storage_manager.add_table("table_a", _table_a);

    _table_node = StoredTableNode::make("table_a");
    _i = lqp_column_(_table_node, ColumnID{0});
    _f = lqp_column_(_table_node, ColumnID{1});

    _predicate_node = PredicateNode::make(equals_(_i, 5), _table_node);
  }

  std::shared_ptr<StoredTableNode> _table_node;
  std::shared_ptr<LQPColumnExpression> _i, _f;
  std::shared_ptr<PredicateNode> _predicate_node;
  std::shared_ptr<Table> _table_a;
};

TEST_F(PredicateNodeTest, Descriptions) {
  EXPECT_EQ(_predicate_node->description(), "[Predicate] i = 5");
}

TEST_F(PredicateNodeTest, HashingAndEqualityCheck) {
  EXPECT_EQ(*_predicate_node, *_predicate_node);
  const auto equal_table_node = StoredTableNode::make("table_a");
  const auto equal_i = equal_table_node->get_column("i");

  const auto other_predicate_node_a = PredicateNode::make(equals_(_i, 5), _table_node);
  const auto other_predicate_node_b = PredicateNode::make(equals_(_f, 5), _table_node);
  const auto other_predicate_node_c = PredicateNode::make(not_equals_(_i, 5), _table_node);
  const auto other_predicate_node_d = PredicateNode::make(equals_(_i, 6), _table_node);
  const auto other_predicate_node_e = PredicateNode::make(equals_(equal_i, 5), equal_table_node);

  EXPECT_EQ(*other_predicate_node_a, *_predicate_node);
  EXPECT_NE(*other_predicate_node_b, *_predicate_node);
  EXPECT_NE(*other_predicate_node_c, *_predicate_node);
  EXPECT_NE(*other_predicate_node_d, *_predicate_node);
  EXPECT_EQ(*other_predicate_node_e, *_predicate_node);

  EXPECT_EQ(other_predicate_node_a->hash(), _predicate_node->hash());
  EXPECT_NE(other_predicate_node_b->hash(), _predicate_node->hash());
  EXPECT_NE(other_predicate_node_c->hash(), _predicate_node->hash());
  EXPECT_NE(other_predicate_node_d->hash(), _predicate_node->hash());
  EXPECT_EQ(other_predicate_node_e->hash(), _predicate_node->hash());
}

TEST_F(PredicateNodeTest, Copy) {
  EXPECT_EQ(*_predicate_node->deep_copy(), *_predicate_node);
}

TEST_F(PredicateNodeTest, NodeExpressions) {
  ASSERT_EQ(_predicate_node->node_expressions.size(), 1u);
  EXPECT_EQ(*_predicate_node->node_expressions.at(0), *equals_(_i, 5));
}

TEST_F(PredicateNodeTest, ForwardUniqueColumnCombinations) {
  EXPECT_TRUE(_table_node->unique_column_combinations().empty());
  EXPECT_TRUE(_predicate_node->unique_column_combinations().empty());

  _table_a->add_soft_key_constraint({{ColumnID{0}}, KeyConstraintType::UNIQUE});
  const auto ucc = UniqueColumnCombination{{_i}};
  EXPECT_EQ(_table_node->unique_column_combinations().size(), 1);
  EXPECT_TRUE(_table_node->unique_column_combinations().contains(ucc));

  const auto& unique_column_combinations = _predicate_node->unique_column_combinations();
  EXPECT_EQ(unique_column_combinations.size(), 1);
  EXPECT_TRUE(unique_column_combinations.contains(ucc));
}

}  // namespace hyrise
