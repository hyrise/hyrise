#include <vector>

#include "logical_query_plan/projection_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "logical_query_plan/union_node.hpp"
#include "optimizer/strategy/stored_table_column_alignment_rule.hpp"
#include "strategy_base_test.hpp"

namespace opossum {

class StoredTableColumnAlignmentRuleTest : public StrategyBaseTest {
 public:
  void SetUp() override {
    Hyrise::get().storage_manager.add_table("t_a", load_table("resources/test_data/tbl/int_int_float.tbl", 1));
    Hyrise::get().storage_manager.add_table("t_b", load_table("resources/test_data/tbl/int_int_float.tbl", 1));

    _stn_left = StoredTableNode::make("t_a");
    _stn_left->set_pruned_chunk_ids({ChunkID{2}});
    _stn_left->set_pruned_column_ids({ColumnID{0}});
    _stn_right = std::static_pointer_cast<StoredTableNode>(_stn_left->deep_copy());

    _union_node = UnionNode::make(SetOperationMode::All);
    _union_node->set_left_input(_stn_left);
    _union_node->set_right_input(_stn_right);

    _rule = std::make_shared<StoredTableColumnAlignmentRule>();
  }

  std::shared_ptr<StoredTableColumnAlignmentRule> _rule;
  std::shared_ptr<UnionNode> _union_node;
  std::shared_ptr<StoredTableNode> _stn_left, _stn_right;
};

TEST_F(StoredTableColumnAlignmentRuleTest, EqualTableEqualChunksEqualColumns) {
  apply_rule(_rule, _union_node);

  EXPECT_EQ(_stn_left->pruned_column_ids(), (std::vector{ColumnID{0}}));
  EXPECT_EQ(_stn_right->pruned_column_ids(), (std::vector{ColumnID{0}}));
  EXPECT_EQ(_stn_left->hash(), _stn_right->hash());
  EXPECT_EQ(*_stn_left, *_stn_right);
}

TEST_F(StoredTableColumnAlignmentRuleTest, EqualTableEqualChunksDifferentColumns) {
  _stn_right->set_pruned_column_ids({ColumnID{0}, ColumnID{1}});
  _union_node->set_right_input(_stn_right);

  apply_rule(_rule, _union_node);

  EXPECT_EQ(_stn_left->pruned_column_ids(), (std::vector{ColumnID{0}}));
  EXPECT_EQ(_stn_right->pruned_column_ids(), (std::vector{ColumnID{0}}));
  EXPECT_EQ(_stn_left->hash(), _stn_right->hash());
  EXPECT_EQ(*_stn_left, *_stn_right);
}

TEST_F(StoredTableColumnAlignmentRuleTest, EqualTableDifferentChunksDifferentColumns) {
  _stn_right->set_pruned_chunk_ids({ChunkID{2}, ChunkID{3}});
  _stn_right->set_pruned_column_ids({ColumnID{0}, ColumnID{1}});
  _union_node->set_right_input(_stn_right);

  apply_rule(_rule, _union_node);

  EXPECT_EQ(_stn_left->pruned_column_ids(), (std::vector{ColumnID{0}}));
  EXPECT_EQ(_stn_right->pruned_column_ids(), (std::vector{ColumnID{0}, ColumnID{1}}));
  EXPECT_NE(_stn_left->hash(), _stn_right->hash());
  EXPECT_NE(*_stn_left, *_stn_right);
}

TEST_F(StoredTableColumnAlignmentRuleTest, DifferentTableEqualChunksDifferentColumns) {
  _stn_right = StoredTableNode::make("t_b");
  _stn_right->set_pruned_chunk_ids({ChunkID{2}});
  _stn_right->set_pruned_column_ids({ColumnID{0}, ColumnID{1}});
  _union_node->set_right_input(_stn_right);

  apply_rule(_rule, _union_node);

  EXPECT_EQ(_stn_left->pruned_column_ids(), (std::vector{ColumnID{0}}));
  EXPECT_EQ(_stn_right->pruned_column_ids(), (std::vector{ColumnID{0}, ColumnID{1}}));
  EXPECT_NE(_stn_left->hash(), _stn_right->hash());
  EXPECT_NE(*_stn_left, *_stn_right);
}

TEST_F(StoredTableColumnAlignmentRuleTest, CoverSubqueries) {

  // Prepare root & subquery LQP
  auto stn_subquery = std::static_pointer_cast<StoredTableNode>(_stn_left->deep_copy());
  auto column_c = lqp_column_(stn_subquery, ColumnID{2});
  auto projection_subquery = ProjectionNode::make(expression_vector(column_c), stn_subquery);
  auto subquery = lqp_subquery_(projection_subquery);

  auto projection_root = ProjectionNode::make(expression_vector(subquery), _union_node);

  // Set pruned column ids
  auto pruned_column_set_a = std::vector{ColumnID{0}};
  _stn_left->set_pruned_column_ids(pruned_column_set_a);
  _stn_right->set_pruned_column_ids(pruned_column_set_a);
  auto pruned_column_set_a_b = std::vector{ColumnID{0}, ColumnID{1}};
  stn_subquery->set_pruned_column_ids(pruned_column_set_a_b);

  // Prerequisites
  ASSERT_EQ(_stn_left->pruned_column_ids(), pruned_column_set_a);
  ASSERT_EQ(_stn_right->pruned_column_ids(), pruned_column_set_a);
  ASSERT_EQ(stn_subquery->pruned_column_ids(), pruned_column_set_a_b); // differs

  apply_rule(_rule, projection_root);

  EXPECT_EQ(_stn_left->pruned_column_ids(), pruned_column_set_a);
  EXPECT_EQ(_stn_right->pruned_column_ids(), pruned_column_set_a);
  EXPECT_EQ(stn_subquery->pruned_column_ids(), pruned_column_set_a);
}

}  // namespace opossum
