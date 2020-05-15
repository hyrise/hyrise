#include <vector>

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

    _stored_table_node_left = StoredTableNode::make("t_a");
    _stored_table_node_right = StoredTableNode::make("t_a");

    _stored_table_node_left->set_pruned_chunk_ids({ChunkID{2}});
    _stored_table_node_right->set_pruned_chunk_ids({ChunkID{2}});

    _stored_table_node_left->set_pruned_column_ids({ColumnID{0}});
    _stored_table_node_right->set_pruned_column_ids({ColumnID{0}});

    _union_node = UnionNode::make(SetOperationMode::All);
    _union_node->set_left_input(_stored_table_node_left);
    _union_node->set_right_input(_stored_table_node_right);

    _rule = std::make_shared<StoredTableColumnAlignmentRule>();
  }

  std::shared_ptr<StoredTableColumnAlignmentRule> _rule;
  std::shared_ptr<UnionNode> _union_node;
  std::shared_ptr<StoredTableNode> _stored_table_node_left, _stored_table_node_right;
};

TEST_F(StoredTableColumnAlignmentRuleTest, EqualTableEqualChunksEqualColumns) {
  apply_rule(_rule, _union_node);

  EXPECT_EQ(_stored_table_node_left->pruned_column_ids(), (std::vector{ColumnID{0}}));
  EXPECT_EQ(_stored_table_node_right->pruned_column_ids(), (std::vector{ColumnID{0}}));
  EXPECT_EQ(_stored_table_node_left->hash(), _stored_table_node_right->hash());
  EXPECT_EQ(*_stored_table_node_left, *_stored_table_node_right);
}

TEST_F(StoredTableColumnAlignmentRuleTest, EqualTableEqualChunksDifferentColumns) {
  _stored_table_node_right->set_pruned_column_ids({ColumnID{0}, ColumnID{1}});
  _union_node->set_right_input(_stored_table_node_right);

  apply_rule(_rule, _union_node);

  EXPECT_EQ(_stored_table_node_left->pruned_column_ids(), (std::vector{ColumnID{0}}));
  EXPECT_EQ(_stored_table_node_right->pruned_column_ids(), (std::vector{ColumnID{0}}));
  EXPECT_EQ(_stored_table_node_left->hash(), _stored_table_node_right->hash());
  EXPECT_EQ(*_stored_table_node_left, *_stored_table_node_right);
}

TEST_F(StoredTableColumnAlignmentRuleTest, EqualTableDifferentChunksDifferentColumns) {
  _stored_table_node_right->set_pruned_chunk_ids({ChunkID{2}, ChunkID{3}});
  _stored_table_node_right->set_pruned_column_ids({ColumnID{0}, ColumnID{1}});
  _union_node->set_right_input(_stored_table_node_right);

  apply_rule(_rule, _union_node);

  EXPECT_EQ(_stored_table_node_left->pruned_column_ids(), (std::vector{ColumnID{0}}));
  EXPECT_EQ(_stored_table_node_right->pruned_column_ids(), (std::vector{ColumnID{0}, ColumnID{1}}));
  EXPECT_NE(_stored_table_node_left->hash(), _stored_table_node_right->hash());
  EXPECT_NE(*_stored_table_node_left, *_stored_table_node_right);
}

TEST_F(StoredTableColumnAlignmentRuleTest, DifferentTableEqualChunksDifferentColumns) {
  _stored_table_node_right = StoredTableNode::make("t_b");
  _stored_table_node_right->set_pruned_chunk_ids({ChunkID{2}});
  _stored_table_node_right->set_pruned_column_ids({ColumnID{0}, ColumnID{1}});
  _union_node->set_right_input(_stored_table_node_right);

  apply_rule(_rule, _union_node);

  EXPECT_EQ(_stored_table_node_left->pruned_column_ids(), (std::vector{ColumnID{0}}));
  EXPECT_EQ(_stored_table_node_right->pruned_column_ids(), (std::vector{ColumnID{0}, ColumnID{1}}));
  EXPECT_NE(_stored_table_node_left->hash(), _stored_table_node_right->hash());
  EXPECT_NE(*_stored_table_node_left, *_stored_table_node_right);
}

}  // namespace opossum
