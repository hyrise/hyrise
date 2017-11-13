#include <memory>

#include "gtest/gtest.h"

#include "base_test.hpp"

#include "optimizer/abstract_syntax_tree/stored_table_node.hpp"
#include "optimizer/abstract_syntax_tree/union_node.hpp"

namespace opossum {

class UnionNodeTest : public BaseTest {
 protected:
  void SetUp() override {
    StorageManager::get().add_table("table_a", load_table("src/test/tables/int_int.tbl", 2));
    StorageManager::get().add_table("table_b", load_table("src/test/tables/int_float.tbl", 2));

    _table_node_a = std::make_shared<StoredTableNode>("table_a");
    _table_node_b = std::make_shared<StoredTableNode>("table_b");

    _union_node = std::make_shared<UnionNode>(UnionMode::Positions);
    _union_node->set_left_child(_table_node_a);
    _union_node->set_right_child(_table_node_b);
  }

  std::shared_ptr<StoredTableNode> _table_node_a, _table_node_b;
  std::shared_ptr<UnionNode> _union_node;
};

TEST_F(UnionNodeTest, Description) { EXPECT_EQ(_union_node->description(), "[UnionNode] Mode: UnionPositions"); }

TEST_F(UnionNodeTest, StatisticsNotImplemented) {
  EXPECT_THROW(_union_node->derive_statistics_from(_table_node_a, _table_node_b), std::exception);
}

TEST_F(UnionNodeTest, NoInputTableColumn) {
  EXPECT_THROW(_union_node->get_output_column_ids_for_table("table_a"), std::exception);
}

TEST_F(UnionNodeTest, OutputColumnMapping) {
  auto column_ids = _union_node->output_column_ids_to_input_column_ids();

  EXPECT_EQ(column_ids.size(), 2u);
  EXPECT_EQ(column_ids[0], ColumnID{0});
  EXPECT_EQ(column_ids[1], ColumnID{1});
}

TEST_F(UnionNodeTest, MismatchingColumnNames) {
  StorageManager::get().add_table("table_c", load_table("src/test/tables/string_int.tbl", 2));

  auto table_node_c = std::make_shared<StoredTableNode>("table_c");
  auto invalid_union = std::make_shared<UnionNode>(UnionMode::Positions);
  invalid_union->set_left_child(_table_node_a);
  invalid_union->set_right_child(table_node_c);

  EXPECT_THROW(invalid_union->get_verbose_column_name(ColumnID{0}), std::exception);
}

TEST_F(UnionNodeTest, KnowsNoTables) {
  EXPECT_FALSE(_union_node->knows_table("table_a"));
  EXPECT_FALSE(_union_node->knows_table("table_b"));
  EXPECT_FALSE(_union_node->knows_table("any_table"));
}

TEST_F(UnionNodeTest, DISABLED_VerboseColumnNames) {
  /* 
   * Currently the verbose column names of the inputs need to be *exactly* the same.
   * That is why we need to use the same input table for left AND right input.
   * This is probably a logical error in `UnionNode::get_verbose_column_name`.
   */

  auto verbose_union = std::make_shared<UnionNode>(UnionMode::Positions);
  verbose_union->set_left_child(_table_node_a);
  verbose_union->set_right_child(_table_node_b);

  EXPECT_EQ(verbose_union->get_verbose_column_name(ColumnID{0}), "a");
  EXPECT_EQ(verbose_union->get_verbose_column_name(ColumnID{1}), "b");
}

TEST_F(UnionNodeTest, DISABLED_FindingColumnReferences) {
  /*
   * Resolving names does not seem to work properly. On one hand, it is required to have two input tables
   * with exactly the same name and alias for `get_verbose_column_name` to work, but on the other hand,
   * `find_column_id_by_named_column_reference` requires column names to be different on each side.
   */

  // finding column named "a"
  EXPECT_EQ(_union_node->get_column_id_by_named_column_reference({"a", std::nullopt}), ColumnID{0});
  EXPECT_EQ(_union_node->get_column_id_by_named_column_reference({"a", {"table_a"}}), ColumnID{0});
  EXPECT_EQ(_union_node->get_column_id_by_named_column_reference({"a", {"table_b"}}), ColumnID{0});

  // finding column named "b"
  EXPECT_EQ(_union_node->get_column_id_by_named_column_reference({"b", std::nullopt}), ColumnID{1});
  EXPECT_EQ(_union_node->get_column_id_by_named_column_reference({"b", {"table_a"}}), ColumnID{1});
  EXPECT_EQ(_union_node->get_column_id_by_named_column_reference({"b", {"table_b"}}), ColumnID{1});

  // invalid names
  EXPECT_EQ(_union_node->find_column_id_by_named_column_reference({"c", std::nullopt}), std::nullopt);
  EXPECT_EQ(_union_node->find_column_id_by_named_column_reference({"c", {"table_a"}}), std::nullopt);
  EXPECT_EQ(_union_node->find_column_id_by_named_column_reference({"c", {"table_b"}}), std::nullopt);
}

}  // namespace opossum
