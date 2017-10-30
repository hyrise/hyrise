#include <memory>
#include <utility>

#include "gtest/gtest.h"

#include "base_test.hpp"

#include "optimizer/abstract_syntax_tree/join_node.hpp"
#include "optimizer/abstract_syntax_tree/mock_table_node.hpp"
#include "optimizer/abstract_syntax_tree/predicate_node.hpp"
#include "optimizer/abstract_syntax_tree/stored_table_node.hpp"
#include "storage/storage_manager.hpp"

namespace opossum {

class JoinNodeTest : public BaseTest {
 protected:
  void SetUp() override {
    StorageManager::get().add_table("t_a", load_table("src/test/tables/int_int_int.tbl", 0));
    StorageManager::get().add_table("t_b", load_table("src/test/tables/int_float_alt_column_names.tbl", 0));

    _stored_table_node_a = std::make_shared<StoredTableNode>("t_a");
    _stored_table_node_b = std::make_shared<StoredTableNode>("t_b");

    _join_node = std::make_shared<JoinNode>(JoinMode::Cross);
    _join_node->set_left_child(_stored_table_node_a);
    _join_node->set_right_child(_stored_table_node_b);
  }

  void TearDown() override { StorageManager::get().reset(); }

  std::shared_ptr<StoredTableNode> _stored_table_node_a;
  std::shared_ptr<StoredTableNode> _stored_table_node_b;
  std::shared_ptr<JoinNode> _join_node;
};

TEST_F(JoinNodeTest, ColumnIdForColumnIdentifier) {
  EXPECT_EQ(_join_node->get_column_id_by_named_column_reference({"a", std::nullopt}), 0);
  EXPECT_EQ(_join_node->get_column_id_by_named_column_reference({"a", {"t_a"}}), 0);
  EXPECT_EQ(_join_node->find_column_id_by_named_column_reference({"x", {"t_a"}}), std::nullopt);
  EXPECT_EQ(_join_node->get_column_id_by_named_column_reference({"x", {"t_b"}}), 3);
  EXPECT_EQ(_join_node->find_column_id_by_named_column_reference({"x", {"t_a"}}), std::nullopt);
  EXPECT_EQ(_join_node->find_column_id_by_named_column_reference({"z", std::nullopt}), std::nullopt);
  EXPECT_EQ(_join_node->find_column_id_by_named_column_reference({"z", {"t_z"}}), std::nullopt);
}

TEST_F(JoinNodeTest, AliasedSubqueryTest) {
  const auto join_node_with_alias = std::make_shared<JoinNode>(*_join_node);
  join_node_with_alias->set_alias({"foo"});

  EXPECT_TRUE(join_node_with_alias->knows_table("foo"));
  EXPECT_FALSE(join_node_with_alias->knows_table("t_a"));
  EXPECT_FALSE(join_node_with_alias->knows_table("t_b"));

  EXPECT_EQ(join_node_with_alias->get_column_id_by_named_column_reference({"a"}), ColumnID{0});
  EXPECT_EQ(join_node_with_alias->get_column_id_by_named_column_reference({"a", {"foo"}}), ColumnID{0});
  EXPECT_EQ(join_node_with_alias->find_column_id_by_named_column_reference({"a", {"t_a"}}), std::nullopt);
  EXPECT_EQ(join_node_with_alias->find_column_id_by_named_column_reference({"a", {"t_b"}}), std::nullopt);
  EXPECT_EQ(join_node_with_alias->get_column_id_by_named_column_reference({"x", {"foo"}}), ColumnID{3});
}

TEST_F(JoinNodeTest, MapColumnIDs) {
  /**
   * Test that changing the column order in the left AS WELL AS the right child propagates correctly to the
   * JoinColumnIds and the parent node
   */

  /**
   *    Predicate(MockA.b = MockB.c)
   *                |
   *     Join(MockA.a = MockB.b)
   *    /                      \
   *  MockA                     MockB
   */

  auto mock_a = std::make_shared<MockTableNode>("a", 3);
  auto mock_b = std::make_shared<MockTableNode>("b", 4);
  auto join = std::make_shared<JoinNode>(JoinMode::Inner, std::make_pair(ColumnID{0}, ColumnID{1}), ScanType::OpEquals);
  auto predicate = std::make_shared<PredicateNode>(ColumnID{1}, ScanType::OpEquals, ColumnID{5});

  join->set_children(mock_a, mock_b);
  predicate->set_left_child(join);

  // MockA previous order: {a,b,c} - new order: {c,a,b}
  auto column_id_mapping_a = ColumnIDMapping({ColumnID{1}, ColumnID{2}, ColumnID{0}});
  join->map_column_ids(column_id_mapping_a, ASTChildSide::Left);

  // MockB previous order: {a,b,c,d} - new order: {d,c,b,a}
  auto column_id_mapping_b = ColumnIDMapping({ColumnID{3}, ColumnID{2}, ColumnID{1}, ColumnID{0}});
  join->map_column_ids(column_id_mapping_b, ASTChildSide::Right);

  EXPECT_EQ(join->join_column_ids()->first, ColumnID{1});
  EXPECT_EQ(join->join_column_ids()->second, ColumnID{2});
  EXPECT_EQ(predicate->column_id(), ColumnID{2});
  EXPECT_EQ(boost::get<ColumnID>(predicate->value()), ColumnID{4});
}
}  // namespace opossum
