#include "base_test.hpp"

#include "logical_query_plan/alter_table_node.hpp"
#include "logical_query_plan/lqp_utils.hpp"
#include "logical_query_plan/mock_node.hpp"
#include "logical_query_plan/static_table_node.hpp"
#include "storage/table.hpp"
#include "storage/table_column_definition.hpp"

namespace opossum {

class AlterTableNodeTest : public BaseTest {
 public:
  void SetUp() override {
    Hyrise::get().storage_manager.add_table(table_name, load_table("resources/test_data/tbl/int_int_float.tbl", 1));

    char column_name[] = {'a','\0'};
    auto alter_table_action = hsql::DropColumnAction(column_name);
    auto drop_column_action = std::make_shared<DropColumnAction>(alter_table_action);

    alter_table_node = AlterTableNode::make(table_name, drop_column_action);
  }

  std::string table_name = "t_a";
  std::shared_ptr<AlterTableNode> alter_table_node;
};

TEST_F(AlterTableNodeTest, Description) {
  EXPECT_EQ(alter_table_node->description(), "[AlterTable] Table: 't_a'; [DropColumn] column: 'a'");

  char column_name[] = {'a','\0'};
  auto alter_table_action = hsql::DropColumnAction(column_name);
  alter_table_action.ifExists = true;
  auto drop_column_action = std::make_shared<DropColumnAction>(alter_table_action);

  auto alter_table_node_2 = AlterTableNode::make(table_name, drop_column_action);
  EXPECT_EQ(alter_table_node_2->description(), "[AlterTable] Table: 't_a'; [DropColumn] (if exists) column: 'a'");
}

TEST_F(AlterTableNodeTest, NodeExpressions) { ASSERT_EQ(alter_table_node->node_expressions.size(), 0u); }

TEST_F(AlterTableNodeTest, HashingAndEqualityCheck) {
  const auto deep_copy_node = alter_table_node->deep_copy();
  EXPECT_EQ(*alter_table_node, *deep_copy_node);

  char column_name[] = {'b','\0'};
  auto alter_table_action = hsql::DropColumnAction(column_name);
  auto drop_column_action = std::make_shared<DropColumnAction>(alter_table_action);
  const auto different_alter_table_node_a = AlterTableNode::make(table_name, drop_column_action);

  char other_column_name[] = {'a','\0'};
  auto other_alter_table_action = hsql::DropColumnAction(other_column_name);
  other_alter_table_action.ifExists = true;
  auto other_drop_column_action = std::make_shared<DropColumnAction>(other_alter_table_action);
  const auto different_alter_table_node_b = AlterTableNode::make(table_name, other_drop_column_action);

  char different_column_name[] = {'a','\0'};
  auto different_alter_table_action = hsql::DropColumnAction(different_column_name);
  auto different_drop_column_action = std::make_shared<DropColumnAction>(different_alter_table_action);
  const auto different_alter_table_node_c = AlterTableNode::make("some_other_table_name", different_drop_column_action);

  EXPECT_NE(*different_alter_table_node_a, *alter_table_node);
  EXPECT_NE(*different_alter_table_node_b, *alter_table_node);
  EXPECT_NE(*different_alter_table_node_c, *alter_table_node);

  EXPECT_NE(different_alter_table_node_a->hash(), alter_table_node->hash());
  EXPECT_NE(different_alter_table_node_b->hash(), alter_table_node->hash());
  EXPECT_NE(different_alter_table_node_c->hash(), alter_table_node->hash());
}

TEST_F(AlterTableNodeTest, Copy) {
  auto copy = alter_table_node->deep_copy();
  EXPECT_EQ(*alter_table_node, *copy);
  EXPECT_EQ(alter_table_node->hash(), copy->hash());
}

}  // namespace opossum
