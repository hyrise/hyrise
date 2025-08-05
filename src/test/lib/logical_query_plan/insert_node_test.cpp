#include "base_test.hpp"
#include "hyrise.hpp"
#include "logical_query_plan/insert_node.hpp"
#include "logical_query_plan/lqp_utils.hpp"
#include "storage/table.hpp"

namespace hyrise {

class InsertNodeTest : public BaseTest {
 protected:
  void SetUp() override {
    const auto dummy_table = Table::create_dummy_table({{"a", DataType::Int, false}});
    const auto table_id = Hyrise::get().catalog.add_table("table_a", dummy_table);
    _insert_node = InsertNode::make(table_id);
  }

  std::shared_ptr<InsertNode> _insert_node;
};

TEST_F(InsertNodeTest, Description) {
  EXPECT_EQ(_insert_node->description(), "[Insert] Into table 'table_a'");
}

TEST_F(InsertNodeTest, TableID) {
  EXPECT_EQ(_insert_node->table_id, Hyrise::get().catalog.table_id("table_a"));
}

TEST_F(InsertNodeTest, HashingAndEqualityCheck) {
  EXPECT_EQ(*_insert_node, *_insert_node);
  EXPECT_EQ(*_insert_node, *InsertNode::make(Hyrise::get().catalog.table_id("table_a")));
  EXPECT_NE(*_insert_node, *InsertNode::make(ObjectID{17}));

  EXPECT_EQ(_insert_node->hash(), InsertNode::make(Hyrise::get().catalog.table_id("table_a"))->hash());
}

TEST_F(InsertNodeTest, NodeExpressions) {
  EXPECT_TRUE(_insert_node->node_expressions.empty());
}

TEST_F(InsertNodeTest, ColumnExpressions) {
  EXPECT_TRUE(_insert_node->output_expressions().empty());
}

TEST_F(InsertNodeTest, NoUniqueColumnCombinations) {
  EXPECT_THROW(_insert_node->unique_column_combinations(), std::logic_error);
}

TEST_F(InsertNodeTest, NoOrderDependencies) {
  EXPECT_THROW(_insert_node->order_dependencies(), std::logic_error);
}

}  // namespace hyrise
