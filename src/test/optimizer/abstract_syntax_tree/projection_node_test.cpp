#include <memory>
#include <vector>

#include "gtest/gtest.h"

#include "base_test.hpp"

#include "optimizer/abstract_syntax_tree/projection_node.hpp"
#include "optimizer/abstract_syntax_tree/stored_table_node.hpp"
#include "optimizer/expression.hpp"
#include "storage/storage_manager.hpp"

namespace opossum {

class ProjectionNodeTest : public BaseTest {
 protected:
  void SetUp() override {
    StorageManager::get().add_table("t_a", load_table("src/test/tables/int_int_int.tbl", 0));

    _stored_table_node = std::make_shared<StoredTableNode>("t_a");

    // SELECT c, a, b AS alias_for_b, b+c AS some_addition, a+c [...]
    _projection_node = std::make_shared<ProjectionNode>(std::vector<std::shared_ptr<Expression>>{
        Expression::create_column(ColumnID{2}), Expression::create_column(ColumnID{0}),
        Expression::create_column(ColumnID{1}, {"alias_for_b"}),
        Expression::create_binary_operator(ExpressionType::Addition, Expression::create_column(ColumnID{1}),
                                           Expression::create_column(ColumnID{2}), {"some_addition"}),
        Expression::create_binary_operator(ExpressionType::Addition, Expression::create_column(ColumnID{0}),
                                           Expression::create_column(ColumnID{2}))});
    _projection_node->set_left_child(_stored_table_node);
  }

  void TearDown() override { StorageManager::get().reset(); }

  std::shared_ptr<StoredTableNode> _stored_table_node;
  std::shared_ptr<ProjectionNode> _projection_node;
};

TEST_F(ProjectionNodeTest, ColumnIdForColumnIdentifier) {
  EXPECT_EQ(_projection_node->get_column_id_by_named_column_reference({"c", nullopt}), 0);
  EXPECT_EQ(_projection_node->get_column_id_by_named_column_reference({"c", {"t_a"}}), 0);
  EXPECT_EQ(_projection_node->get_column_id_by_named_column_reference({"a", nullopt}), 1);
  EXPECT_EQ(_projection_node->find_column_id_by_named_column_reference({"b", nullopt}), nullopt);
  EXPECT_EQ(_projection_node->find_column_id_by_named_column_reference({"b", {"t_a"}}), nullopt);
  EXPECT_EQ(_projection_node->get_column_id_by_named_column_reference({"alias_for_b", nullopt}), 2);
  EXPECT_EQ(_projection_node->find_column_id_by_named_column_reference({"alias_for_b", {"t_a"}}), nullopt);
  EXPECT_EQ(_projection_node->get_column_id_by_named_column_reference({"some_addition", nullopt}), 3);
  EXPECT_EQ(_projection_node->find_column_id_by_named_column_reference({"some_addition", {"t_a"}}), nullopt);
  EXPECT_EQ(_projection_node->find_column_id_by_named_column_reference({"some_addition", {"t_b"}}), nullopt);
  EXPECT_EQ(_projection_node->get_column_id_by_named_column_reference({"a + c", nullopt}), 4);
}

}  // namespace opossum
