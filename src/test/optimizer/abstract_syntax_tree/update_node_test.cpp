#include <memory>
#include <vector>

#include "gtest/gtest.h"

#include "optimizer/abstract_syntax_tree/mock_node.hpp"
#include "optimizer/abstract_syntax_tree/update_node.hpp"
#include "optimizer/expression.hpp"

namespace opossum {

class UpdateNodeTest : public ::testing::Test {};

TEST_F(UpdateNodeTest, MapColumnIDs) {
  /**
   * Test that
   *    - _column_id is mapped
   */

  /**
   *    Update (a=5)
   *        |
   *      Mock
   */
  auto mock = std::make_shared<MockNode>("a", 4);
  auto update = std::make_shared<UpdateNode>(
      "a", std::vector<std::shared_ptr<Expression>>({Expression::create_column(ColumnID{0})}));

  update->set_left_child(mock);

  // Previous order: {a, b, c, d} - New order: {c, a, d, b}
  ColumnIDMapping column_id_mapping({ColumnID{1}, ColumnID{3}, ColumnID{0}, ColumnID{2}});

  update->map_column_ids(column_id_mapping, ASTChildSide::Left);

  EXPECT_EQ(update->column_expressions().at(0)->column_id(), ColumnID{1});
}

}  // namespace opossum
