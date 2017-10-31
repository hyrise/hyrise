#include <memory>
#include <vector>

#include "gtest/gtest.h"

#include "optimizer/abstract_syntax_tree/mock_table_node.hpp"
#include "optimizer/abstract_syntax_tree/sort_node.hpp"

namespace opossum {

class SortNodeTest : public ::testing::Test {};

TEST_F(SortNodeTest, MapColumnIDs) {
  /**
   * Test that
   *    - column_ids are mapped
   *    - parent nodes are mapped
   */

  /**
   * Sort_1 (a ASC, c DESC)
   *        |
   * Sort_0 (b DESC)
   *        |
   *      Mock
   */
  auto mock = std::make_shared<MockTableNode>("a", 4);
  auto sort_1 = std::make_shared<SortNode>(
      OrderByDefinitions{{ColumnID{0}, OrderByMode::Ascending}, {ColumnID{2}, OrderByMode::Descending}});
  auto sort_0 = std::make_shared<SortNode>(OrderByDefinitions{{ColumnID{1}, OrderByMode::Descending}});

  sort_1->set_left_child(sort_0);
  sort_0->set_left_child(mock);

  // Previous order: {a, b, c, d} - New order: {c, a, d, b}
  ColumnIDMapping column_id_mapping({ColumnID{1}, ColumnID{3}, ColumnID{0}, ColumnID{2}});

  sort_0->map_column_ids(column_id_mapping, ASTChildSide::Left);

  EXPECT_EQ(sort_1->order_by_definitions().at(0).column_id, ColumnID{1});
  EXPECT_EQ(sort_1->order_by_definitions().at(1).column_id, ColumnID{0});
  EXPECT_EQ(sort_0->order_by_definitions().at(0).column_id, ColumnID{3});
}

}  // namespace opossum
