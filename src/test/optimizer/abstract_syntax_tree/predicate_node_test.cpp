#include <memory>

#include "gtest/gtest.h"

#include "optimizer/abstract_syntax_tree/mock_table_node.hpp"
#include "optimizer/abstract_syntax_tree/predicate_node.hpp"

namespace opossum {

class PredicateNodeTest : public ::testing::Test {};

TEST_F(PredicateNodeTest, MapColumnIDs) {
  /**
   * Test that
   *    - _column_id is mapped
   *    - _value, if it is a ColumnID, is mapped
   *    - parent nodes are mapped
   */

  /**
   * Predicate_1 (a > b)
   *        |
   * Predicate_0 (d < 3)
   *        |
   *      Mock
   */
  auto mock = std::make_shared<MockTableNode>("a", 4);
  auto predicate_0 = std::make_shared<PredicateNode>(ColumnID{3}, ScanType::OpLessThan, 3);
  auto predicate_1 = std::make_shared<PredicateNode>(ColumnID{0}, ScanType::OpGreaterThan, ColumnID{1});

  predicate_1->set_left_child(predicate_0);
  predicate_0->set_left_child(mock);

  // Previous order: {a, b, c, d} - New order: {c, a, d, b}
  ColumnIDMapping column_id_mapping({ColumnID{1}, ColumnID{3}, ColumnID{0}, ColumnID{2}});

  mock->map_column_ids(column_id_mapping);

  EXPECT_EQ(predicate_1->column_id(), ColumnID{1});
  EXPECT_EQ(predicate_0->column_id(), ColumnID{2});
  EXPECT_EQ(boost::get<ColumnID>(predicate_1->value()), ColumnID{3});
}

}  // namespace opossum
