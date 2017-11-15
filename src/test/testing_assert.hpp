#pragma once

#include <memory>

#include "gtest/gtest.h"

#include "optimizer/abstract_syntax_tree/abstract_ast_node.hpp"
#include "types.hpp"

namespace opossum {

class AbstractLogicalPlanNode;
class Table;

::testing::AssertionResult check_table_equal(const Table& tleft, const Table& tright, bool order_sensitive,
                                             bool strict_types);

void EXPECT_TABLE_EQ(const Table& tleft, const Table& tright, bool order_sensitive = false, bool strict_types = true);
void ASSERT_TABLE_EQ(const Table& tleft, const Table& tright, bool order_sensitive = false, bool strict_types = true);

void EXPECT_TABLE_EQ(std::shared_ptr<const Table> tleft, std::shared_ptr<const Table> tright,
                     bool order_sensitive = false, bool strict_types = true);
void ASSERT_TABLE_EQ(std::shared_ptr<const Table> tleft, std::shared_ptr<const Table> tright,
                     bool order_sensitive = false, bool strict_types = true);

void ASSERT_INNER_JOIN_NODE(const std::shared_ptr<AbstractLogicalPlanNode>& node, ScanType scanType, ColumnID left_column_id,
                            ColumnID right_column_id);

void ASSERT_CROSS_JOIN_NODE(const std::shared_ptr<AbstractLogicalPlanNode>& node);

bool check_ast_tie(const std::shared_ptr<const AbstractLogicalPlanNode>& parent, LQPChildSide child_side,
                   const std::shared_ptr<const AbstractLogicalPlanNode>& child);
}  // namespace opossum

#define ASSERT_AST_TIE(parent, child_side, child) \
  if (!opossum::check_ast_tie(parent, child_side, child)) FAIL();
