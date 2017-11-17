#pragma once

#include <memory>

#include "gtest/gtest.h"

#include "logical_query_plan/abstract_lqp_node.hpp"
#include "types.hpp"

namespace opossum {

class AbstractLQPNode;
class Table;

::testing::AssertionResult check_table_equal(const Table& tleft, const Table& tright, bool order_sensitive,
                                             bool strict_types);

void EXPECT_TABLE_EQ(const Table& tleft, const Table& tright, bool order_sensitive = false, bool strict_types = true);
void ASSERT_TABLE_EQ(const Table& tleft, const Table& tright, bool order_sensitive = false, bool strict_types = true);

void EXPECT_TABLE_EQ(std::shared_ptr<const Table> tleft, std::shared_ptr<const Table> tright,
                     bool order_sensitive = false, bool strict_types = true);
void ASSERT_TABLE_EQ(std::shared_ptr<const Table> tleft, std::shared_ptr<const Table> tright,
                     bool order_sensitive = false, bool strict_types = true);

void ASSERT_INNER_JOIN_NODE(const std::shared_ptr<AbstractLQPNode>& node, ScanType scanType,
                            ColumnID left_column_id, ColumnID right_column_id);

void ASSERT_CROSS_JOIN_NODE(const std::shared_ptr<AbstractLQPNode>& node);

bool check_lqp_tie(const std::shared_ptr<const AbstractLQPNode>& parent, LQPChildSide child_side,
                   const std::shared_ptr<const AbstractLQPNode>& child);
}  // namespace opossum

#define ASSERT_LQP_TIE(parent, child_side, child) \
  if (!opossum::check_lqp_tie(parent, child_side, child)) FAIL();
