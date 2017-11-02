#pragma once

#include <memory>

#include "gtest/gtest.h"

#include "all_parameter_variant.hpp"
#include "all_type_variant.hpp"
#include "optimizer/abstract_syntax_tree/abstract_ast_node.hpp"
#include "optimizer/abstract_syntax_tree/ast_utils.hpp"
#include "types.hpp"

namespace opossum {

class AbstractASTNode;
class JoinGraph;
class Table;

::testing::AssertionResult check_table_equal(const Table& tleft, const Table& tright, bool order_sensitive,
                                             bool strict_types);

::testing::AssertionResult check_predicate_node(const std::shared_ptr<AbstractASTNode>& node, ColumnID column_id,
                                                ScanType scan_type, const AllParameterVariant& value,
                                                const std::optional<AllTypeVariant>& value2 = std::nullopt);

void EXPECT_TABLE_EQ(const Table& tleft, const Table& tright, bool order_sensitive = false, bool strict_types = true);

void ASSERT_TABLE_EQ(const Table& tleft, const Table& tright, bool order_sensitive = false, bool strict_types = true);

void EXPECT_TABLE_EQ(std::shared_ptr<const Table> tleft, std::shared_ptr<const Table> tright,
                     bool order_sensitive = false, bool strict_types = true);

void ASSERT_TABLE_EQ(std::shared_ptr<const Table> tleft, std::shared_ptr<const Table> tright,
                     bool order_sensitive = false, bool strict_types = true);

void ASSERT_INNER_JOIN_NODE(const std::shared_ptr<AbstractASTNode>& node, ScanType scanType, ColumnID left_column_id,
                            ColumnID right_column_id);

void ASSERT_CROSS_JOIN_NODE(const std::shared_ptr<AbstractASTNode>& node);

bool check_ast_tie(const std::shared_ptr<const AbstractASTNode>& parent, ASTChildSide child_side,
                   const std::shared_ptr<const AbstractASTNode>& child);

bool check_join_edge(const std::shared_ptr<JoinGraph>& join_graph, const std::shared_ptr<AbstractASTNode>& node_a,
                     const std::shared_ptr<AbstractASTNode>& node_b, ColumnID column_id_a, ColumnID column_id_b,
                     ScanType scan_type);

bool check_cross_join_edge(const std::shared_ptr<JoinGraph>& join_graph, const std::shared_ptr<AbstractASTNode>& node_a,
                           const std::shared_ptr<AbstractASTNode>& node_b);

/**
 * Check whether the join plan (i.e. the subtree rooted by `node`) fulfills an edge, either via an Inner Join or a
 * Predicate
 */
bool check_contains_join_edge(const std::shared_ptr<AbstractASTNode>& node,
                              const std::shared_ptr<AbstractASTNode>& leaf_a,
                              const std::shared_ptr<AbstractASTNode>& leaf_b, ColumnID column_id_a,
                              ColumnID column_id_b, ScanType scan_type);
}  // namespace opossum

#define ASSERT_AST_TIE(parent, child_side, child) \
  if (!opossum::check_ast_tie(parent, child_side, child)) FAIL();

#define EXPECT_JOIN_EDGE(join_graph, node_a, node_b, column_id_a, column_id_b, scan_type) \
  EXPECT_TRUE(opossum::check_join_edge(join_graph, node_a, node_b, column_id_a, column_id_b, scan_type))

#define EXPECT_CROSS_JOIN_EDGE(join_graph, node_a, node_b) \
  EXPECT_TRUE(opossum::check_cross_join_edge(join_graph, node_a, node_b))

#define EXPECT_JOIN_VERTICES(vertices_a, vertices_b) EXPECT_EQ(vertices_a, vertices_b)

#define EXPECT_PREDICATE_NODE(node, column_id, scan_type, value) \
  EXPECT_TRUE(check_predicate_node(node, column_id, scan_type, value))

#define EXPECT_AST_CONTAINS_JOIN_EDGE(node, leaf_a, leaf_b, column_id_a, column_id_b, scan_type) \
  EXPECT_TRUE(ast_contains_join_edge(node, leaf_a, leaf_b, column_id_a, column_id_b, scan_type))
