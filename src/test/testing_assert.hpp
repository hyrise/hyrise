#pragma once

#include <memory>

#include "gtest/gtest.h"

#include "optimizer/abstract_syntax_tree/abstract_ast_node.hpp"
#include "all_parameter_variant.hpp"
#include "types.hpp"

namespace opossum {

class AbstractASTNode;

class JoinGraph;

class Table;

::testing::AssertionResult check_table_equal(const Table& tleft, const Table& tright, bool order_sensitive,
                                             bool strict_types);

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

bool check_vertex_predicate(const std::shared_ptr<JoinGraph>& join_graph, const std::shared_ptr<AbstractASTNode>& node,
                            ColumnID column_id, ScanType scan_type, const AllParameterVariant & value);

bool check_vertex_nodes(const std::shared_ptr<JoinGraph>& join_graph, const std::vector<std::shared_ptr<AbstractASTNode>> & vertex_nodes);

}  // namespace opossum

#define ASSERT_AST_TIE(parent, child_side, child) \
  if (!opossum::check_ast_tie(parent, child_side, child)) FAIL();

#define EXPECT_JOIN_EDGE(join_graph, node_a, node_b, column_id_a, column_id_b, scan_type) \
  EXPECT_TRUE(opossum::check_join_edge(join_graph, node_a, node_b, column_id_a, column_id_b, scan_type))

#define EXPECT_CROSS_JOIN_EDGE(join_graph, node_a, node_b) \
  EXPECT_TRUE(opossum::check_cross_join_edge(join_graph, node_a, node_b))

#define EXPECT_VERTEX_NODES(join_graph, vertex_nodes) EXPECT_TRUE(check_vertex_nodes(join_graph, vertex_nodes));

#define EXPECT_VERTEX_PREDICATE(join_graph, vertex, column_id, scan_type, value) \
  EXPECT_TRUE(check_vertex_predicate(join_graph, vertex, column_id, scan_type, value))
