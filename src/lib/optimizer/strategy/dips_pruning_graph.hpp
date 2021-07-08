#pragma once

#include <set>
#include <vector>

#include "dips_pruning_graph_edge.hpp"
#include "expression/abstract_expression.hpp"
#include "expression/binary_predicate_expression.hpp"
#include "expression/expression_functional.hpp"
#include "hyrise.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/logical_plan_root_node.hpp"
#include "logical_query_plan/lqp_utils.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "resolve_type.hpp"

namespace opossum {

// This graph is a data structure that represents a tree on which the dip algorithm can be executed. The graph can be
// build from an LQP. The nodes of it are representing the join tables and the edges are representing the predicates.

struct DipsPruningGraph {
  friend class DipsPruningGraphTest_BuildJoinGraph_Test;
  friend class DipsPruningGraphTest_JoinGraphIsTree_Test;
  friend class DipsPruningGraphTest_DipsJoinGraphIsNoTree_Test;
  friend class DipsPruningGraphTest_DipsJoinGraphTraversal_Test;

  static constexpr size_t ROOT_VERTEX = 0;

  explicit DipsPruningGraph(std::vector<JoinMode> supported_join_types) : supported_join_types(supported_join_types) {}

  // Traverses the tree via depth-first search and returns the visited edges in top-down order of the graph.
  std::vector<DipsPruningGraphEdge> top_down_traversal();

  // Traverses the tree via depth-first search and returns the visited edges in bottom-up order of the graph.
  std::vector<DipsPruningGraphEdge> bottom_up_traversal();

  bool is_tree();
  bool empty();
  void build_graph(const std::shared_ptr<AbstractLQPNode>& node);

 private:
  // Each table node gets a number assigned. This number is the index of the vertices vector. If the table node exists
  // in the graph its number is returned. If not it will be added to the graph.
  size_t _get_vertex(const std::shared_ptr<StoredTableNode>& table_node);

  // Returns a set of two vertices that can be connected and checks if the vertices exists inside the graph.
  std::set<size_t> _get_vertex_set(const size_t noda_a, const size_t noda_b);

  // Checks if there is already an edge that is connecting the vertices inside the vertex set. If so it will only
  // append the predicate to the edge. If not it will add a new edge to the graph with the predicates.
  void _add_edge(const std::set<size_t>& vertex_set, const std::shared_ptr<BinaryPredicateExpression>& predicate);

  bool _is_tree_visit(const size_t current_node, const size_t parrent, std::set<size_t>& visited);

  void _top_down_traversal_visit(const size_t current_node, std::vector<DipsPruningGraphEdge>& traversal_order,
                                std::set<size_t>& visited);

  void _bottom_up_traversal_visit(const size_t current_node, std::vector<DipsPruningGraphEdge>& traversal_order,
                                  std::set<size_t>& visited);

  std::vector<JoinMode> supported_join_types;
  std::vector<std::shared_ptr<StoredTableNode>> vertices;
  std::vector<DipsPruningGraphEdge> edges;
};

}  // namespace opossum
