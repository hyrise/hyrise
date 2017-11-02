#pragma once

#include <limits>
#include <memory>
#include <utility>
#include <vector>

#include "optimizer/abstract_syntax_tree/abstract_ast_node.hpp"
#include "optimizer/abstract_syntax_tree/predicate_node.hpp"
#include "types.hpp"

namespace opossum {

class JoinNode;
class PredcicateNode;

/**
 * A connection between two JoinGraph-Vertices.
 */
struct JoinEdge {
  JoinEdge(const std::pair<JoinVertexID, JoinVertexID>& vertex_indices, JoinMode join_mode);
  JoinEdge(const std::pair<JoinVertexID, JoinVertexID>& vertex_indices, const std::pair<ColumnID, ColumnID>& column_ids,
           JoinMode join_mode, ScanType scan_type);

  std::pair<JoinVertexID, JoinVertexID> vertex_indices;
  std::optional<std::pair<ColumnID, ColumnID>> column_ids;
  JoinMode join_mode;
  std::optional<ScanType> scan_type;
};

/**
 * Describes a set of AST subtrees (called "vertices") and the predicates (called "edges") they are connected with.
 * JoinGraphs are the core data structure worked on during JoinOrdering.
 * A JoinGraph is a unordered representation of a JoinPlan, i.e. a AST subtree that consists of Joins,
 * ColumnToColumn-Predicates and Leafs (which are all other kind of nodes).
 *
 * See the tests for examples.
 */
class JoinGraph final {
 public:
  using Vertices = std::vector<std::shared_ptr<AbstractASTNode>>;
  using Edges = std::vector<JoinEdge>;

  /**
   * From the subtree of root, build a join graph.
   * The AST is not modified during this process.
   */
  static std::shared_ptr<JoinGraph> build_join_graph(const std::shared_ptr<AbstractASTNode>& root);

  JoinGraph() = default;
  JoinGraph(Vertices&& vertices, Edges&& edges);

  const Vertices& vertices() const;
  const Edges& edges() const;

  void print(std::ostream& out = std::cout) const;

 private:
  /**
   * Helper method/actual implementation for build_join_graph().
   * @param node                The subtree to investigate
   * @param o_vertices          Output parameter, collecting all the vertex-AST-nodes
   * @param o_edges             Output parameter, collecting all edges/predicates in the tree
   */
  static void _traverse_ast_for_join_graph(const std::shared_ptr<AbstractASTNode>& node,
                                           JoinGraph::Vertices& o_vertices, JoinGraph::Edges& o_edges);
  static void _traverse_inner_join_node(const std::shared_ptr<JoinNode>& node, JoinGraph::Vertices& o_vertices,
                                        JoinGraph::Edges& o_edges);
  static void _traverse_cross_join_node(const std::shared_ptr<JoinNode>& node, JoinGraph::Vertices& o_vertices,
                                        JoinGraph::Edges& o_edges);
  static void _traverse_predicate_node(const std::shared_ptr<PredicateNode>& node, JoinGraph::Vertices& o_vertices,
                                       JoinGraph::Edges& o_edges);

  /**
   * Within the index range [vertex_range_begin, vertex_range_end) in vertices, look for the `column_id`th column and
   * return the index of the Vertex it belongs to, as well as the ColumnID in that vertex
   */
  static std::pair<JoinVertexID, ColumnID> _find_vertex_and_column_id(
      const std::vector<std::shared_ptr<AbstractASTNode>>& vertices, ColumnID column_id,
      JoinVertexID vertex_range_begin, JoinVertexID vertex_range_end);

  Vertices _vertices;
  Edges _edges;
};
}  // namespace opossum
