#pragma once

#include <limits>
#include <memory>

#include "optimizer/abstract_syntax_tree/abstract_ast_node.hpp"
#include "types.hpp"

namespace opossum {

struct JoinPredicate {
  // TODO(moritz) ensure no crosses and naturals here
  // TODO(moritz) Create with constructor
  JoinMode mode;
  std::pair<ColumnID, ColumnID> column_ids;
  ScanType scan_type;
};

struct JoinEdge {
  JoinEdge(const std::pair<JoinVertexID, JoinVertexID>& vertex_indices, JoinMode mode,
           const std::pair<ColumnID, ColumnID>& column_ids, ScanType scan_type);

  JoinPredicate predicate;
  std::pair<JoinVertexID, JoinVertexID> vertex_indices{INVALID_JOIN_VERTEX_ID, INVALID_JOIN_VERTEX_ID};
};

class JoinGraph final {
 public:
  using Vertices = std::vector<std::shared_ptr<AbstractASTNode>>;
  using Edges = std::vector<JoinEdge>;

  static std::shared_ptr<JoinGraph> build_join_graph(const std::shared_ptr<AbstractASTNode>& root);

  JoinGraph() = default;
  JoinGraph(Vertices&& vertices, Edges&& edges);

  const Vertices& vertices() const;
  const Edges& edges() const;

  void print(std::ostream& out = std::cout) const;

 private:
  static void _traverse_ast_for_join_graph(const std::shared_ptr<AbstractASTNode>& node,
                                           JoinGraph::Vertices& o_vertices,
                                           JoinGraph::Edges& o_edges, ColumnID column_id_offset = ColumnID{0});

  Vertices _vertices;
  Edges _edges;
};
}