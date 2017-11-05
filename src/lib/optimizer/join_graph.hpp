#pragma once

#include <limits>
#include <memory>
#include <unordered_set>
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
  JoinEdge(const std::pair<JoinVertexID, JoinVertexID>& vertex_ids, JoinMode join_mode);
  JoinEdge(const std::pair<JoinVertexID, JoinVertexID>& vertex_ids, const std::pair<ColumnID, ColumnID>& column_ids,
           JoinMode join_mode, ScanType scan_type);

  std::pair<JoinVertexID, JoinVertexID> vertex_ids;
  std::optional<std::pair<ColumnID, ColumnID>> column_ids;
  JoinMode join_mode;
  std::optional<ScanType> scan_type;
};

struct JoinVertexPredicate {
  JoinVertexPredicate(ColumnID column_id, ScanType scan_type, const AllParameterVariant& value);

  ColumnID column_id;
  ScanType scan_type;
  AllParameterVariant value;
};

struct JoinVertex {
  explicit JoinVertex(const std::shared_ptr<AbstractASTNode>& node);

  std::shared_ptr<AbstractASTNode> node;
  std::vector<JoinVertexPredicate> predicates;
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
  using Vertices = std::vector<JoinVertex>;
  using Edges = std::vector<JoinEdge>;


  JoinGraph() = default;
  JoinGraph(Vertices&& vertices, Edges&& edges);

  const Vertices& vertices() const;
  const Edges& edges() const;

  void print(std::ostream& out = std::cout) const;

 private:
  Vertices _vertices;
  Edges _edges;
};
}  // namespace opossum
