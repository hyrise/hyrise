#pragma once

#include <map>
#include <memory>
#include <optional>
#include <set>
#include <vector>

#include "join_graph.hpp"
#include "logical_query_plan/union_node.hpp"

namespace opossum {

class AbstractLQPNode;

/**
 * Turns an LQP into a JoinGraph.
 *
 * Consider this simple LQP:
 * [0] [Projection] x1, y1
 *  \_[1] [Predicate] x2 <= y1
 *     \_[2] [Cross Join]
 *        \_[3] [MockTable] x
 *        \_[4] [MockTable] y
 *
 * The JoinGraph created from it would contain two vertices (x and y), one edge (between x and y, with the predicate
 * x2 <= y1) and one output_relation (Projection, left input side)
 */
class JoinGraphBuilder final {
 public:
  /**
   * From an LQP, build a JoinGraph. The LQP is not modified during this process.
   * std::nullopt is returned if the JoinGraph would be trivial (i.e., one vertex, no predicates)
   */
  std::optional<JoinGraph> operator()(const std::shared_ptr<AbstractLQPNode>& lqp);

 private:
  static std::vector<JoinGraphEdge> _join_edges_from_predicates(
      const std::vector<std::shared_ptr<AbstractLQPNode>>& vertices,
      const std::vector<std::shared_ptr<AbstractExpression>>& predicates);

  static std::vector<JoinGraphEdge> _cross_edges_between_components(
      const std::vector<std::shared_ptr<AbstractLQPNode>>& vertices, std::vector<JoinGraphEdge> edges);

  /**
   * Traverse the LQP recursively identifying predicates and vertices along the way
   */
  void _traverse(const std::shared_ptr<AbstractLQPNode>& node);
  /**
   * A subgraph in the LQP consisting of PredicateNodes can be translated into a single complex predicate
   *
   *                  PredicateNode(a < 3)
   *                           |
   *                  PredicateNode(b > 12)
   *                           |
   *                    AggregateNode
   *                           |
   *                         [...]
   *
   *  Represents the JoinPlanPredicate "a < 3 AND b > 12". The AggregateNode is the Predicate's "base_node"
   *  i.e. the node on which's output the Predicate operates on.
   *
   *  _parse_predicate() performs this conversion.
   */
  struct PredicateParseResult {
    PredicateParseResult(const std::shared_ptr<AbstractLQPNode>& base_node,
                         const std::shared_ptr<AbstractExpression>& predicate)
        : base_node(base_node), predicate(predicate) {}

    std::shared_ptr<AbstractLQPNode> base_node;
    std::shared_ptr<AbstractExpression> predicate;
  };

  PredicateParseResult _parse_predicate(const std::shared_ptr<AbstractLQPNode>& node) const;

  /**
   * Returns whether a node of the given type is a JoinGraph vertex in all cases. This is true for all node types that
   * aren't Predicates, Joins or Unions.
   */
  bool _lqp_node_type_is_vertex(const LQPNodeType node_type) const;

  /**
   * Lookup which vertices an expression references
   */
  static JoinGraphVertexSet _get_vertex_set_accessed_by_expression(
      const AbstractExpression& expression, const std::vector<std::shared_ptr<AbstractLQPNode>>& vertices);

  std::vector<std::shared_ptr<AbstractLQPNode>> _vertices;
  std::vector<std::shared_ptr<AbstractExpression>> _predicates;
};
}  // namespace opossum
