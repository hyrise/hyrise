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
   * From the subtree of root, build a JoinGraph.
   * The LQP is not modified during this process.
   *
   * Need an instance of the shared_ptr to keep the ref count > 0
   */
  std::shared_ptr<JoinGraph> operator()(const std::shared_ptr<AbstractLQPNode>& lqp);

 private:
  /**
   * Traverse the LQP recursively identifying predicates and vertices along the way
   */
  void _traverse(const std::shared_ptr<AbstractLQPNode>& node);

  /**
   * A subgraph in the LQP consisting of UnionNodes and PredicateNodes can be translated into a single complex predicate
   *
   *         ______________UnionNode________
   *        /                               \
   *    PredicateNode(a > 5)            PredicateNode(a < 3)
   *       |                                 |
   *       |                            PredicateNode(b > 12)
   *       \____________AggregateNode_______/
   *                           |
   *                         [...]
   *
   *  Represents the JoinPlanPredicate "a > 5 OR (a < 3 AND b > 12)". The AggregateNode is the Predicate's "base_node"
   *  i.e. the node on which's output the Predicate operates on.
   *
   *  _parse_predicate() and _parse_union() perform this conversion, calling each other recursively
   */
  struct PredicateParseResult {
    PredicateParseResult(const std::shared_ptr<AbstractLQPNode>& base_node,
                         const std::shared_ptr<const AbstractJoinPlanPredicate>& predicate)
        : base_node(base_node), predicate(predicate) {}

    std::shared_ptr<AbstractLQPNode> base_node;
    std::shared_ptr<const AbstractJoinPlanPredicate> predicate;
  };

  PredicateParseResult _parse_predicate(const std::shared_ptr<AbstractLQPNode>& node) const;
  PredicateParseResult _parse_union(const std::shared_ptr<UnionNode>& union_node) const;

  /**
   * Returns whether a node of the given type is a JoinGraph vertex in all cases. This is true for all node types that
   * aren't Predicates, Joins or Unions.
   */
  bool _lqp_node_type_is_vertex(const LQPNodeType node_type) const;

  std::vector<std::shared_ptr<AbstractLQPNode>> _vertices;
  std::vector<std::shared_ptr<const AbstractJoinPlanPredicate>> _predicates;
};
}  // namespace opossum
