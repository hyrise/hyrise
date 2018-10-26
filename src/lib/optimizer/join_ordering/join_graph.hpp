#pragma once

#include <limits>
#include <memory>
#include <optional>
#include <unordered_set>
#include <utility>
#include <vector>

#include "join_graph_edge.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "types.hpp"

namespace opossum {

/**
 * A JoinGraph contains
 *      -> Vertices: output tables subplans
 *      -> Edges: predicates clustered by the vertices that they access
 *
 * It is used by join ordering algorithms.
 */
class JoinGraph final {
 public:
  /**
   * Tries to turn the subplan rooted at @lqp into a JoinGraph.
   * @return nullopt, if the root node would already be a vertex and thus the JoinGraph wouldn't be meaningful
   */
  static std::optional<JoinGraph> build_from_lqp(const std::shared_ptr<AbstractLQPNode>& lqp);

  /**
   * Traverse a plan for subgraphs from which JoinGraphs can be built with JoinGraph::build_from_lqp().
   * Once one is found, continue the traversal recursively from this JoinGraphs vertices.
   * @return All JoinGraphs that can be built from subgraphs in the specified LQP
   */
  static std::vector<JoinGraph> build_all_in_lqp(const std::shared_ptr<AbstractLQPNode>& lqp);

  JoinGraph() = default;
  JoinGraph(const std::vector<std::shared_ptr<AbstractLQPNode>>& vertices, const std::vector<JoinGraphEdge>& edges);

  /**
   * Find all predicates that reference to only the vertex at @param vertex_idx
   */
  std::vector<std::shared_ptr<AbstractExpression>> find_local_predicates(const size_t vertex_idx) const;

  /**
   * Find all predicates that "connect" the two vertex sets, i.e. have operands in both of them and nowhere else
   */
  std::vector<std::shared_ptr<AbstractExpression>> find_join_predicates(const JoinGraphVertexSet& vertex_set_a,
                                                                        const JoinGraphVertexSet& vertex_set_b) const;

  void print(std::ostream& stream = std::cout) const;

  const std::vector<std::shared_ptr<AbstractLQPNode>> vertices;
  const std::vector<JoinGraphEdge> edges;
};

}  // namespace opossum
