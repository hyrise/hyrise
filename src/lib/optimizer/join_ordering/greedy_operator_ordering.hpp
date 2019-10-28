#pragma once

#include <map>
#include <memory>

#include "abstract_join_ordering_algorithm.hpp"
#include "join_graph_edge.hpp"

namespace opossum {

class AbstractLQPNode;
class AbstractCostEstimator;
class JoinGraph;

/**
 * Heuristic join ordering algorithm derived from "A New Heuristic for Optimizing Large Queries"
 *
 * "At each step of the algorithm, we select two nodes i and j that have a minimum value of cardinality(join(i, j))) and
 * create a new node k = join(i, j)."
 */
class GreedyOperatorOrdering : public AbstractJoinOrderingAlgorithm {
 public:
  /**
   * @param join_graph      A JoinGraph for a part of an LQP with further subplans as vertices. GreedyOperatorOrdering
   *                        is only applied to this particular JoinGraph and doesn't modify the subplans in the
   *                        vertices.
   * @return                An LQP consisting of
   *                         * the operations from the JoinGraph in a greedy order, trying to minimize intermediate
   *                           cardinalities
   *                         * the subplans from the vertices below them
   */
  std::shared_ptr<AbstractLQPNode> operator()(const JoinGraph& join_graph,
                                              const std::shared_ptr<AbstractCostEstimator>& cost_estimator);

 private:
  // Cache plan cardinalities because calculating the repeatedly during sorting is expensive
  using PlanCardinalityPair = std::pair<std::shared_ptr<AbstractLQPNode>, float>;

  // Build a plan from joining all vertex clusters connected by @param edge
  static PlanCardinalityPair _build_plan_for_edge(
      const JoinGraphEdge& edge, const std::map<JoinGraphVertexSet, std::shared_ptr<AbstractLQPNode>>& vertex_clusters,
      const std::shared_ptr<AbstractCostEstimator>& cost_estimator);
};

}  // namespace opossum
