#pragma once

#include <memory>
#include <vector>

namespace opossum {

class AbstractExpression;
class AbstractCostEstimator;
class AbstractLQPNode;
class JoinGraph;

/**
 * Optimal join ordering algorithm described in "Analysis of two existing and one new dynamic programming algorithm for
 * the generation of optimal bushy join trees without cross products"
 * https://dl.acm.org/citation.cfm?id=1164207
 *
 * DpCcp is an optimal join ordering algorithm based on dynamic programming. It handles only inner joins and cross
 * joins and treats outer joins as opaque (i.e. outer joins are not moved and no other joins are moved pass them).
 * DpCcp is driven by EnumerateCcp which enumerates all candidate join operations.
 *
 * Local predicates are pushed down and sorted by increasing cost.
 */
class DpCcp final {
 public:
  explicit DpCcp(const std::shared_ptr<AbstractCostEstimator>& cost_estimator);

  /**
   * @param join_graph      A JoinGraph for a part of an LQP with further subplans as vertices. DpCcp is only applied
   *                        to this particular JoinGraph and doesn't modify the subplans in the vertices.
   * @return                An LQP consisting of
   *                         * the operations from the JoinGraph in an optimal order
   *                         * the subplans from the vertices below them
   */
  std::shared_ptr<AbstractLQPNode> operator()(const JoinGraph& join_graph);

 private:
  std::shared_ptr<AbstractLQPNode> _add_predicates_to_plan(
      const std::shared_ptr<AbstractLQPNode>& lqp,
      const std::vector<std::shared_ptr<AbstractExpression>>& predicates) const;

  std::shared_ptr<AbstractLQPNode> _add_join_to_plan(
      const std::shared_ptr<AbstractLQPNode>& left_lqp, const std::shared_ptr<AbstractLQPNode>& right_lqp,
      std::vector<std::shared_ptr<AbstractExpression>> join_predicates) const;

  std::shared_ptr<AbstractCostEstimator> _cost_estimator;
};

}  // namespace opossum
