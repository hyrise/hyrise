#pragma once

#include <memory>
#include <vector>

namespace opossum {

class AbstractExpression;
class AbstractCardinalityEstimator;
class AbstractCostEstimator;
class AbstractLQPNode;
class JoinGraph;
class CardinalityEstimationCache;

/**
 * Given a JoinGraph (created by the JoinGraphBuilder), the responsibility of a join ordering algorithm is to bring the
 * joins into a close-to-optimal order. This means that the total cost of all joins, as estimated by the
 * AbstractCostEstimator should be minimal.
 *
 * JoinOrderingAlgorithms operate on vertices, which are considered to be atomic, as described in the JoinGraph class.
 * These may be StoredTableNodes, but could also be more complex subplans that are treated as a single table for the
 * purpose of join ordering.
 */
class AbstractJoinOrderingAlgorithm {
 public:
  virtual ~AbstractJoinOrderingAlgorithm() = default;

  /*
   * @param join_graph      A JoinGraph for a part of an LQP with further subplans as vertices. GreedyOperatorOrdering
   *                        is only applied to this particular JoinGraph and does not modify the subplans in the
   *                        vertices.
   * @return                An LQP consisting of
   *                         * the operations from the JoinGraph with their order determined by the join ordering
   *                           algorithm, which usually tries to minimize the intermediate cardinalities,
   *                         * the subplans from the vertices below them.
   */
  virtual std::shared_ptr<AbstractLQPNode> operator()(const JoinGraph& join_graph,
                                                      const std::shared_ptr<AbstractCostEstimator>& cost_estimator) = 0;

 protected:
  /**
   * Join two subplans using a set of predicates. This is called internally. For example, a greedy ordering algorithm
   * would call this with the smallest table (more correctly: subplan) and the next bigger one.
   *
   * One predicate ("primary predicate") becomes the join predicate, the others ("secondary predicates") are executed as
   * table scans (column vs column) after the join.
   * The primary predicate needs to be a simple "<column> <operator> <column>" predicate, otherwise the join operators
   * won't be able to execute it.
   *
   * Predicates are ordered so that the most selective predicate is executed first. This is also done by the
   * JoinPredicateOrderingRule, which is more general in two regards: First, it also handles non-inner joins. Second,
   * because of its position in the optimizer flow, optimizes subqueries that were flattened into joins. However, there
   * are other rules between the JoinOrderingRule and the JoinPredicateOrderingRule. These depend on cardinality
   * estimations, which currently only take the primary predicate into account (#1560). As such, we do some preliminary
   * ordering here in order for the most selective predicate to be the primary predicate.
   */
  static std::shared_ptr<AbstractLQPNode> _add_join_to_plan(
      std::shared_ptr<AbstractLQPNode> left_lqp, std::shared_ptr<AbstractLQPNode> right_lqp,
      const std::vector<std::shared_ptr<AbstractExpression>>& join_predicates,
      const std::shared_ptr<AbstractCostEstimator>& cost_estimator);

  /**
   * Adds a non-join predicate, i.e., a predicate that filters the output of `lqp`, on top of `lqp` and returns
   * the resulting node. This is called internally for predicates that are contained in the JoinGraph but do not link
   * two subplans.
   *
   * Predicates are ordered so that the cheapest predicate is executed first. This has some overlap with the
   * PredicateReorderingRule (see #1860).
   */
  static std::shared_ptr<AbstractLQPNode> _add_predicates_to_plan(
      const std::shared_ptr<AbstractLQPNode>& lqp, const std::vector<std::shared_ptr<AbstractExpression>>& predicates,
      const std::shared_ptr<AbstractCostEstimator>& cost_estimator);
};

}  // namespace opossum
