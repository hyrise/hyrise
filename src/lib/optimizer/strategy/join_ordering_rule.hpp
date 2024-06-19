#pragma once

#include <memory>
#include <string>

#include "abstract_rule.hpp"

namespace hyrise {

class AbstractCostEstimator;

/**
 * A rule that brings join operations into a (supposedly) efficient order. Currently, only the order of inner joins is
 * modified using either the DpCcp algorithm or GreedyOperatorOrdering.
 */
class JoinOrderingRule : public AbstractRule {
 public:
  std::string name() const override;

  // Below this threshold, we use DpCcp. Else, we use GreedyOperatorOrdering.
  // TODO(anybody): Evaluate and adapt if our cost/cardinality estimation becomes faster and/or better. As investigated
  //                in #2626 and #2642, we see two main bottlenecks in cardinality estimation:
  //     (i) Scaling histograms because we do it very often (thousands of times for some TPC-DS and JOB queries).
  //    (ii) Creating new histograms for joined tables.
  constexpr static auto MIN_VERTICES_FOR_HEURISTIC = size_t{9};

 protected:
  void _apply_to_plan_without_subqueries(const std::shared_ptr<AbstractLQPNode>& lqp_root) const override;
};

}  // namespace hyrise
