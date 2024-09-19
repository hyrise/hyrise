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

  // Below an empirically determined threshold, we use DpCcp. Else, we use GreedyOperatorOrdering (GOO).
  //
  // This threshold was most recently evaluated and adapted in #2652, backed by former investigations in #2626 and
  // #2642. There, experiments showed that join ordering was a huge performance issue for JOB and TPC-DS queries (many
  // queries spent several hundreds of milliseconds here). One issue was the shere number of candidates, but we also
  // identified two main bottlenecks in cardinality estimation:
  //     (i) Scaling histograms because we do it very often (thousands of times for some TPC-DS and JOB queries).
  //    (ii) Creating new histograms for joined tables and column vs. column predicates.
  // Before that, we used a threshold of 9. Using GOO for eight vertices also improved execution for some TPC-DS queries
  // and many JOB qweries, where our cardinality estimations are notoriously bad (skew, many dimension table joins).
  // Further decreasing this value did not have noticeable positive effects (but some negative ones).
  //
  // TODO(anybody): Revisit and re-evaluate whenever we significantly change something in cardinality estimation.
  constexpr static auto MIN_VERTICES_FOR_HEURISTIC = size_t{8};

 protected:
  void _apply_to_plan_without_subqueries(const std::shared_ptr<AbstractLQPNode>& lqp_root) const override;
};

}  // namespace hyrise
