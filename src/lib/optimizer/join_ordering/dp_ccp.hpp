#pragma once

#include <memory>

#include "abstract_join_ordering_algorithm.hpp"

namespace hyrise {

class AbstractCostEstimator;
class JoinGraph;

/**
 * Optimal join ordering algorithm described in Moerkotte and Neumann: "Analysis of two existing and one new dynamic
 * programming algorithm for the generation of optimal bushy join trees without cross products" (see
 * https://www.vldb.org/conf/2006/p930-moerkotte.pdf).
 * TODO(dey4ss): Check if EnumerateCcp implements the original or the errata version  of EnumerateCmp
 *               (http://www.vldb.org/pvldb/vol11/p1069-meister.pdf).
 *
 * DpCcp is an optimal join ordering algorithm based on dynamic programming. It handles only inner joins and cross
 * joins and treats outer joins as opaque (i.e. outer joins are not moved and no other joins are moved pass them).
 * DpCcp is driven by EnumerateCcp which enumerates all candidate join operations.
 *
 * Local predicates are pushed down and sorted by increasing cost.
 */
class DpCcp final : public AbstractJoinOrderingAlgorithm {
 public:
  std::shared_ptr<AbstractLQPNode> operator()(const JoinGraph& join_graph,
                                              const std::shared_ptr<AbstractCostEstimator>& cost_estimator) override;
};

}  // namespace hyrise
