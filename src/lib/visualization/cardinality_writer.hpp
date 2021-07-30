#pragma once

#include <iomanip>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <boost/algorithm/string.hpp>

#include "expression/abstract_expression.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "operators/abstract_operator.hpp"
#include "statistics/cardinality_estimator.hpp"

namespace opossum {

/**
 * Class for writing the output cardinalities of each node in PQP and LQP into a CSV file./
 */

class CardinalityWriter {
 public:
  CardinalityWriter();

  /**
   * Writes output cardinalities of each node in PQP rooted at pqp_roots and LQP rooted at lqp_roots into a CSV file 
   * and adds column which states benchmark_item_id.
   */
  void write_cardinalities(const std::vector<std::shared_ptr<AbstractLQPNode>>& lqp_roots,
                           const std::vector<std::shared_ptr<AbstractOperator>>& pqp_roots,
                           const std::string& benchmark_item_id);

 protected:
  /**
   * Methods for traversing LQP and PQP trees.
   */
  void _build_lqp_graph(const std::vector<std::shared_ptr<AbstractLQPNode>>& lqp_roots);

  void _build_pqp_graph(const std::vector<std::shared_ptr<AbstractOperator>>& pqp_roots);

  void _build_lqp_subtree(const std::shared_ptr<AbstractLQPNode>& node,
                          std::unordered_set<std::shared_ptr<const AbstractLQPNode>>& visited_nodes,
                          ExpressionUnorderedSet& visited_sub_queries);

  void _build_pqp_subtree(const std::shared_ptr<const AbstractOperator>& op,
                          std::unordered_set<std::shared_ptr<const AbstractOperator>>& visited_ops);

  void _visit_pqp_subqueries(const std::shared_ptr<const AbstractOperator>& op,
                             const std::shared_ptr<AbstractExpression>& expression,
                             std::unordered_set<std::shared_ptr<const AbstractOperator>>& visited_ops);

  CardinalityEstimator _cardinality_estimator;
  std::vector<std::shared_ptr<const AbstractLQPNode>> _lqp_nodes;
  std::vector<std::shared_ptr<const AbstractOperator>> _pqp_nodes;
};

}  // namespace opossum