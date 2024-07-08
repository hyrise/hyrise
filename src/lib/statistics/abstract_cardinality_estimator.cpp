#include "abstract_cardinality_estimator.hpp"

#include <memory>

#include "expression/abstract_expression.hpp"
#include "expression/expression_utils.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/lqp_utils.hpp"
#include "optimizer/join_ordering/join_graph.hpp"
#include "statistics/join_graph_statistics_cache.hpp"

namespace hyrise {

void AbstractCardinalityEstimator::guarantee_join_graph(const JoinGraph& join_graph) const {
  cardinality_estimation_cache.join_graph_statistics_cache.emplace(
      JoinGraphStatisticsCache::from_join_graph(join_graph));
}

void AbstractCardinalityEstimator::guarantee_bottom_up_construction(
    const std::shared_ptr<const AbstractLQPNode>& lqp) const {
  cardinality_estimation_cache.statistics_by_lqp.emplace();
  // populate_required_column_expressions(lqp);
}

void AbstractCardinalityEstimator::populate_required_column_expressions(
    const std::shared_ptr<const AbstractLQPNode>& lqp) const {
  // cardinality_estimation_cache.required_column_expressions.emplace();

  // visit_lqp(lqp, [&](const auto& node) {
  //   _add_required_columns(node, *cardinality_estimation_cache.required_column_expressions);
  //   return LQPVisitation::VisitInputs;
  // });
}

void AbstractCardinalityEstimator::_add_required_columns(const std::shared_ptr<const AbstractLQPNode>& node,
                                                         ExpressionUnorderedSet& required_columns) {
  // if (node->type == LQPNodeType::Join || node->type == LQPNodeType::Predicate) {
  //   for (const auto& root_expression : node->node_expressions) {
  //     visit_expression(root_expression, [&](const auto& expression) {
  //       if (expression->type == ExpressionType::LQPColumn) {
  //         required_columns.emplace(expression);
  //       }

  //       return ExpressionVisitation::VisitArguments;
  //     });
  //   }
  // }
}

}  // namespace hyrise
