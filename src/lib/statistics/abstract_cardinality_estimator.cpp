#include "abstract_cardinality_estimator.hpp"

#include "optimizer/join_ordering/join_graph.hpp"
#include "statistics/join_graph_statistics_cache.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/lqp_utils.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "expression/expression_utils.hpp"

namespace hyrise {

void AbstractCardinalityEstimator::guarantee_join_graph(const JoinGraph& join_graph) const {
  cardinality_estimation_cache.join_graph_statistics_cache.emplace(
      JoinGraphStatisticsCache::from_join_graph(join_graph));
}

void AbstractCardinalityEstimator::guarantee_bottom_up_construction() const {
  cardinality_estimation_cache.statistics_by_lqp.emplace();
}

void AbstractCardinalityEstimator::populate_required_column_expressions(const std::shared_ptr<const AbstractLQPNode>& lqp) const {
  cardinality_estimation_cache.required_column_expressions.emplace();

  visit_lqp(lqp, [&](const auto& node){
    if (node->type == LQPNodeType::Join || node->type == LQPNodeType::Predicate) {
      for (const auto& root_expression : node->node_expressions) {
        visit_expression(root_expression, [&](const auto& expression) {
          if (expression->type == ExpressionType::LQPColumn) {
            cardinality_estimation_cache.required_column_expressions->emplace(expression);
          }

          return ExpressionVisitation::VisitArguments;
        });
      }
    }
    return LQPVisitation::VisitInputs;
  });
}

}  // namespace hyrise
