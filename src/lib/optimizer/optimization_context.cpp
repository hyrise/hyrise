#include "optimization_context.hpp"

#include "expression/abstract_expression.hpp"
#include "expression/expression_utils.hpp"
#include "expression/lqp_subquery_expression.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/lqp_utils.hpp"

namespace {

using namespace opossum;  // NOLINT

void populate_context(const std::shared_ptr<AbstractLQPNode>& plan,
                      const std::shared_ptr<OptimizationContext>& context) {
  visit_lqp(plan, [&](const auto& node) {
    if (node->input_count() == 0) {
      context->vertex_indices.emplace(node, context->vertex_indices.size());
    }

    if (const auto join_node = std::dynamic_pointer_cast<JoinNode>(node)) {
      if (join_node->join_predicate()) {
        context->predicate_indices.emplace(join_node->join_predicate(), context->predicate_indices.size());
      }
    } else if (const auto predicate_node = std::dynamic_pointer_cast<PredicateNode>(node)) {
      context->predicate_indices.emplace(predicate_node->predicate(), context->predicate_indices.size());
    }

    for (const auto& node_expression : node->node_expressions) {
      visit_expression(node_expression, [&](const auto& sub_expression) {
        if (const auto select_expression = std::dynamic_pointer_cast<LQPSubqueryExpression>(sub_expression)) {
          populate_context(select_expression->lqp, context);
        }
        return ExpressionVisitation::VisitArguments;
      });
    }

    return LQPVisitation::VisitInputs;
  });
}

}  // namespace

namespace opossum {

std::shared_ptr<OptimizationContext> OptimizationContext::create_context_for_lqp(const std::shared_ptr<AbstractLQPNode>& lqp) {

  const auto context = std::make_shared<OptimizationContext>();

  populate_context(plan, context);

  return context;
}

void OptimizationContext::clear_caches() {
  join_statistics_cache.reset();
  plan_statistics_cache.reset();
  plan_cost_cache.reset();
}

void OptimizationContext::print(std::ostream& stream) const {
  stream << "OptimizationContext {" << std::endl;
  stream << "Leafs:" << std::endl;
  for (const auto& [leaf, idx] : vertex_indices) {
    stream << "  " << leaf->description() << ": " << idx << std::endl;
  }
  stream << "Predicates:" << std::endl;
  for (const auto& [predicate, idx] : predicate_indices) {
    stream << "  " << predicate->as_column_name() << ": " << (idx + vertex_indices.size()) << std::endl;
  }

  stream << "}" << std::endl;
}

}  // namespace opossum