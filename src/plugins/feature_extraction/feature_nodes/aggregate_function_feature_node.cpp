#include "aggregate_function_feature_node.hpp"

#include "expression/pqp_column_expression.hpp"
#include "feature_extraction/feature_extraction_utils.hpp"
#include "feature_extraction/feature_nodes/column_feature_node.hpp"

namespace hyrise {

AggregateFunctionFeatureNode::AggregateFunctionFeatureNode(
    const std::shared_ptr<AbstractExpression>& lqp_expression,
    const std::shared_ptr<AbstractExpression>& pqp_expression,
    const std::shared_ptr<AbstractFeatureNode>& operator_node_input)
    : AbstractFeatureNode(FeatureNodeType::AggregateFunction, nullptr, nullptr) {
  Assert(pqp_expression->type == ExpressionType::Aggregate, "AggregateExpression expected");
  const auto& aggregate_expression = static_cast<AggregateExpression&>(*pqp_expression);
  Assert(aggregate_expression.argument()->type == ExpressionType::PQPColumn, "Column expected");
  const auto column_id = static_cast<PQPColumnExpression&>(*aggregate_expression.argument()).column_id;
  _left_input = ColumnFeatureNode::from_expression(operator_node_input, lqp_expression->arguments[0], column_id);
  _aggregate_function = aggregate_expression.aggregate_function;
  _is_count_star = AggregateExpression::is_count_star(aggregate_expression);
}

std::shared_ptr<FeatureVector> AggregateFunctionFeatureNode::_on_to_feature_vector() const {
  const auto features = one_hot_encoding<AggregateFunction>(_aggregate_function);
  features->emplace_back(static_cast<Feature>(_is_count_star));
  return features;
}

const std::vector<std::string>& AggregateFunctionFeatureNode::feature_headers() const {
  return headers();
}

const std::vector<std::string>& AggregateFunctionFeatureNode::headers() {
  static auto ohe_headers_function = one_hot_headers<AggregateFunction>("aggregate_function.");
  if (ohe_headers_function.size() == magic_enum::enum_count<AggregateFunction>()) {
    ohe_headers_function.emplace_back("is_count_star");
  }
  return ohe_headers_function;
}

}  // namespace hyrise
