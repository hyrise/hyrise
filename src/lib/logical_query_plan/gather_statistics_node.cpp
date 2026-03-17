#include "gather_statistics_node.hpp"

#include <cstddef>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include "expression/abstract_expression.hpp"
#include "expression/expression_utils.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/data_dependencies/order_dependency.hpp"
#include "logical_query_plan/data_dependencies/unique_column_combination.hpp"

namespace hyrise {

GatherStatisticsNode::GatherStatisticsNode(const std::shared_ptr<AbstractExpression>& init_expression)
    : AbstractLQPNode(LQPNodeType::GatherStatistics, {init_expression}) {}

std::string GatherStatisticsNode::description(const DescriptionMode mode) const {
  const auto expression_mode = _expression_description_mode(mode);

  auto stream = std::stringstream{};
  stream << "[GatherStatistics] " << expression()->description(expression_mode);
  return stream.str();
}

UniqueColumnCombinations GatherStatisticsNode::unique_column_combinations() const {
  return _forward_left_unique_column_combinations();
}

OrderDependencies GatherStatisticsNode::order_dependencies() const {
  return _forward_left_order_dependencies();
}

std::shared_ptr<AbstractExpression> GatherStatisticsNode::expression() const {
  return node_expressions[0];
}

std::shared_ptr<AbstractLQPNode> GatherStatisticsNode::_on_shallow_copy(LQPNodeMapping& node_mapping) const {
  return GatherStatisticsNode::make(expression_copy_and_adapt_to_different_lqp(*expression(), node_mapping));
}

bool GatherStatisticsNode::_on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const {
  const auto& gather_statistics_node = static_cast<const GatherStatisticsNode&>(rhs);
  const auto equal = expression_equal_to_expression_in_different_lqp(
      *expression(), *gather_statistics_node.expression(), node_mapping);

  return equal;
}

}  // namespace hyrise
