#include "window_node.hpp"

#include "expression/expression_utils.hpp"
#include "expression/window_function_expression.hpp"
#include "utils/assert.hpp"

namespace hyrise {

WindowNode::WindowNode(const std::shared_ptr<AbstractExpression>& window_function_expression)
    : AbstractLQPNode{LQPNodeType::Window, {window_function_expression}} {
  if constexpr (HYRISE_DEBUG) {
    Assert(window_function_expression && window_function_expression->type == ExpressionType::WindowFunction,
           "Expression used as window function must be of type WindowFunctionExpression.");
    const auto& window_function = static_cast<const WindowFunctionExpression&>(*window_function_expression);
    Assert(window_function.window() && window_function.window()->type == ExpressionType::Window,
           "WindowFunctionExpression must define a window.");
  }
}

std::string WindowNode::description(const DescriptionMode mode) const {
  const auto expression_mode = _expression_description_mode(mode);
  auto stream = std::stringstream{};

  stream << "[Window] ";
  stream << node_expressions.front()->description(expression_mode);
  // Print the window definition in any case.
  if (mode == DescriptionMode::Short) {
    const auto& window = static_cast<const WindowFunctionExpression&>(*node_expressions.front()).window();
    stream << " OVER (" << window->description(expression_mode) << ")";
  }

  return stream.str();
}

std::vector<std::shared_ptr<AbstractExpression>> WindowNode::output_expressions() const {
  auto output_expressions = left_input()->output_expressions();
  output_expressions.emplace_back(node_expressions.front());
  return output_expressions;
}

bool WindowNode::is_column_nullable(const ColumnID column_id) const {
  const auto& output_expressions = this->output_expressions();
  Assert(column_id < output_expressions.size(), "ColumnID out of range");
  Assert(left_input(), "Need left input to determine nullability");
  return output_expressions[column_id]->is_nullable_on_lqp(*left_input());
}

UniqueColumnCombinations WindowNode::unique_column_combinations() const {
  // TODO(anyone): The column added by the window function can be unique under certain circumstances. However, since the
  // WindowNode is close to the LQP root by definition, its data dependencies should not be the subject of many possible
  // optimizations. In detail, the window function's column is unique for each of the follwoing cases:
  //   (1) The window is not partitioned and the window function is row_number().
  //   (2) The window is not partitioned, there is a UCC on the ordered columns, the frame starts at partition begin,
  //       and the function is rank() or dense_rank().
  return _forward_left_unique_column_combinations();
}

size_t WindowNode::_on_shallow_hash() const {
  // The WindowFunctionExpression contains everything that is required for the hash. Its hash is combined with the
  // WindowNode's hash in AbstractLQPNode::hash().
  return 0;
}

std::shared_ptr<AbstractLQPNode> WindowNode::_on_shallow_copy(LQPNodeMapping& node_mapping) const {
  return std::make_shared<WindowNode>(
      expression_copy_and_adapt_to_different_lqp(*node_expressions.front(), node_mapping));
}

bool WindowNode::_on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const {
  return expression_equal_to_expression_in_different_lqp(*node_expressions.front(), *rhs.node_expressions.front(),
                                                         node_mapping);
}
}  // namespace hyrise
