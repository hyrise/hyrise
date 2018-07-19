#include "projection_node.hpp"

#include <sstream>

#include "expression/expression_utils.hpp"
#include "resolve_type.hpp"
#include "statistics/column_statistics.hpp"
#include "statistics/table_statistics.hpp"
#include "utils/assert.hpp"

namespace opossum {

ProjectionNode::ProjectionNode(const std::vector<std::shared_ptr<AbstractExpression>>& expressions)
    : AbstractLQPNode(LQPNodeType::Projection), expressions(expressions) {}

std::string ProjectionNode::description() const {
  std::stringstream stream;

  stream << "[Projection] " << expression_column_names(expressions);

  return stream.str();
}

const std::vector<std::shared_ptr<AbstractExpression>>& ProjectionNode::column_expressions() const {
  return expressions;
}

std::vector<std::shared_ptr<AbstractExpression>> ProjectionNode::node_expressions() const { return expressions; }

std::shared_ptr<TableStatistics> ProjectionNode::derive_statistics_from(
    const std::shared_ptr<AbstractLQPNode>& left_input, const std::shared_ptr<AbstractLQPNode>& right_input) const {
  DebugAssert(left_input && !right_input, "ProjectionNode need left_input and no right_input");

  const auto input_statistics = left_input->get_statistics();
  auto table_type = input_statistics->table_type();
  const auto row_count = input_statistics->row_count();

  std::vector<std::shared_ptr<const BaseColumnStatistics>> column_statistics;
  column_statistics.reserve(expressions.size());

  for (const auto& expression : expressions) {
    const auto column_id = left_input->find_column_id(*expression);
    if (column_id) {
      column_statistics.emplace_back(input_statistics->column_statistics()[*column_id]);
    } else {
      // TODO(anybody) Statistics for expressions not yet supported
      resolve_data_type(expression->data_type(), [&](const auto data_type_t) {
        using ExpressionDataType = typename decltype(data_type_t)::type;
        column_statistics.emplace_back(
            std::make_shared<ColumnStatistics<ExpressionDataType>>(ColumnStatistics<ExpressionDataType>::dummy()));
      });

      table_type = TableType::Data;
    }
  }

  return std::make_shared<TableStatistics>(table_type, row_count, column_statistics);
}

std::shared_ptr<AbstractLQPNode> ProjectionNode::_on_shallow_copy(LQPNodeMapping& node_mapping) const {
  return make(expressions_copy_and_adapt_to_different_lqp(expressions, node_mapping));
}

bool ProjectionNode::_on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const {
  const auto& rhs_expressions = static_cast<const ProjectionNode&>(rhs).expressions;
  return expressions_equal_to_expressions_in_different_lqp(expressions, rhs_expressions, node_mapping);
}

}  // namespace opossum
