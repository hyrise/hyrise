#include "aggregate_node.hpp"

#include <algorithm>
#include <memory>
#include <optional>
#include <sstream>
#include <string>
#include <vector>

#include "expression/expression_utils.hpp"
#include "expression/lqp_cxlumn_expression.hpp"
#include "resolve_type.hpp"
#include "statistics/cxlumn_statistics.hpp"
#include "statistics/table_statistics.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

AggregateNode::AggregateNode(const std::vector<std::shared_ptr<AbstractExpression>>& group_by_expressions,
                             const std::vector<std::shared_ptr<AbstractExpression>>& aggregate_expressions)
    : AbstractLQPNode(LQPNodeType::Aggregate),
      group_by_expressions(group_by_expressions),
      aggregate_expressions(aggregate_expressions) {
#if IS_DEBUG
  for (const auto& aggregate_expression : aggregate_expressions) {
    DebugAssert(aggregate_expression->type == ExpressionType::Aggregate,
                "Expression used as aggregate expression must be of type AggregateExpression.");
  }
#endif

  _column_expressions.reserve(group_by_expressions.size() + aggregate_expressions.size());
  _column_expressions.insert(_column_expressions.end(), group_by_expressions.begin(), group_by_expressions.end());
  _column_expressions.insert(_column_expressions.end(), aggregate_expressions.begin(), aggregate_expressions.end());
}

std::string AggregateNode::description() const {
  std::stringstream stream;

  stream << "[Aggregate] GroupBy: [" << expression_cxlumn_names(group_by_expressions);
  stream << "] Aggregates: [" << expression_cxlumn_names(aggregate_expressions) << "]";

  return stream.str();
}

std::shared_ptr<TableStatistics> AggregateNode::derive_statistics_from(
    const std::shared_ptr<AbstractLQPNode>& left_input, const std::shared_ptr<AbstractLQPNode>& right_input) const {
  DebugAssert(left_input && !right_input, "AggregateNode need left_input and no right_input");

  const auto input_statistics = left_input->get_statistics();
  const auto row_count = input_statistics->row_count();

  std::vector<std::shared_ptr<const BaseCxlumnStatistics>> cxlumn_statistics;
  cxlumn_statistics.reserve(_column_expressions.size());

  for (const auto& expression : _column_expressions) {
    const auto cxlumn_id = left_input->find_cxlumn_id(*expression);
    if (cxlumn_id) {
      cxlumn_statistics.emplace_back(input_statistics->cxlumn_statistics()[*cxlumn_id]);
    } else {
      // TODO(anybody) Statistics for expressions not yet supported
      resolve_data_type(expression->data_type(), [&](const auto data_type_t) {
        using ExpressionDataType = typename decltype(data_type_t)::type;
        cxlumn_statistics.emplace_back(
            std::make_shared<CxlumnStatistics<ExpressionDataType>>(CxlumnStatistics<ExpressionDataType>::dummy()));
      });
    }
  }

  return std::make_shared<TableStatistics>(TableType::Data, row_count, cxlumn_statistics);
}

const std::vector<std::shared_ptr<AbstractExpression>>& AggregateNode::column_expressions() const {
  return _column_expressions;
}

std::vector<std::shared_ptr<AbstractExpression>> AggregateNode::node_expressions() const { return _column_expressions; }

std::shared_ptr<AbstractLQPNode> AggregateNode::_on_shallow_copy(LQPNodeMapping& node_mapping) const {
  return std::make_shared<AggregateNode>(
      expressions_copy_and_adapt_to_different_lqp(group_by_expressions, node_mapping),
      expressions_copy_and_adapt_to_different_lqp(aggregate_expressions, node_mapping));
}

bool AggregateNode::_on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const {
  const auto& aggregate_node = static_cast<const AggregateNode&>(rhs);

  return expressions_equal_to_expressions_in_different_lqp(group_by_expressions, aggregate_node.group_by_expressions,
                                                           node_mapping) &&
         expressions_equal_to_expressions_in_different_lqp(aggregate_expressions, aggregate_node.aggregate_expressions,
                                                           node_mapping);
}
}  // namespace opossum
