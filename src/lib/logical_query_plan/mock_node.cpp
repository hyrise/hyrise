#include "mock_node.hpp"

#include <memory>
#include <string>
#include <vector>

#include "expression/lqp_column_expression.hpp"
#include "statistics/table_statistics.hpp"
#include "utils/assert.hpp"

using namespace std::string_literals;  // NOLINT

namespace opossum {

MockNode::MockNode(const ColumnDefinitions& column_definitions, const std::optional<std::string>& name)
    : AbstractLQPNode(LQPNodeType::Mock), name(name), _column_definitions(column_definitions) {}

LQPColumnReference MockNode::get_column(const std::string& column_name) const {
  const auto& column_definitions = this->column_definitions();

  for (auto column_id = ColumnID{0}; column_id < column_definitions.size(); ++column_id) {
    if (column_definitions[column_id].second == column_name) return LQPColumnReference{shared_from_this(), column_id};
  }

  Fail("Couldn't find column named '"s + column_name + "' in MockNode");
}

const MockNode::ColumnDefinitions& MockNode::column_definitions() const { return _column_definitions; }

const std::vector<std::shared_ptr<AbstractExpression>>& MockNode::column_expressions() const {
  if (!_column_expressions) {
    _column_expressions.emplace();

    const auto column_count = _column_definitions.size();

    for (auto column_id = ColumnID{0}; column_id < column_count; ++column_id) {
      const auto column_reference = LQPColumnReference(shared_from_this(), column_id);
      _column_expressions->emplace_back(std::make_shared<LQPColumnExpression>(column_reference));
    }
  }

  return *_column_expressions;
}

bool MockNode::is_column_nullable(const ColumnID column_id) const {
  Assert(column_id < _column_definitions.size(), "ColumnID out of range");
  return false;
}

std::string MockNode::description() const { return "[MockNode '"s + name.value_or("Unnamed") + "']"; }

std::shared_ptr<TableStatistics> MockNode::derive_statistics_from(
    const std::shared_ptr<AbstractLQPNode>& left_input, const std::shared_ptr<AbstractLQPNode>& right_input) const {
  Assert(_table_statistics, "MockNode statistics need to be explicitely set");
  return _table_statistics;
}

void MockNode::set_statistics(const std::shared_ptr<TableStatistics>& statistics) { _table_statistics = statistics; }

std::shared_ptr<AbstractLQPNode> MockNode::_on_shallow_copy(LQPNodeMapping& node_mapping) const {
  const auto mock_node = MockNode::make(_column_definitions);
  mock_node->set_statistics(_table_statistics);
  return mock_node;
}

bool MockNode::_on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const {
  const auto& mock_node = static_cast<const MockNode&>(rhs);
  return _column_definitions == mock_node._column_definitions && _table_statistics == mock_node._table_statistics;
}

}  // namespace opossum
