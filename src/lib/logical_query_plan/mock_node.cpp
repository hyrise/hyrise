#include "mock_node.hpp"

#include <memory>
#include <string>
#include <vector>

#include "expression/lqp_cxlumn_expression.hpp"
#include "statistics/table_statistics.hpp"
#include "utils/assert.hpp"

using namespace std::string_literals;  // NOLINT

namespace opossum {

MockNode::MockNode(const ColumnDefinitions& cxlumn_definitions, const std::optional<std::string>& name)
    : AbstractLQPNode(LQPNodeType::Mock), _name(name), _constructor_arguments(cxlumn_definitions) {}

MockNode::MockNode(const std::shared_ptr<TableStatistics>& statistics)
    : AbstractLQPNode(LQPNodeType::Mock), _constructor_arguments(statistics) {}

LQPColumnReference MockNode::get_column(const std::string& name) const {
  const auto& cxlumn_definitions = this->cxlumn_definitions();

  for (auto cxlumn_id = CxlumnID{0}; cxlumn_id < cxlumn_definitions.size(); ++cxlumn_id) {
    if (cxlumn_definitions[cxlumn_id].second == name) return LQPColumnReference{shared_from_this(), cxlumn_id};
  }

  Fail("Couldn't find column named '"s + name + "' in MockNode");
}

const MockNode::ColumnDefinitions& MockNode::cxlumn_definitions() const {
  Assert(_constructor_arguments.type() == typeid(ColumnDefinitions), "Unexpected type");
  return boost::get<ColumnDefinitions>(_constructor_arguments);
}

const boost::variant<MockNode::ColumnDefinitions, std::shared_ptr<TableStatistics>>& MockNode::constructor_arguments()
    const {
  return _constructor_arguments;
}

const std::vector<std::shared_ptr<AbstractExpression>>& MockNode::column_expressions() const {
  if (!_column_expressions) {
    _column_expressions.emplace();

    auto cxlumn_count = size_t{0};
    if (_constructor_arguments.type() == typeid(ColumnDefinitions)) {
      cxlumn_count = boost::get<ColumnDefinitions>(_constructor_arguments).size();
    } else {
      cxlumn_count = boost::get<std::shared_ptr<TableStatistics>>(_constructor_arguments)->cxlumn_statistics().size();
    }

    for (auto cxlumn_id = CxlumnID{0}; cxlumn_id < cxlumn_count; ++cxlumn_id) {
      const auto column_reference = LQPColumnReference(shared_from_this(), cxlumn_id);
      _column_expressions->emplace_back(std::make_shared<LQPColumnExpression>(column_reference));
    }
  }

  return *_column_expressions;
}

std::string MockNode::description() const { return "[MockNode '"s + _name.value_or("Unnamed") + "']"; }

std::shared_ptr<TableStatistics> MockNode::derive_statistics_from(
    const std::shared_ptr<AbstractLQPNode>& left_input, const std::shared_ptr<AbstractLQPNode>& right_input) const {
  Assert(_constructor_arguments.type() == typeid(std::shared_ptr<TableStatistics>),
         "Can only return statistics from statistics mock node");
  return boost::get<std::shared_ptr<TableStatistics>>(_constructor_arguments);
}

std::shared_ptr<AbstractLQPNode> MockNode::_on_shallow_copy(LQPNodeMapping& node_mapping) const {
  if (_constructor_arguments.type() == typeid(std::shared_ptr<TableStatistics>)) {
    return MockNode::make(boost::get<std::shared_ptr<TableStatistics>>(_constructor_arguments));
  } else {
    return MockNode::make(boost::get<ColumnDefinitions>(_constructor_arguments));
  }
}

bool MockNode::_on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const {
  const auto& mock_node = static_cast<const MockNode&>(rhs);

  if (_constructor_arguments.which() != mock_node._constructor_arguments.which()) return false;

  if (_constructor_arguments.type() == typeid(ColumnDefinitions)) {
    return boost::get<ColumnDefinitions>(_constructor_arguments) ==
           boost::get<ColumnDefinitions>(mock_node._constructor_arguments);
  } else {
    Fail("Comparison of statistics not implemented, because this is painful");
  }
}

}  // namespace opossum
