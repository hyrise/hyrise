#include "mock_node.hpp"

#include <memory>
#include <string>
#include <vector>

#include "optimizer/table_statistics.hpp"
#include "utils/assert.hpp"

namespace opossum {

MockNode::MockNode(const ColumnDefinitions& column_definitions, const std::optional<std::string>& alias)
    : AbstractLQPNode(LQPNodeType::Mock), _constructor_arguments(column_definitions) {
  for (const auto& column_definition : column_definitions) {
    _output_column_names.emplace_back(column_definition.second);
  }

  set_alias(alias);
}

MockNode::MockNode(const TableStatisticsSPtr& statistics, const std::optional<std::string>& alias)
    : AbstractLQPNode(LQPNodeType::Mock), _constructor_arguments(statistics) {
  set_statistics(statistics);

  for (size_t column_statistics_idx = 0; column_statistics_idx < statistics->column_statistics().size();
       ++column_statistics_idx) {
    _output_column_names.emplace_back("MockCol" + std::to_string(column_statistics_idx));
  }

  set_alias(alias);
}

AbstractLQPNodeSPtr MockNode::_deep_copy_impl(
    const AbstractLQPNodeSPtr& copied_left_input,
    const AbstractLQPNodeSPtr& copied_right_input) const {
  if (_constructor_arguments.type() == typeid(TableStatisticsSPtr)) {
    return MockNode::make(boost::get<TableStatisticsSPtr>(_constructor_arguments), _table_alias);
  }

  Assert(_constructor_arguments.type() == typeid(ColumnDefinitions), "Invalid constructor args state. Bug.");

  return MockNode::make(boost::get<ColumnDefinitions>(_constructor_arguments), _table_alias);
}

const std::vector<std::string>& MockNode::output_column_names() const { return _output_column_names; }

const boost::variant<MockNode::ColumnDefinitions, TableStatisticsSPtr>& MockNode::constructor_arguments()
    const {
  return _constructor_arguments;
}

std::string MockNode::get_verbose_column_name(ColumnID column_id) const {
  // Aliasing a MockNode doesn't really make sense, but let's stay covered
  if (_table_alias) {
    return *_table_alias + "." + output_column_names()[column_id];
  }
  return output_column_names()[column_id];
}

std::string MockNode::description() const { return "[MockTable]"; }

bool MockNode::shallow_equals(const AbstractLQPNode& rhs) const {
  Assert(rhs.type() == type(), "Can only compare nodes of the same type()");
  const auto& mock_node = static_cast<const MockNode&>(rhs);

  Assert(_constructor_arguments.type() != typeid(TableStatisticsSPtr),
         "Comparison of statistics not implemented, because this is painful");
  return _constructor_arguments == mock_node._constructor_arguments;
}

}  // namespace opossum
