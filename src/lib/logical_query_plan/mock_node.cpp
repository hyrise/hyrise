#include "mock_node.hpp"

#include <memory>
#include <string>
#include <vector>

#include "optimizer/table_statistics.hpp"
#include "utils/assert.hpp"

namespace opossum {

MockNode::MockNode(const std::optional<std::string>& alias) : AbstractLQPNode(LQPNodeType::Mock) { set_alias(alias); }

MockNode::MockNode(const ColumnDefinitions& column_definitions, const std::optional<std::string>& alias)
    : AbstractLQPNode(LQPNodeType::Mock) {
  for (const auto& column_definition : column_definitions) {
    _output_column_names.emplace_back(column_definition.second);
  }

  set_alias(alias);
}

MockNode::MockNode(const std::shared_ptr<TableStatistics>& statistics, const std::optional<std::string>& alias)
    : AbstractLQPNode(LQPNodeType::Mock) {
  set_statistics(statistics);

  for (size_t column_statistics_idx = 0; column_statistics_idx < statistics->column_statistics().size();
       ++column_statistics_idx) {
    _output_column_names.emplace_back("MockCol" + std::to_string(column_statistics_idx));
  }

  set_alias(alias);
}

std::shared_ptr<AbstractLQPNode> MockNode::_deep_copy_impl(const std::shared_ptr<AbstractLQPNode>& left_child, const std::shared_ptr<AbstractLQPNode>& right_child) const {
  Fail("Cannot deep_copy MockNodes because we cannot get a deep copy of the statistics");
  return nullptr;
}

const std::vector<std::string>& MockNode::output_column_names() const { return _output_column_names; }

std::string MockNode::get_verbose_column_name(ColumnID column_id) const {
  // Aliasing a MockNode doesn't really make sense, but let's stay covered
  if (_table_alias) {
    return *_table_alias + "." + output_column_names()[column_id];
  }
  return output_column_names()[column_id];
}

std::string MockNode::description() const { return "[MockTable]"; }
}  // namespace opossum
