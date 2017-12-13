#include "mock_node.hpp"

#include <memory>
#include <string>
#include <vector>

#include "optimizer/table_statistics.hpp"
#include "utils/assert.hpp"

namespace opossum {

MockNode::MockNode() : AbstractLQPNode(LQPNodeType::Mock) {}

MockNode::MockNode(const std::shared_ptr<TableStatistics>& statistics) : AbstractLQPNode(LQPNodeType::Mock) {
  set_statistics(statistics);

  for (size_t column_statistics_idx = 0; column_statistics_idx < statistics->column_statistics().size();
       ++column_statistics_idx) {
    _output_column_names.emplace_back("MockCol" + std::to_string(column_statistics_idx));
  }

  _output_column_ids_to_input_column_ids.emplace(output_column_count(), INVALID_COLUMN_ID);
}

const std::vector<std::optional<ColumnID>>& MockNode::output_column_ids_to_input_column_ids() const {
  if (!_output_column_ids_to_input_column_ids) {
    _output_column_ids_to_input_column_ids->emplace(output_column_count());
  }

  return *_output_column_ids_to_input_column_ids;
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
