#include "mock_node.hpp"

#include <memory>
#include <string>
#include <vector>

#include "optimizer/table_statistics.hpp"
#include "utils/assert.hpp"

namespace opossum {

MockNode::MockNode() : AbstractASTNode(ASTNodeType::Mock) {}

MockNode::MockNode(const std::shared_ptr<TableStatistics>& statistics) : AbstractASTNode(ASTNodeType::Mock) {
  set_statistics(statistics);

  for (size_t column_statistics_idx = 0; column_statistics_idx < statistics->column_statistics().size();
       ++column_statistics_idx) {
    _output_column_names.emplace_back("MockCol" + std::to_string(column_statistics_idx));
  }

  _output_column_id_to_input_column_id.resize(output_column_count(), INVALID_COLUMN_ID);
}

const std::vector<ColumnID>& MockNode::output_column_id_to_input_column_id() const {
  return _output_column_id_to_input_column_id;
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
