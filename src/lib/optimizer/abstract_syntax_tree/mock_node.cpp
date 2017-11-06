#include "mock_node.hpp"

#include <memory>
#include <string>
#include <vector>

#include "optimizer/table_statistics.hpp"
#include "utils/assert.hpp"

namespace opossum {

MockNode::MockNode() : AbstractASTNode(ASTNodeType::Mock) {}

MockNode::MockNode(const std::string& name, size_t column_count) : AbstractASTNode(ASTNodeType::Mock), _name(name) {
  for (size_t column_idx = 0; column_idx < column_count; ++column_idx) {
    _output_column_names.emplace_back("MockCol" + std::to_string(column_idx));
  }

  _output_column_ids_to_input_column_ids.emplace(output_column_count(), INVALID_COLUMN_ID);
}

MockNode::MockNode(const std::shared_ptr<TableStatistics>& statistics, const std::string& name)
    : AbstractASTNode(ASTNodeType::Mock), _name(name) {
  set_statistics(statistics);

  for (size_t column_statistics_idx = 0; column_statistics_idx < statistics->column_statistics().size();
       ++column_statistics_idx) {
    _output_column_names.emplace_back("MockCol" + std::to_string(column_statistics_idx));
  }

  _output_column_ids_to_input_column_ids.emplace(output_column_count(), INVALID_COLUMN_ID);
}

const std::vector<ColumnID>& MockNode::output_column_ids_to_input_column_ids() const {
  DebugAssert(_output_column_ids_to_input_column_ids, "Invalid operation");
  return *_output_column_ids_to_input_column_ids;
}

const std::vector<std::string>& MockNode::output_column_names() const { return _output_column_names; }

std::string MockNode::get_qualified_column_name(ColumnID column_id) const {
  // Aliasing a MockNode doesn't really make sense, but let's stay covered
  if (_table_alias) {
    return *_table_alias + "." + output_column_names()[column_id];
  }
  return output_column_names()[column_id];
}

std::string MockNode::description(DescriptionMode mode) const { return "[MockTable]"; }
}  // namespace opossum
