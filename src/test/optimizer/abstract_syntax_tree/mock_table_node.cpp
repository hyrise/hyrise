#include "mock_table_node.hpp"

#include "optimizer/table_statistics.hpp"
#include "utils/assert.hpp"

namespace opossum {

MockTableNode::MockTableNode(const std::shared_ptr<TableStatistics> & statistics):
  AbstractASTNode(ASTNodeType::Mock)
{
  set_statistics(statistics);

  for (size_t column_statistics_idx = 0;
       column_statistics_idx < statistics->column_statistics().size();
       ++column_statistics_idx) {
    _output_column_names.emplace_back("MockCol" + std::to_string(column_statistics_idx));
  }

  _output_column_id_to_input_column_id.resize(output_col_count(), INVALID_COLUMN_ID);
}

const std::vector<ColumnID>& MockTableNode::output_column_id_to_input_column_id() const {
  return _output_column_id_to_input_column_id;
}

const std::vector<std::string>& MockTableNode::output_column_names() const { return _output_column_names; }

optional<ColumnID> MockTableNode::find_column_id_by_named_column_reference(
  const NamedColumnReference& named_column_reference) const {
  Fail("Operation not supported on MockTableNodes");
  return INVALID_COLUMN_ID;
}

bool MockTableNode::knows_table(const std::string& table_name) const {
  Fail("Operation not supported on MockTableNodes");
  return false;
}

std::vector<ColumnID> MockTableNode::get_output_column_ids_for_table(const std::string& table_name) const {
  Fail("Operation not supported on MockTableNodes");
  return {};
}

void MockTableNode::_on_child_changed() { Fail("MockTableNode cannot have children."); }

std::string MockTableNode::description() const {
  return "MockTable";
}

}