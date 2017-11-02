#include "dummy_table_node.hpp"

#include <optional>
#include <string>
#include <vector>

#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

DummyTableNode::DummyTableNode() : AbstractASTNode(ASTNodeType::DummyTable) {
  _output_column_ids_to_input_column_ids.emplace();
}

std::string DummyTableNode::description() const { return "[DummyTable]"; }

void DummyTableNode::_on_child_changed() { Fail("DummyTableNode cannot have children."); }

const std::vector<std::string>& DummyTableNode::output_column_names() const { return _output_column_names; }

const std::vector<ColumnID>& DummyTableNode::output_column_ids_to_input_column_ids() const {
  return *_output_column_ids_to_input_column_ids;
}

std::vector<ColumnID> DummyTableNode::get_output_column_ids_for_table(const std::string& table_name) const {
  return {};
}

std::optional<ColumnID> DummyTableNode::find_column_id_by_named_column_reference(
    const NamedColumnReference& named_column_reference) const {
  return {};
}

}  // namespace opossum
