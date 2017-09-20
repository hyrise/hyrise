#include "stored_table_node.hpp"

#include <memory>
#include <string>
#include <vector>

#include "optimizer/table_statistics.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"
#include "types.hpp"

namespace opossum {

StoredTableNode::StoredTableNode(const std::string& table_name, const optional<std::string>& alias)
    : AbstractASTNode(ASTNodeType::StoredTable), _table_name(table_name), _alias(alias) {
  /**
   * Initialize output information.
   */
  auto table = StorageManager::get().get_table(_table_name);
  _output_column_names = table->column_names();

  _output_column_id_to_input_column_id.resize(output_col_count(), INVALID_COLUMN_ID);
}

std::string StoredTableNode::description() const { return "Table: " + _table_name; }

const std::vector<ColumnID>& StoredTableNode::output_column_id_to_input_column_id() const {
  return _output_column_id_to_input_column_id;
}

const std::vector<std::string>& StoredTableNode::output_column_names() const { return _output_column_names; }

const std::shared_ptr<TableStatistics> StoredTableNode::_gather_statistics() const {
  return StorageManager::get().get_table(_table_name)->table_statistics();
}

const std::string& StoredTableNode::table_name() const { return _table_name; }

optional<ColumnID> StoredTableNode::find_column_id_by_named_column_reference(
    const NamedColumnReference& named_column_reference) const {
  if (named_column_reference.table_name && !knows_table(*named_column_reference.table_name)) {
    return nullopt;
  }

  const auto& columns = output_column_names();
  const auto iter = std::find(columns.begin(), columns.end(), named_column_reference.column_name);

  if (iter == columns.end()) {
    return nullopt;
  }

  auto idx = std::distance(columns.begin(), iter);
  return ColumnID{static_cast<ColumnID::base_type>(idx)};
}

bool StoredTableNode::knows_table(const std::string& table_name) const {
  if (_alias) {
    // If this table was given an ALIAS on retrieval, does it match the queried table name?
    // Example: SELECT * FROM T1 AS some_table
    return *_alias == table_name;
  } else {
    return _table_name == table_name;
  }
}

std::vector<ColumnID> StoredTableNode::get_output_column_ids_for_table(const std::string& table_name) const {
  if (!knows_table(table_name)) {
    return {};
  }

  std::vector<ColumnID> column_ids;
  column_ids.reserve(output_col_count());

  for (ColumnID column_id{0}; column_id < column_ids.capacity(); ++column_id) {
    column_ids.emplace_back(column_id);
  }

  return column_ids;
}

void StoredTableNode::_on_child_changed() { Fail("StoredTableNode cannot have children."); }

}  // namespace opossum
