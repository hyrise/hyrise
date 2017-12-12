#include "stored_table_node.hpp"

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "optimizer/table_statistics.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"
#include "types.hpp"

namespace opossum {

StoredTableNode::StoredTableNode(const std::string& table_name)
    : AbstractLQPNode(LQPNodeType::StoredTable), _table_name(table_name) {
  /**
   * Initialize output information.
   */
  auto table = StorageManager::get().get_table(_table_name);
  _output_column_names = table->column_names();

  _output_column_ids_to_input_column_ids.emplace(output_column_count(), INVALID_COLUMN_ID);
}

std::string StoredTableNode::description() const { return "[StoredTable] Name: '" + _table_name + "'"; }

std::shared_ptr<AbstractLQPNode> StoredTableNode::find_table_name_origin(const std::string& table_name) const {
  if (_table_alias) {
    return *_table_alias == table_name ? shared_from_this() : nullptr;
  }

  return table_name == _table_name ? shared_from_this() : nullptr;
}

const std::vector<ColumnID>& StoredTableNode::output_column_ids_to_input_column_ids() const {
  DebugAssert(_output_column_ids_to_input_column_ids, "Not initialized");
  return *_output_column_ids_to_input_column_ids;
}

const std::vector<std::string>& StoredTableNode::output_column_names() const { return _output_column_names; }

std::shared_ptr<TableStatistics> StoredTableNode::derive_statistics_from(
    const std::shared_ptr<AbstractLQPNode>& left_child, const std::shared_ptr<AbstractLQPNode>& right_child) const {
  DebugAssert(!left_child && !right_child, "StoredTableNode must be leaf");
  return StorageManager::get().get_table(_table_name)->table_statistics();
}

const std::string& StoredTableNode::table_name() const { return _table_name; }

std::optional<ColumnID> StoredTableNode::find_column_id_by_named_column_reference(
    const NamedColumnReference& named_column_reference) const {
  if (named_column_reference.table_name && !knows_table(*named_column_reference.table_name)) {
    return std::nullopt;
  }

  const auto& columns = output_column_names();
  const auto iter = std::find(columns.begin(), columns.end(), named_column_reference.column_name);

  if (iter == columns.end()) {
    return std::nullopt;
  }

  auto idx = std::distance(columns.begin(), iter);
  return ColumnID{static_cast<ColumnID::base_type>(idx)};
}

bool StoredTableNode::knows_table(const std::string& table_name) const {
  if (_table_alias) {
    // If this table was given an ALIAS on retrieval, does it match the queried table name?
    // Example: SELECT * FROM T1 AS some_table
    return *_table_alias == table_name;
  } else {
    return _table_name == table_name;
  }
}

std::vector<ColumnID> StoredTableNode::get_output_column_ids_for_table(const std::string& table_name) const {
  if (!knows_table(table_name)) {
    return {};
  }

  if (_table_alias && *_table_alias == table_name) {
    return get_output_column_ids();
  }

  std::vector<ColumnID> column_ids;
  column_ids.reserve(output_column_count());

  for (ColumnID column_id{0}; column_id < column_ids.capacity(); ++column_id) {
    column_ids.emplace_back(column_id);
  }

  return column_ids;
}

std::string StoredTableNode::get_verbose_column_name(ColumnID column_id) const {
  if (_table_alias) {
    return "(" + _table_name + " AS " + *_table_alias + ")." + output_column_names()[column_id];
  }
  return _table_name + "." + output_column_names()[column_id];
}

void StoredTableNode::_on_child_changed() { Fail("StoredTableNode cannot have children."); }

std::optional<NamedColumnReference> StoredTableNode::_resolve_local_column_prefix(const NamedColumnReference& named_column_reference) const {
  if (!reference.table_name) {
    return reference;
  }

  if (_table_alias) {
    if (*reference.table_name != *_table_alias) {
      return std::nullopt;
    }
  } else {
    if (_table_name != *reference.table_name) {
      return std::nullopt;
    }
  }

  auto reference_without_local_alias = reference;
  reference_without_local_alias.table_name = std::nullopt;
  return reference_without_local_alias;
}

}  // namespace opossum
