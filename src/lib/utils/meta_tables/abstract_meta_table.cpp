#include "abstract_meta_table.hpp"

#include "storage/chunk.hpp"
#include "storage/table.hpp"
#include "types.hpp"
#include "utils/assert.hpp"
#include "utils/meta_table_manager.hpp"

namespace hyrise {

AbstractMetaTable::AbstractMetaTable(const TableColumnDefinitions& column_definitions)
    : _column_definitions(column_definitions) {}

std::shared_ptr<Table> AbstractMetaTable::_generate() const {
  auto table = _on_generate();

  if (table->chunk_count()) {
    // Previous chunks were marked as immutable by `Table::append()`.
    table->last_chunk()->set_immutable();
  }
  return table;
}

bool AbstractMetaTable::can_insert() const {
  return false;
}

bool AbstractMetaTable::can_update() const {
  return false;
}

bool AbstractMetaTable::can_delete() const {
  return false;
}

void AbstractMetaTable::_insert(const std::vector<AllTypeVariant>& values) {
  Assert(can_insert(), "Cannot insert into " + MetaTableManager::META_PREFIX + name() + ".");
  _validate_data_types(values);
  _on_insert(values);
}

void AbstractMetaTable::_remove(const std::vector<AllTypeVariant>& values) {
  Assert(can_delete(), "Cannot delete from " + MetaTableManager::META_PREFIX + name() + ".");
  _validate_data_types(values);
  _on_remove(values);
}

void AbstractMetaTable::_update(const std::vector<AllTypeVariant>& selected_values,
                                const std::vector<AllTypeVariant>& update_values) {
  Assert(can_update(), "Cannot update " + MetaTableManager::META_PREFIX + name() + ".");
  _validate_data_types(selected_values);
  _validate_data_types(update_values);
  _on_update(selected_values, update_values);
}

void AbstractMetaTable::_on_insert(const std::vector<AllTypeVariant>& /*values*/) {
  Fail("Cannot insert into " + MetaTableManager::META_PREFIX + name() + ".");
}

void AbstractMetaTable::_on_remove(const std::vector<AllTypeVariant>& /*values*/) {
  Fail("Cannot delete from " + MetaTableManager::META_PREFIX + name() + ".");
}

void AbstractMetaTable::_on_update(const std::vector<AllTypeVariant>& /*selected_values*/,
                                   const std::vector<AllTypeVariant>& /*update_values*/) {
  Fail("Cannot update " + MetaTableManager::META_PREFIX + name() + ".");
}

void AbstractMetaTable::_validate_data_types(const std::vector<AllTypeVariant>& values) const {
  const auto column_count = values.size();
  Assert(column_count == _column_definitions.size(), "Number of values must match column definitions.");

  for (auto column_id = ColumnID{0}; column_id < column_count; ++column_id) {
    const auto value_type = data_type_from_all_type_variant(values[column_id]);
    const auto column_type = _column_definitions[column_id].data_type;
    Assert(value_type == column_type, "Data types must match column definitions.");
  }
}

const TableColumnDefinitions& AbstractMetaTable::column_definitions() const {
  return _column_definitions;
}

}  // namespace hyrise
