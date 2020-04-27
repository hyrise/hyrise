#include "abstract_meta_table.hpp"

#include "statistics/table_statistics.hpp"
#include "utils/assert.hpp"
#include "utils/meta_table_manager.hpp"

namespace opossum {

AbstractMetaTable::AbstractMetaTable(const TableColumnDefinitions& column_definitions)
    : _column_definitions(column_definitions) {}

std::shared_ptr<Table> AbstractMetaTable::_generate() const {
  const auto table = _on_generate();

  if (table->chunk_count()) {
    table->last_chunk()->finalize();
  }

  table->set_table_statistics(TableStatistics::from_table(*table));

  return table;
}

bool AbstractMetaTable::can_insert() const { return false; }

bool AbstractMetaTable::can_update() const { return false; }

bool AbstractMetaTable::can_delete() const { return false; }

void AbstractMetaTable::_insert(const std::vector<AllTypeVariant>& values) {
  Assert(can_insert(), "Cannot insert into " + name() + ".");
  _validate_data_types(values);
  _on_insert(values);
}

void AbstractMetaTable::_remove(const std::vector<AllTypeVariant>& values) {
  Assert(can_delete(), "Cannot delete from " + name() + ".");
  _validate_data_types(values);
  _on_remove(values);
}

void AbstractMetaTable::_update(const std::vector<AllTypeVariant>& selected_values,
                                const std::vector<AllTypeVariant>& update_values) {
  Assert(can_update(), "Cannot update " + name() + ".");
  _validate_data_types(selected_values);
  _validate_data_types(update_values);
  _on_update(selected_values, update_values);
}

void AbstractMetaTable::_on_insert(const std::vector<AllTypeVariant>& values) {
  Fail("Cannot insert into " + MetaTableManager::META_PREFIX + name() + ".");
}

void AbstractMetaTable::_on_remove(const std::vector<AllTypeVariant>& values) {
  Fail("Cannot delete from " + MetaTableManager::META_PREFIX + name() + ".");
}

void AbstractMetaTable::_on_update(const std::vector<AllTypeVariant>& selected_values,
                                   const std::vector<AllTypeVariant>& update_values) {
  Fail("Cannot update " + MetaTableManager::META_PREFIX + name() + ".");
}

void AbstractMetaTable::_validate_data_types(const std::vector<AllTypeVariant>& values) const {
  Assert(values.size() == column_definitions().size(), "Number of values must match column definitions.");

  for (size_t column = 0; column < values.size(); column++) {
    const auto value_type = data_type_from_all_type_variant(values[column]);
    const auto column_type = column_definitions()[column].data_type;
    Assert(value_type == column_type, "Data types must match column definitions.");
  }
}

const TableColumnDefinitions& AbstractMetaTable::column_definitions() const { return _column_definitions; }

}  // namespace opossum
