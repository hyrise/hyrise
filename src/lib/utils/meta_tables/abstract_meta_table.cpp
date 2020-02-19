#include "abstract_meta_table.hpp"

#include "statistics/table_statistics.hpp"
#include "utils/assert.hpp"
#include "utils/meta_table_manager.hpp"

namespace opossum {

AbstractMetaTable::AbstractMetaTable() {}

const std::shared_ptr<Table> AbstractMetaTable::generate() const {
  const auto table = _on_generate();

  if (table->chunk_count()) {
    table->last_chunk()->finalize();
  }

  table->set_table_statistics(TableStatistics::from_table(*table));

  return table;
}

bool AbstractMetaTable::can_insert() { return false; }

bool AbstractMetaTable::can_update() { return false; }

bool AbstractMetaTable::can_remove() { return false; }

void AbstractMetaTable::insert(const std::vector<AllTypeVariant>& values) {
  _assert_data_types(values);
  _on_insert(values);
}

void AbstractMetaTable::remove(const std::vector<AllTypeVariant>& values) {
  _assert_data_types(values);
  _on_remove(values);
}

void AbstractMetaTable::_on_insert(const std::vector<AllTypeVariant>& values) {
  Fail("Cannot insert into " + MetaTableManager::META_PREFIX + name() + ".");
}

void AbstractMetaTable::_on_remove(const std::vector<AllTypeVariant>& values) {
  Fail("Cannot delete from " + MetaTableManager::META_PREFIX + name() + ".");
}

void AbstractMetaTable::_assert_data_types(const std::vector<AllTypeVariant>& values) const {
  Assert(values.size() == column_definitions().size(), "Number of values must match column definitions.");

  for (size_t i = 0; i < values.size(); i++) {
    const auto& value_type = data_type_from_all_type_variant(values.at(i));
    const auto& column_type = column_definitions().at(i).data_type;
    Assert(value_type == column_type, "Value data types must match column definitions.");
  }
}

const TableColumnDefinitions& AbstractMetaTable::column_definitions() const { return _column_definitions; }

}  // namespace opossum
