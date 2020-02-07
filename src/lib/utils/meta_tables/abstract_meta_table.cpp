#include "abstract_meta_table.hpp"

#include "statistics/table_statistics.hpp"
#include "utils/assert.hpp"
#include "utils/meta_table_manager.hpp"

namespace opossum {

AbstractMetaTable::AbstractMetaTable(const TableColumnDefinitions& column_definitions)
    : _column_definitions(column_definitions) {}

const std::shared_ptr<Table> AbstractMetaTable::generate() const {
  const auto table = _on_generate();

  for (ChunkID chunk_id{0}; chunk_id < table->chunk_count(); chunk_id++) {
    table->get_chunk(chunk_id)->finalize();
  }

  table->set_table_statistics(TableStatistics::from_table(*table));

  return table;
}

bool AbstractMetaTable::can_insert() { return false; }

bool AbstractMetaTable::can_update() { return false; }

bool AbstractMetaTable::can_remove() { return false; }

void AbstractMetaTable::_insert(const std::vector<AllTypeVariant>& values) {
  Fail("Cannot insert into " + MetaTableManager::META_PREFIX + name() + ".");
}

void AbstractMetaTable::_update(const AllTypeVariant& key, const std::vector<AllTypeVariant>& values) {
  Fail("Cannot update " + MetaTableManager::META_PREFIX + name() + ".");
}

void AbstractMetaTable::_remove(const AllTypeVariant& key) {
  Fail("Cannot delete from " + MetaTableManager::META_PREFIX + name() + ".");
}

const TableColumnDefinitions& AbstractMetaTable::column_definitions() const { return _column_definitions; }

}  // namespace opossum
