#include "abstract_meta_table.hpp"

#include "statistics/table_statistics.hpp"
#include "utils/assert.hpp"

namespace opossum {

const std::shared_ptr<Table> AbstractMetaTable::generate() const {
  const auto table = _on_generate();
  table->last_chunk()->finalize();
  table->set_table_statistics(TableStatistics::from_table(*table));
  return table;
}

bool AbstractMetaTable::can_insert() { return false; }

bool AbstractMetaTable::can_update() { return false; }

bool AbstractMetaTable::can_remove() { return false; }

void AbstractMetaTable::insert(const std::vector<AllTypeVariant>& values) { Fail("Cannot insert into " + name() + "."); }

void AbstractMetaTable::update(const AllTypeVariant& key, const std::vector<AllTypeVariant>& values) {
  Fail("Cannot update " + name() + ".");
}

void AbstractMetaTable::remove(const AllTypeVariant& key) { Fail("Cannot delete from " + name() + "."); }

const TableColumnDefinitions& AbstractMetaTable::column_definitions() const { return _column_definitions; }

}  // namespace opossum
