#include "abstract_meta_table.hpp"

#include "utils/assert.hpp"

namespace opossum {

bool AbstractMetaTable::can_insert() { return false; }
bool AbstractMetaTable::can_update() { return false; }
bool AbstractMetaTable::can_remove() { return false; }

void AbstractMetaTable::insert(const std::vector<AllTypeVariant>& values) {
  Fail("Cannot insert into " + name());
}
void AbstractMetaTable::update(const AllTypeVariant& key, const std::vector<AllTypeVariant>& values) {
  Fail("Cannot update " + name());
}

void AbstractMetaTable::remove(const AllTypeVariant& key) {
  Fail("Cannot delete from " + name());
}

const TableColumnDefinitions& AbstractMetaTable::column_definitions() const {
	return _column_definitions;
}

}  // namespace opossum
