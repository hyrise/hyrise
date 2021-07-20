#include "abstract_table_index.hpp"

namespace opossum {

AbstractTableIndex::AbstractTableIndex(const SegmentIndexType type) : _type(type) {}

AbstractTableIndex::IteratorPair AbstractTableIndex::equals(const AllTypeVariant& value) const {
  return _equals(value);
}

std::pair<AbstractTableIndex::IteratorPair, AbstractTableIndex::IteratorPair> AbstractTableIndex::not_equals(
    const AllTypeVariant& value) const {
  return _not_equals(value);
}

bool AbstractTableIndex::is_index_for(const ColumnID column_id) const { return _is_index_for(column_id); }

std::set<ChunkID> AbstractTableIndex::get_indexed_chunk_ids() const { return _get_indexed_chunk_ids(); }

}  // namespace opossum
