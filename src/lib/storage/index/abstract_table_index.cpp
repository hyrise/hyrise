#include "abstract_table_index.hpp"

namespace opossum {

AbstractTableIndex::AbstractTableIndex(const TableIndexType type) : _type(type) {}

AbstractTableIndex::IteratorPair AbstractTableIndex::range_equals(const AllTypeVariant& value) const {
  return _range_equals(value);
}

std::pair<AbstractTableIndex::IteratorPair, AbstractTableIndex::IteratorPair> AbstractTableIndex::range_not_equals(
    const AllTypeVariant& value) const {
  return _range_not_equals(value);
}

AbstractTableIndex::Iterator AbstractTableIndex::cbegin() const { return _cbegin(); }

AbstractTableIndex::Iterator AbstractTableIndex::cend() const { return _cend(); }

AbstractTableIndex::Iterator AbstractTableIndex::null_cbegin() const { return _null_cbegin(); }

AbstractTableIndex::Iterator AbstractTableIndex::null_cend() const { return _null_cend(); }

TableIndexType AbstractTableIndex::type() const { return _type; }

size_t AbstractTableIndex::memory_consumption() const {
  size_t bytes{0u};
  bytes += _memory_consumption();
  bytes += sizeof(_type);
  return bytes;
}

bool AbstractTableIndex::is_index_for(const ColumnID column_id) const { return _is_index_for(column_id); }

std::set<ChunkID> AbstractTableIndex::get_indexed_chunk_ids() const { return _get_indexed_chunk_ids(); }

}  // namespace opossum
