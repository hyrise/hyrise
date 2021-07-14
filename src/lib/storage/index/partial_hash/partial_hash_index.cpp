#include "partial_hash_index.hpp"

#include "storage/segment_iterate.hpp"

namespace opossum {

size_t PartialHashIndex::estimate_memory_consumption(ChunkOffset row_count, ChunkOffset distinct_count,
                                                     uint32_t value_bytes) {
  Fail("PartialHashIndex::estimate_memory_consumption() is not implemented yet");
}

PartialHashIndex::PartialHashIndex(const std::vector<std::pair<ChunkID, std::shared_ptr<Chunk>>>& chunks_to_index,
                                   const ColumnID column_id)
    : AbstractTableIndex{get_index_type_of<PartialHashIndex>()} {
  if (!chunks_to_index.empty()) {
    resolve_data_type(
        chunks_to_index.front().second->get_segment(column_id)->data_type(), [&](const auto column_data_type) {
          using ColumnDataType = typename decltype(column_data_type)::type;

          _impl =
              std::make_shared<PartialHashIndexImpl<ColumnDataType>>(chunks_to_index, column_id, _null_positions);
        });
  } else {
    /**
     * Because 'chunks_to_index' is empty, we cannot determine the data type of the column and therefore construct
     * an empty Impl. When chunks are added to this index, it is swapped out again
     */
    _impl = std::make_shared<EmptyPartialHashIndexImpl>();
  }
}

std::pair<PartialHashIndex::Iterator, PartialHashIndex::Iterator> PartialHashIndex::_equals(
    const AllTypeVariant& value) const {
  return _impl->equals(value);
}

PartialHashIndex::Iterator PartialHashIndex::_cbegin() const { return _impl->cbegin(); }

PartialHashIndex::Iterator PartialHashIndex::_cend() const { return _impl->cend(); }

std::vector<std::shared_ptr<const AbstractSegment>> PartialHashIndex::_get_indexed_segments() const {
  return _impl->get_indexed_segments();
}

size_t PartialHashIndex::_memory_consumption() const { return 0; }

bool PartialHashIndex::_is_index_for(const ColumnID column_id) const {
  return _impl->is_index_for(column_id);
};

std::set<ChunkID> PartialHashIndex::_get_indexed_chunk_ids() const {
  return _impl->get_indexed_chunk_ids();
};

}  // namespace opossum
