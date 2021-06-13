#include "partial_hash_index.hpp"

#include "storage/segment_iterate.hpp"

namespace opossum {

size_t PartialHashIndex::estimate_memory_consumption(ChunkOffset row_count, ChunkOffset distinct_count,
                                               uint32_t value_bytes) {
  Fail("PartialHashIndex::estimate_memory_consumption() is not implemented yet");
}

PartialHashIndex::PartialHashIndex(const std::shared_ptr<Table> referenced_table, const std::vector<ChunkID>& chunk_ids_to_index, const ColumnID column_id)
    : AbstractIndex{get_index_type_of<PartialHashIndex>()} {

  resolve_data_type(referenced_table->get_chunk(chunk_ids_to_index.front())->get_segment(column_id)->data_type(), [&](const auto column_data_type) {
    using ColumnDataType = typename decltype(column_data_type)::type;

    // ToDo(pi) check all have same data type by taking first and comapring rest
    for(const auto&chunk_id:chunk_ids_to_index){
      auto indexed_segment = referenced_table->get_chunk(chunk_id)->get_segment(column_id);
      segment_iterate<ColumnDataType>(*indexed_segment, [&](const auto& position) {
        if(!_map.contains(position.value())){
          _map[position.value()] = std::vector<RowID>(); // ToDo(pi) size
        }
        _map[position.value()].push_back(RowID{chunk_id, position.chunk_offset()});
      });
      _indexed_segments.push_back(indexed_segment);
    }
  });
}

PartialHashIndex::Iterator PartialHashIndex::equal(const AllTypeVariant& value) const {
  //ToDo(pi) implement
  return cbegin();
}
PartialHashIndex::Iterator PartialHashIndex::_cbegin() const {
  return null_cbegin();
}

PartialHashIndex::Iterator PartialHashIndex::_cend() const {
  return null_cend();
}

std::vector<std::shared_ptr<const AbstractSegment>> PartialHashIndex::_get_indexed_segments() const {
  return _indexed_segments;
}

size_t PartialHashIndex::_memory_consumption() const {
  return 0;
}

} // namespace opossum
