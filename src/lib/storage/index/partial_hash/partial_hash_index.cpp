#include "partial_hash_index.hpp"

#include "storage/segment_iterate.hpp"

namespace opossum {

size_t PartialHashIndex::estimate_memory_consumption(ChunkOffset row_count, ChunkOffset distinct_count,
                                                     uint32_t value_bytes) {
  Fail("PartialHashIndex::estimate_memory_consumption() is not implemented yet");
}

PartialHashIndex::PartialHashIndex(const std::vector<std::pair<ChunkID, std::shared_ptr<Chunk>>>& chunks_to_index,
                                   const ColumnID column_id)
    : AbstractTableIndex{get_index_type_of<PartialHashIndex>()}, _column_id(column_id) {
  if (!chunks_to_index.empty()) {
    resolve_data_type(chunks_to_index.front().second->get_segment(column_id)->data_type(),
                      [&](const auto column_data_type) {
                        using ColumnDataType = typename decltype(column_data_type)::type;

                        // ToDo(pi) check all have same data type by taking first and comapring rest
                        //ToDo(pi) set null positions
                        for (const auto& chunk : chunks_to_index) {
                          if (_indexed_chunk_ids.contains(chunk.first)) continue;

                          _indexed_chunk_ids.insert(chunk.first);
                          auto indexed_segment = chunk.second->get_segment(column_id);
                          segment_iterate<ColumnDataType>(*indexed_segment, [&](const auto& position) {
                            auto row_id = RowID{chunk.first, position.chunk_offset()};
                            if (position.is_null()) {
                              _null_positions.emplace_back(row_id);
                            } else {
                              if (!_map.contains(position.value())) {
                                _map[position.value()] = std::vector<RowID>();  // ToDo(pi) size
                              }
                              _map[position.value()].push_back(row_id);
                              _row_ids.push_back(row_id);
                            }
                          });
                          _indexed_segments.push_back(indexed_segment);
                        }
                      });
  }
}

void PartialHashIndex::change_indexed_column(const ColumnID column_id) {
  _column_id = column_id;
}


//ToDO(pi) change index (add) chunks later after creation

// ToDo(pi) return from cbegin to cend instead of map vectors
std::pair<PartialHashIndex::Iterator, PartialHashIndex::Iterator> PartialHashIndex::_equals(
    const AllTypeVariant& value) const {
  auto result = _map.find(value);
  if (result == _map.end()) {
    auto end_iter = _cend();
    return std::make_pair(end_iter, end_iter);  // ToDo public or protected member
  }
  return std::make_pair(result->second.begin(), result->second.end());
}

PartialHashIndex::Iterator PartialHashIndex::_cbegin() const { return _row_ids.begin(); }

PartialHashIndex::Iterator PartialHashIndex::_cend() const { return _row_ids.end(); }

std::vector<std::shared_ptr<const AbstractSegment>> PartialHashIndex::_get_indexed_segments() const {
  return _indexed_segments;
}

size_t PartialHashIndex::_memory_consumption() const { return 0; }

}  // namespace opossum
