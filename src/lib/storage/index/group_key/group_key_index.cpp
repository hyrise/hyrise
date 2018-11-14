#include "group_key_index.hpp"

#include <memory>
#include <vector>

#include "storage/base_dictionary_segment.hpp"
#include "storage/vector_compression/resolve_compressed_vector_type.hpp"

namespace opossum {

size_t GroupKeyIndex::estimate_memory_consumption(ChunkOffset row_count, ChunkOffset distinct_count,
                                                  uint32_t value_bytes) {
  return row_count * sizeof(ChunkOffset) + distinct_count * sizeof(std::size_t);
}

GroupKeyIndex::GroupKeyIndex(const std::vector<std::shared_ptr<const BaseSegment>>& segments_to_index)
    : BaseIndex{get_index_type_of<GroupKeyIndex>()},
      _indexed_segments(std::dynamic_pointer_cast<const BaseDictionarySegment>(segments_to_index[0])) {
  Assert(static_cast<bool>(_indexed_segments), "GroupKeyIndex only works with dictionary segments_to_index.");
  Assert((segments_to_index.size() == 1), "GroupKeyIndex only works with a single segment.");

  // 1) Initialize the index structures
  // 1a) Set the index_offset to size of the dictionary + 1 (plus one to mark the ending position)
  //     and set all offsets to 0
  _index_offsets = std::vector<size_t>(_indexed_segments->unique_values_count() + 1u, 0u);
  // 1b) Set the _index_postings to the size of the attribute vector
  _index_postings = std::vector<ChunkOffset>(_indexed_segments->size());

  // 2) Count the occurrences of value-ids: Iterate once over the attribute vector (i.e. value ids)
  //    and count the occurrences of each value id at their respective position in the dictionary,
  //    i.e. the position in the _index_offsets
  resolve_compressed_vector_type(*_indexed_segments->attribute_vector(), [&](auto& attribute_vector) {
    for (const auto& value_id : attribute_vector) {
      _index_offsets[value_id + 1u]++;
    }
  });

  // 3) Create offsets for the postings in _index_offsets
  std::partial_sum(_index_offsets.begin(), _index_offsets.end(), _index_offsets.begin());

  // 4) Create the postings
  // 4a) Copy _index_offsets to use it as a write counter
  auto index_offset_copy = std::vector<size_t>(_index_offsets);

  // 4b) Iterate once again over the attribute vector to obtain the write-offsets
  resolve_compressed_vector_type(*_indexed_segments->attribute_vector(), [&](auto& attribute_vector) {
    auto value_id_it = attribute_vector.cbegin();
    auto position = 0u;
    for (; value_id_it != attribute_vector.cend(); ++value_id_it, ++position) {
      const auto& value_id = static_cast<ValueID>(*value_id_it);
      _index_postings[index_offset_copy[value_id]] = position;

      // increase the write-offset in the copy by one to ensure that further writes
      // are directed to the next position in _index_postings
      index_offset_copy[value_id]++;
    }
  });
}

GroupKeyIndex::Iterator GroupKeyIndex::_lower_bound(const std::vector<AllTypeVariant>& values) const {
  DebugAssert((values.size() == 1), "Group Key Index expects only one input value");

  ValueID value_id = _indexed_segments->lower_bound(*values.begin());
  return _get_postings_iterator_at(value_id);
}

GroupKeyIndex::Iterator GroupKeyIndex::_upper_bound(const std::vector<AllTypeVariant>& values) const {
  DebugAssert((values.size() == 1), "Group Key Index expects only one input value");

  ValueID value_id = _indexed_segments->upper_bound(*values.begin());
  return _get_postings_iterator_at(value_id);
}

GroupKeyIndex::Iterator GroupKeyIndex::_cbegin() const { return _index_postings.cbegin(); }

GroupKeyIndex::Iterator GroupKeyIndex::_cend() const { return _index_postings.cend(); }

/**
   *
   * @returns an iterator pointing to the the first ChunkOffset in the postings-vector
   * that belongs to a given value-id.
   */
GroupKeyIndex::Iterator GroupKeyIndex::_get_postings_iterator_at(ValueID value_id) const {
  if (value_id == INVALID_VALUE_ID) return _index_postings.cend();

  // get the start position in the position-vector, ie the offset, by looking up the index_offset at value_id
  auto start_pos = _index_offsets[value_id];

  // get an iterator pointing to start_pos
  auto iter = _index_postings.cbegin();
  std::advance(iter, start_pos);
  return iter;
}

std::vector<std::shared_ptr<const BaseSegment>> GroupKeyIndex::_get_indexed_segments() const {
  return {_indexed_segments};
}

size_t GroupKeyIndex::_memory_consumption() const {
  size_t bytes = sizeof(_indexed_segments);
  bytes += sizeof(std::size_t) * _index_offsets.size();
  bytes += sizeof(ChunkOffset) * _index_postings.size();
  return bytes;
}

}  // namespace opossum
