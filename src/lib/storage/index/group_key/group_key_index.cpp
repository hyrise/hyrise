#include "group_key_index.hpp"

#include <memory>
#include <vector>

#include "storage/base_dictionary_segment.hpp"
#include "storage/index/abstract_index.hpp"
#include "storage/vector_compression/resolve_compressed_vector_type.hpp"

namespace opossum {

size_t GroupKeyIndex::estimate_memory_consumption(ChunkOffset row_count, ChunkOffset distinct_count,
                                                  uint32_t value_bytes) {
  return row_count * sizeof(ChunkOffset) + distinct_count * sizeof(std::size_t);
}

GroupKeyIndex::GroupKeyIndex(const std::vector<std::shared_ptr<const BaseSegment>>& segments_to_index)
    : AbstractIndex{get_index_type_of<GroupKeyIndex>()},
      _indexed_segment(segments_to_index.empty()  // Empty segment list is illegal
                           ? nullptr              // but range check needed for accessing the first segment
                           : std::dynamic_pointer_cast<const BaseDictionarySegment>(segments_to_index[0])) {
  Assert(static_cast<bool>(_indexed_segment), "GroupKeyIndex only works with dictionary segments_to_index.");
  Assert((segments_to_index.size() == 1), "GroupKeyIndex only works with a single segment.");

  // 1) Initialize the index_offset:
  //    Set the index_offset to size of the dictionary + 1 (plus one to mark the ending position)
  //    and set all offsets to 0.
  //    Using `_index_offsets`, we want to count the occurences of each ValueID of the attribute vector (AV).
  //    The ValueID for NULL in an AV is the highest available ValueID in the dictionary + 1.
  //    `unique_values_count` returns the size of dictionary which does not store a ValueID for NULL.
  //    Therefore we have `unique_values_count` ValueIDs (null value id is not included)
  //    for which we want to count the occurrences.
  _index_offsets =
      std::vector<ChunkOffset>(_indexed_segment->unique_values_count() + 1u /*to mark the ending position */, 0u);

  // 2) Count the occurrences of value-ids: Iterate once over the attribute vector (i.e. value ids)
  //    and count the occurrences of each value id at their respective position in the dictionary,
  //    i.e. the position in the _index_offsets
  const auto null_value_id = _indexed_segment->null_value_id();
  auto null_count = 0u;

  resolve_compressed_vector_type(*_indexed_segment->attribute_vector(), [&](auto& attribute_vector) {
    for (const auto value_id : attribute_vector) {
      if (static_cast<ValueID>(value_id) != null_value_id) {
        _index_offsets[value_id + 1u]++;
      } else {
        ++null_count;
      }
    }
  });

  const auto non_null_count = _indexed_segment->size() - null_count;

  // 3) Set the _index_postings and _index_null_positions
  _index_postings = std::vector<ChunkOffset>(non_null_count);
  _index_null_positions = std::vector<ChunkOffset>(null_count);

  // 4) Create offsets for the postings in _index_offsets
  std::partial_sum(_index_offsets.begin(), _index_offsets.end(), _index_offsets.begin());

  // 5) Create the postings
  // 5a) Copy _index_offsets to use it as a write counter
  auto index_offset_copy = std::vector<ChunkOffset>(_index_offsets);

  // 5b) Iterate once again over the attribute vector to obtain the write-offsets
  resolve_compressed_vector_type(*_indexed_segment->attribute_vector(), [&](auto& attribute_vector) {
    auto value_id_iter = attribute_vector.cbegin();
    auto null_positions_iter = _index_null_positions.begin();
    auto position = 0u;
    for (; value_id_iter != attribute_vector.cend(); ++value_id_iter, ++position) {
      const auto& value_id = static_cast<ValueID>(*value_id_iter);

      if (value_id != null_value_id) {
        _index_postings[index_offset_copy[value_id]] = position;
      } else {
        *null_positions_iter = position;
        ++null_positions_iter;
      }

      // increase the write-offset in the copy by one to ensure that further writes
      // are directed to the next position in _index_postings
      index_offset_copy[value_id]++;
    }
  });
}

GroupKeyIndex::Iterator GroupKeyIndex::_lower_bound(const std::vector<AllTypeVariant>& values) const {
  Assert((values.size() == 1), "Group Key Index expects exactly one input value");
  // the caller is responsible for not passing a null value
  Assert(!variant_is_null(values[0]), "Null was passed to lower_bound().");

  ValueID value_id = _indexed_segment->lower_bound(values[0]);
  return _get_postings_iterator_at(value_id);
}

GroupKeyIndex::Iterator GroupKeyIndex::_upper_bound(const std::vector<AllTypeVariant>& values) const {
  Assert((values.size() == 1), "Group Key Index expects exactly one input value");
  // the caller is responsible for not passing a null value
  Assert(!variant_is_null(values[0]), "Null was passed to upper_bound().");

  ValueID value_id = _indexed_segment->upper_bound(values[0]);
  return _get_postings_iterator_at(value_id);
}

GroupKeyIndex::Iterator GroupKeyIndex::_cbegin() const { return _index_postings.cbegin(); }

GroupKeyIndex::Iterator GroupKeyIndex::_cend() const { return _index_postings.cend(); }

/**
   *
   * @returns an iterator pointing to the the first ChunkOffset in the postings-vector
   * that belongs to a given non-null value-id.
   */
GroupKeyIndex::Iterator GroupKeyIndex::_get_postings_iterator_at(ValueID value_id) const {
  if (value_id == INVALID_VALUE_ID) {
    return _cend();
  }

  // get the start position in the position-vector, ie the offset, by looking up the index_offset at value_id
  auto start_pos = _index_offsets[value_id];

  // get an iterator pointing to start_pos
  auto iter = _index_postings.cbegin();
  std::advance(iter, start_pos);
  return iter;
}

std::vector<std::shared_ptr<const BaseSegment>> GroupKeyIndex::_get_indexed_segments() const {
  return {_indexed_segment};
}

size_t GroupKeyIndex::_memory_consumption() const {
  size_t bytes = sizeof(_indexed_segment);
  bytes += sizeof(std::vector<ChunkOffset>);  // _index_offsets
  bytes += sizeof(ChunkOffset) * _index_offsets.capacity();
  bytes += sizeof(std::vector<ChunkOffset>);  // _index_postings
  bytes += sizeof(ChunkOffset) * _index_postings.capacity();
  return bytes;
}

}  // namespace opossum
