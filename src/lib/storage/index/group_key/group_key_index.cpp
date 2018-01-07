#include "group_key_index.hpp"

#include <memory>
#include <vector>

namespace opossum {

GroupKeyIndex::GroupKeyIndex(const std::vector<std::shared_ptr<const BaseColumn>> index_columns)
    : BaseIndex{get_index_type_of<GroupKeyIndex>()},
      _index_column(std::dynamic_pointer_cast<const BaseDeprecatedDictionaryColumn>(index_columns[0])) {
  DebugAssert(static_cast<bool>(_index_column), "GroupKeyIndex only works with DictionaryColumns");
  DebugAssert((index_columns.size() == 1), "GroupKeyIndex only works with a single column");

  // 1) Initialize the index structures
  // 1a) Set the index_offset to size of the dictionary + 1 (plus one to mark the ending position) and set all offsets
  // to 0
  _index_offsets = std::vector<size_t>(_index_column->unique_values_count() + 1, 0);
  // 1b) Set the _index_postings to the size of the attribute vector
  _index_postings = std::vector<ChunkOffset>(_index_column->size());

  // 2) Count the occurrences of value-ids: Iterate once over the attribute vector (ie value ids) and count the
  // occurrences of each value id at their respective position in the dictionary, ie the position in the
  // _index_offsets
  for (ChunkOffset offset = 0; offset < _index_column->size(); ++offset) {
    auto value_id = _index_column->attribute_vector()->get(offset);
    _index_offsets[value_id + 1]++;
  }

  // 3) Create offsets for the postings in _index_offsets
  std::partial_sum(_index_offsets.begin(), _index_offsets.end(), _index_offsets.begin());

  // 4) Create the postings
  // 4a) Copy _index_offsets to use it as a write counter
  auto index_offset_copy = std::vector<size_t>(_index_offsets);

  // 4b) Iterate once again over the attribute vector to obtain the write-offsets
  for (ChunkOffset pos = 0; pos < _index_column->size(); ++pos) {
    auto value_id = _index_column->attribute_vector()->get(pos);
    _index_postings[index_offset_copy[value_id]] = pos;

    // increase the write-offset in the copy by one to assure that further writes are directed to the next position in
    // _index_postings
    index_offset_copy[value_id]++;
  }
}

GroupKeyIndex::Iterator GroupKeyIndex::_lower_bound(const std::vector<AllTypeVariant>& values) const {
  DebugAssert((values.size() == 1), "Group Key Index expects only one input value");

  ValueID value_id = _index_column->lower_bound(*values.begin());
  return _get_postings_iterator_at(value_id);
}

GroupKeyIndex::Iterator GroupKeyIndex::_upper_bound(const std::vector<AllTypeVariant>& values) const {
  DebugAssert((values.size() == 1), "Group Key Index expects only one input value");

  ValueID value_id = _index_column->upper_bound(*values.begin());
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

std::vector<std::shared_ptr<const BaseColumn>> GroupKeyIndex::_get_index_columns() const { return {_index_column}; }

}  // namespace opossum
