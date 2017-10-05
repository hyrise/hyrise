#pragma once

#include <exception>
#include <memory>
#include <numeric>
#include <utility>
#include <vector>

#include "storage/base_attribute_vector.hpp"
#include "storage/base_dictionary_column.hpp"
#include "storage/index/base_index.hpp"

#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

class BaseColumn;
class GroupKeyIndexTest;

/**
 *
 * The GroupKeyIndex works on a single dictionary compressed column.
 * It uses two structures, one being a postings list containing record positions (ie ChunkOffsets)
 * in the attribute vector. The other structure is an index offset, mapping value-ids to offsets
 * in the postings list.
 *
 * An example structure along with the corresponding dictionary column might look like this:
 *    +---+-----------+------------+---------+----------------+
 *    |(i)| Attribute | Dictionary |  Index  | Index Postings |
 *    |   |  Vector   |            | Offsets |                |
 *    +---+-----------+------------+---------+----------------+
 *    | 0 |         4 | apple    ------->  0 ------------>  4 |  ie "apple" can be found at i = 4 in the AV
 *    | 1 |         2 | charlie  ------->  1 ------------>  5 |
 *    | 2 |         3 | delta    ------->  3 ---------|     6 |
 *    | 3 |         2 | frank    ------->  5 -------| |-->  1 |
 *    | 4 |         0 | hotel    ------->  6 -----| |       3 |  ie "delta" can be found at i = 1 and 3
 *    | 5 |         1 | inbox    ------->  7 ---| | |---->  2 |
 *    | 6 |         1 |            |         |  | |------>  0 |
 *    | 7 |         5 |            |         |  |-------->  7 |  ie "inbox" can be found at i = 7 in the AV
 *    +---+-----------+------------+---------+----------------+
 *
 * Find more information about this in our Wiki: https://github.com/hyrise/zweirise/wiki/GroupKey-Index
 */
class GroupKeyIndex : public BaseIndex {
  friend class GroupKeyIndexTest;

 public:
  GroupKeyIndex() = delete;

  GroupKeyIndex(const GroupKeyIndex &) = delete;
  GroupKeyIndex &operator=(const GroupKeyIndex &) = delete;

  GroupKeyIndex(GroupKeyIndex &&) = default;
  GroupKeyIndex &operator=(GroupKeyIndex &&) = default;

  explicit GroupKeyIndex(const std::vector<std::shared_ptr<BaseColumn>> index_columns)
      : _index_column(std::dynamic_pointer_cast<BaseDictionaryColumn>(index_columns[0])) {
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

 private:
  Iterator _lower_bound(const std::vector<AllTypeVariant> &values) const final {
    DebugAssert((values.size() == 1), "Group Key Index expects only one input value");

    ValueID value_id = _index_column->lower_bound(*values.begin());
    return _get_postings_iterator_at(value_id);
  };

  Iterator _upper_bound(const std::vector<AllTypeVariant> &values) const final {
    DebugAssert((values.size() == 1), "Group Key Index expects only one input value");

    ValueID value_id = _index_column->upper_bound(*values.begin());
    return _get_postings_iterator_at(value_id);
  };

  Iterator _cbegin() const final { return _index_postings.cbegin(); }

  Iterator _cend() const final { return _index_postings.cend(); }

  /**
   *
   * @returns an iterator pointing to the the first ChunkOffset in the postings-vector
   * that belongs to a given value-id.
   */
  Iterator _get_postings_iterator_at(ValueID value_id) const {
    if (value_id == INVALID_VALUE_ID) return _index_postings.cend();

    // get the start position in the position-vector, ie the offset, by looking up the index_offset at value_id
    auto start_pos = _index_offsets[value_id];

    // get an iterator pointing to start_pos
    auto iter = _index_postings.cbegin();
    std::advance(iter, start_pos);
    return iter;
  }

  std::vector<std::shared_ptr<BaseColumn>> _get_index_columns() const {
    std::vector<std::shared_ptr<BaseColumn>> v = {_index_column};
    return v;
  }

 private:
  const std::shared_ptr<BaseDictionaryColumn> _index_column;
  std::vector<std::size_t> _index_offsets;   // maps value-ids to offsets in _index_postings
  std::vector<ChunkOffset> _index_postings;  // records positions in the attribute vector
};
}  // namespace opossum
