#pragma once

#include <memory>
#include <numeric>
#include <utility>
#include <vector>

#include "storage/index/base_index.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

class BaseSegment;
class BaseDictionarySegment;
class GroupKeyIndexTest;

/**
 *
 * The GroupKeyIndex works on a single dictionary compressed segment.
 * It uses two structures, one being a postings list containing record positions (ie ChunkOffsets)
 * in the attribute vector. The other structure is an index offset, mapping value-ids to offsets
 * in the postings list.
 *
 * An example structure along with the corresponding dictionary segment might look like this:
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
 * Find more information about this in our Wiki: https://github.com/hyrise/hyrise/wiki/GroupKey-Index
 */
class GroupKeyIndex : public BaseIndex {
  friend class GroupKeyIndexTest;

 public:
  /**
   * Predicts the memory consumption in bytes of creating this index.
   * See BaseIndex::estimate_memory_consumption()
   */
  static size_t estimate_memory_consumption(ChunkOffset row_count, ChunkOffset distinct_count, uint32_t value_bytes);

  GroupKeyIndex() = delete;

  GroupKeyIndex(const GroupKeyIndex&) = delete;
  GroupKeyIndex& operator=(const GroupKeyIndex&) = delete;

  GroupKeyIndex(GroupKeyIndex&&) = default;
  GroupKeyIndex& operator=(GroupKeyIndex&&) = default;

  explicit GroupKeyIndex(const std::vector<std::shared_ptr<const BaseSegment>>& segments_to_index);

 private:
  Iterator _lower_bound(const std::vector<AllTypeVariant>& values) const final;

  Iterator _upper_bound(const std::vector<AllTypeVariant>& values) const final;

  Iterator _cbegin() const final;

  Iterator _cend() const final;

  /**
   *
   * @returns an iterator pointing to the the first ChunkOffset in the postings-vector
   * that belongs to a given value-id.
   */
  Iterator _get_postings_iterator_at(ValueID value_id) const;

  std::vector<std::shared_ptr<const BaseSegment>> _get_indexed_segments() const;

  size_t _memory_consumption() const final;

 private:
  const std::shared_ptr<const BaseDictionarySegment> _indexed_segments;
  std::vector<std::size_t> _index_offsets;   // maps value-ids to offsets in _index_postings
  std::vector<ChunkOffset> _index_postings;  // records positions in the attribute vector
};
}  // namespace opossum
