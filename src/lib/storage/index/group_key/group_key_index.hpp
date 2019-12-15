#pragma once

#include <memory>
#include <numeric>
#include <utility>
#include <vector>

#include "storage/index/abstract_index.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

class BaseSegment;
class BaseDictionarySegment;
class GroupKeyIndexTest;

/**
 *
 * The GroupKeyIndex works on a single dictionary compressed segment.
 * Besides the AbstractIndex's positions list containing record positions for NULL values (`_null_positions`),
 * this specialized index uses two additional structures. The first is a positions list containing record positions
 * (ie ChunkOffsets) for non-NULL values in the attribute vector, the second is a structure mapping non-NULL-value-ids
 * to the start offsets in the positions list.
 * Since the AbstractIndex's NULL position list only contains NULL value positions, a structure for mapping
 * NULL-value-ids to offsets isn't needed. 
 *
 * An example structure along with the corresponding dictionary segment might look like this:
 *  +----+-----------+------------+---------+----------------+----------------+
 *  | (i)| Attribute | Dictionary |  Index  | Index Postings | NULL Positions |
 *  |    |  Vector   |            | Offsets | (non-NULL)     | [x²]           |
 *  +----+-----------+------------+---------+----------------+----------------+
 *  |  0 |         6 | apple    ------->  0 ------------>  7 |              0 | ie NULL can be found at i = 0, 5, 6 and 11
 *  |  1 |         4 | charlie  ------->  1 ------------>  8 |              5 |
 *  |  2 |         2 | delta    ------->  3 ----------|    9 |              6 |
 *  |  3 |         3 | frank    ------->  5 --------| |->  2 |             11 |
 *  |  4 |         2 | hotel    ------->  6 ------| |      4 |                |       
 *  |  5 |         6 | inbox    ------->  7 ----| | |--->  3 |                |
 *  |  6 |         6 |            | [x¹]  8 |   | |----->  1 |                |
 *  |  7 |         0 |            |         |   |-------> 10 |                |  
 *  |  8 |         1 |            |         |                |                |
 *  |  9 |         1 |            |         |                |                |
 *  | 10 |         5 |            |         |                |                |
 *  | 11 |         6 |            |         |                |                |  
 *  +----+-----------+------------+---------+----------------+----------------+
 * 
 * NULL is represented in the Attribute Vector by ValueID{dictionary.size()}, i.e., ValueID{6} in this example.
 * x¹: Mark for the ending position.
 * x²: NULL positions are stored in `_null_positions` of the AbstractIndex
 *
 * Find more information about this in our Wiki: https://github.com/hyrise/hyrise/wiki/GroupKey-Index
 */
class GroupKeyIndex : public AbstractIndex {
  friend class GroupKeyIndexTest;

 public:
  /**
   * Predicts the memory consumption in bytes of creating this index.
   * See AbstractIndex::estimate_memory_consumption()
   */
  static size_t estimate_memory_consumption(ChunkOffset row_count, ChunkOffset distinct_count, uint32_t value_bytes);

  GroupKeyIndex() = delete;

  GroupKeyIndex(const GroupKeyIndex&) = delete;
  GroupKeyIndex& operator=(const GroupKeyIndex&) = delete;

  GroupKeyIndex(GroupKeyIndex&&) = default;

  explicit GroupKeyIndex(const std::vector<std::shared_ptr<const BaseSegment>>& segments_to_index);

 private:
  Iterator _lower_bound(const std::vector<AllTypeVariant>& values) const final;

  Iterator _upper_bound(const std::vector<AllTypeVariant>& values) const final;

  Iterator _cbegin() const final;

  Iterator _cend() const final;

  /**
   *
   * @returns an iterator pointing to the the first ChunkOffset in the positions-vector
   * that belongs to a given value-id.
   */
  Iterator _get_positions_iterator_at(ValueID value_id) const;

  std::vector<std::shared_ptr<const BaseSegment>> _get_indexed_segments() const;

  size_t _memory_consumption() const final;

 private:
  const std::shared_ptr<const BaseDictionarySegment> _indexed_segment;
  std::vector<ChunkOffset> _value_start_offsets;  // maps value-ids to offsets in _positions
  std::vector<ChunkOffset> _positions;            // non-NULL record positions in the attribute vector
};
}  // namespace opossum
