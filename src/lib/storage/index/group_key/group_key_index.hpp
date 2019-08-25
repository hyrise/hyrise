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
 * It uses three structures, one being a postings list containing record positions (ie ChunkOffsets)
 * for non-null values in the attribute vector, the second is a postings list containing record positions
 * for null values and the third structure is an index offset, mapping value-ids to offsets
 * in the postings list.
 * For the offset structure, the posting structures are considered as one overall structure with null postings
 * at the end. 
 * For null values, the `_index_null_postings` vector of AbstractIndex is used for storing the record positions.
 * Since this structure only contains null value positions, a further structure for mapping value-ids to
 * offsets isn't needed. 
 *
 * An example structure along with the corresponding dictionary segment might look like this:
 *  +----+-----------+------------+---------+----------------+----------------+
 *  | (i)| Attribute | Dictionary |  Index  | Index Postings | Index Postings |
 *  |    |  Vector   |            | Offsets | (non-null)     | (null)         |
 *  +----+-----------+------------+---------+----------------+----------------+
 *  |  0 |         6 | apple    ------->  0 ------------>  7 |  |-------|>  0 | ie null can be found at i = 0, 5, 6 and 11
 *  |  1 |         4 | charlie  ------->  1 ------------>  8 |  |       |>  5 |
 *  |  2 |         2 | delta    ------->  3 ----------|    9 |  |       |>  6 |
 *  |  3 |         3 | frank    ------->  5 --------| |->  2 |  |       |> 11 |
 *  |  4 |         2 | hotel    ------->  6 ------| |      4 |  |             |                 
 *  |  5 |         6 | inbox    ------->  7 ----| | |--->  3 |  |             |
 *  |  6 |         6 |            | (x¹)  8 --| | |----->  1 |  |             |
 *  |  7 |         0 |            | (x²) 12 | | |-------> 10 |  |             |  
 *  |  8 |         1 |            |         | |-----------------|             |
 *  |  9 |         1 |            |         |                |                |
 *  | 10 |         5 |            |         |                |                |
 *  | 11 |         6 |            |         |                |                |  
 *  +----+-----------+------------+---------+----------------+----------------+
 * 
 * Null is represented in the Attribute Vector by a ValueID which is the highest available
 * ValueID in the dictionary + 1, i.e. ValueID{6} in this example.
 * x¹: Starting offset for null values in the null postings list can be calculated by 
 *     the offset minus the number of non-null values in the segment to index.
 * x²: Mark for the ending position.
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
  const std::shared_ptr<const BaseDictionarySegment> _indexed_segment;
  std::vector<ChunkOffset> _index_offsets;   // maps value-ids to offsets in _index_postings
  std::vector<ChunkOffset> _index_postings;  // non-null record positions in the attribute vector
};
}  // namespace opossum
