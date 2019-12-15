#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "storage/index/abstract_index.hpp"
#include "types.hpp"
#include "variable_length_key_store.hpp"

namespace opossum {

class CompositeGroupKeyIndexTest;
class BaseDictionarySegment;

/**
 *
 * The CompositeGroupKey-Index works on an arbitrary number of dictionary compressed segments.
 * It uses three structures:
 *      - a position list containing record positions (ie ChunkOffsets)
 *      - a sorted store containing all unique concatenated keys of the input segments
 *      - an offset list storing where the positions for a certain concatenated key start at
 *        in the position list
 *
 * An example structure along with the corresponding dictionary segment's attribute vectors might look like this:
 *    +---+-----------+     +------------+---------+-----------+
 *    |(i)| AV1 | AV2 |     |Concatenated| Offsets | Positions |
 *    |   |     |     |     |    Key     |         |           |
 *    +---+-----------+     +------------+---------+-----------+
 *    | 0 |  4  |  1  |     |     03     |    0    |     4     | ie key '03' can be found at i = 4 in the AV
 *    | 1 |  2  |  2  |     |     10     |    1    |     6     |
 *    | 2 |  3  |  4  |     |     12     |    2    |     5     |
 *    | 3 |  2  |  2  |     |     22     |    3    |     1     | ie key '22' can be found at i = 1 and 3
 *    | 4 |  0  |  3  |     |     34     |    5    |     3     |
 *    | 5 |  1  |  2  |     |     41     |    6    |     2     |
 *    | 6 |  1  |  0  |     |     51     |    7    |     0     |
 *    | 7 |  5  |  1  |     |            |         |     7     | ie key '51' can be found at i = 7 in the AV
 *    +---+-----------+     +------------+---------+-----------+
 *
 * Find more information about this in our wiki: https://github.com/hyrise/hyrise/wiki/Composite-GroupKey-Index
 */
class CompositeGroupKeyIndex : public AbstractIndex {
  friend class CompositeGroupKeyIndexTest;

 public:
  /**
   * Predicts the memory consumption in bytes of creating this index.
   * See AbstractIndex::estimate_memory_consumption()
   */
  static size_t estimate_memory_consumption(ChunkOffset row_count, ChunkOffset distinct_count, uint32_t value_bytes);

  CompositeGroupKeyIndex(CompositeGroupKeyIndex&&) = default;
  ~CompositeGroupKeyIndex() = default;

  explicit CompositeGroupKeyIndex(const std::vector<std::shared_ptr<const BaseSegment>>& segments_to_index);

 private:
  Iterator _lower_bound(const std::vector<AllTypeVariant>& values) const final;
  Iterator _upper_bound(const std::vector<AllTypeVariant>& values) const final;
  Iterator _cbegin() const final;
  Iterator _cend() const final;
  std::vector<std::shared_ptr<const BaseSegment>> _get_indexed_segments() const final;

  size_t _memory_consumption() const final;

  /**
   * Creates a VariableLengthKey using the values given as parameters.
   *
   * The order of the values matters! The key '14' created from the values '1' and '4'
   * is not the same as the key '41', created from the same values but provided in a different order.
   *
   * We need to provide the information whether the key is used for an upper- or lower-bound
   * search to make sure the key will be mapped to the correct positions in case the last value
   * is not present in the corresponding dictionary segment. Consider the following example:
   *
   *    +------------+---------+-----------+
   *    |Concatenated| Offsets | Positions |
   *    |    Key     |         |           |
   *    +------------+---------+-----------+
   *    |     03     |    0    |     4     |
   *    |     10     |    1    |     6     |
   *    |     12     |    2    |     5     |
   *    |     22     |    3    |     1     |
   *    |     34     |    5    |     3     |
   *    |     41     |    6    |     2     |
   *    |     51     |    7    |     0     |
   *    +------------+---------+-----------+
   *
   * If the search values would correspond to the attribute values '1' and '0', we would
   * want to have the key '10' for a lower-bound-query on the keystore, but use the key '11' for an
   * upper-bound-query. To be able to create the correct search-key, we thus have to provide the
   * information what query the key will be used for.
   *
   * @returns a VariableLengthKey created based on the given values
   */
  VariableLengthKey _create_composite_key(const std::vector<AllTypeVariant>& values, bool is_upper_bound) const;

  /**
   *
   * @returns an iterator pointing to the first ChunkOffset in the position-vector
   * that belongs to the given VariableLengthKey
   */
  Iterator _get_position_iterator_for_key(const VariableLengthKey& key) const;

 private:
  // the segments the index is based on
  std::vector<std::shared_ptr<const BaseDictionarySegment>> _indexed_segments;

  // contains concatenated value-ids
  VariableLengthKeyStore _keys;

  // the start positions within _position_list for every key
  std::vector<ChunkOffset> _key_offsets;

  // contains positions, ie ChunkOffsets, for the concatenated value-ids
  std::vector<ChunkOffset> _position_list;
};
}  // namespace opossum
