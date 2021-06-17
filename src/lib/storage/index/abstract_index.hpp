#pragma once

#include <memory>
#include <vector>

#include "all_type_variant.hpp"
#include "segment_index_type.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

class AbstractSegment;

/**
 * AbstractRangeIndex is the abstract super class for all index types, e.g. GroupKeyIndex, CompositeGroupKeyIndex,
 * ARTIndex etc.
 * It is assumed that all index types support range queries and that they are composite indexes.
 * I.e. the index is sorted based on the column order. To check whether a key is less than another
 * key, the comparison is performed for the first column. If and only if they are equal, a check is
 * executed for the part of the next column. If needed, this step is repeated for all column
 * parts of both keys.
 * Since all indexes have to support range queries, we chose to use iterators as means to get all
 * values that are requested from an index.
 * Further, this allows to use many stl functions like std::transform, std::for_each, std::find ...
 * In addition to that, we save space and time as it is not needed to copy values (compared to returning vectors of
 * values).
 * Instead, each index is able to return an index for the lower and one for the upper bound of the requested value(s).
 * As each index has a different way of iterating over its data structures, it has to implement its iterator as well.
 * We might use the impl-pattern similar to the TableScan, but this will be in a future commit.
 *
 * Find more information about this in our wiki: https://github.com/hyrise/hyrise/wiki/AbstractRangeIndex and
 *                                               https://github.com/hyrise/hyrise/wiki/IndexesAndFilters
 **/

template <typename PositionEntry>
class AbstractIndex : private Noncopyable {

 public:
  // For now we use an iterator over a vector of chunkoffsets as the GroupKeyIndex works like this
  using Iterator = typename std::vector<PositionEntry>::const_iterator;

  /**
   * Predicts the memory consumption in bytes of creating an index with the specific index implementation <type>
   * on a Chunk with the following statistics:
   *
   * row_count - overall number of rows
   * distinct_count - number of distinct values
   * value_bytes - (average) size of a single value in bytes
   *
   * If no prediction is possible (or it is not implemented yet), this shall fail.
   */
  static size_t estimate_memory_consumption(SegmentIndexType type, ChunkOffset row_count, ChunkOffset distinct_count,
                                            uint32_t value_bytes);

  /**
   * Creates an index on all given segments. Since all indexes are composite indexes the order of
   * the provided segments matters. Creating two indexes with the same segments, but in different orders
   * leads to very different indexes.
   */

  AbstractIndex() = delete;
  explicit AbstractIndex(const SegmentIndexType type) : _type{type} {}
  AbstractIndex(AbstractIndex&&) = default;
  virtual ~AbstractIndex() = default;

  /**
   * Checks whether the given segments are covered by the index. This is the case when the order of the given columns
   * and the columns of the index match, and the given segments are either exactly or a subset of the indexed segments.
   *
   * For example:
   * We have an index on columns DAB.
   * The index is considered to be applicable for columns D, DA and DAB.
   * The index is NOT considered to be applicable for columns A, DABC, BAD etc.
   * @return true if the given columns are covered by the index.
   */
  bool is_index_for(const std::vector<std::shared_ptr<const AbstractSegment>>& segments) const {
    auto indexed_segments = _get_indexed_segments();
    if (segments.size() > indexed_segments.size()) return false;
    if (segments.empty()) return false;

    for (size_t i = 0; i < segments.size(); ++i) {
      if (segments[i] != indexed_segments[i]) return false;
    }
    return true;
  }

  /**
   * Returns an Iterator to the position of the smallest indexed non-NULL element. This is useful for range queries
   * with no specified begin.
   * Iterating from cbegin() to cend() will result in a position list with ordered values.
   * Calls _cbegin() of the most derived class.
   * @return An Iterator on the position of first non-NULL element of the Index.
   */
  Iterator cbegin() const { return _cbegin(); }

  /**
   * Returns an Iterator past the position of the largest indexed non-NULL element. This is useful for open
   * end range queries.
   * Iterating from cbegin() to cend() will result in a position list with ordered values.
   * Calls _cend() of the most derived class.
   * @return An Iterator on the end of the non-NULL elements (one after the last element).
   */
  Iterator cend() const { return _cend(); }

  /**
   * Returns an Iterator to the first NULL.
   * Iterating from null_cbegin() to null_cend() will result in a position list with all NULL values.
   * NULL handing is currently only supported for single-column indexes.
   * We do not have a concept for multi-column NULL handling yet. #1818
   *
   * @return An Iterator on the position of the first NULL.
   */
  Iterator null_cbegin() const { return _null_positions.cbegin(); }

  /**
   * Returns an Iterator past the position of the last NULL.
   * Iterating from null_cbegin() to null_cend() will result in a position list with all NULL values.
   * NULL handing is currently only supported for single-column indexes.
   * We do not have a concept for multi-column NULL handling yet. #1818
   *
   * @return An Iterator on the end of the NULLs (one after the last NULL).
   */
  Iterator null_cend() const { return _null_positions.cend(); }

  SegmentIndexType type() const { return _type; }

  /**
   * Returns the memory consumption of this Index in bytes
   */
  size_t memory_consumption() const {
    size_t bytes{0u};
    bytes += _memory_consumption();
    bytes += sizeof(std::vector<ChunkOffset>);  // _null_positions
    bytes += sizeof(ChunkOffset) * _null_positions.capacity();
    bytes += sizeof(_type);
    return bytes;
  }

 protected:
  /**
   * Seperate the public interface of the index from the interface for programmers implementing own
   * indexes. Each method has to fullfill the contract of the corresponding public methods.
   */
  virtual Iterator _cbegin() const = 0;
  virtual Iterator _cend() const = 0;
  virtual std::vector<std::shared_ptr<const AbstractSegment>> _get_indexed_segments() const = 0;
  virtual size_t _memory_consumption() const = 0;
  std::vector<PositionEntry> _null_positions;

 private:
  const SegmentIndexType _type;
};
}  // namespace opossum
