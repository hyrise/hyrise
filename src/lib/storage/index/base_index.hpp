#pragma once

#include <memory>
#include <vector>

#include "all_type_variant.hpp"
#include "segment_index_type.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

class BaseSegment;

/**
 * BaseIndex is the abstract super class for all index types, e.g. GroupKeyIndex, CompositeGroupKeyIndex,
 * ARTIndex etc.
 * It is assumed that all index types support range queries and that they are composite indices.
 * I.e. the index is sorted based on the column order. To check whether a key is less than another
 * key, the comparison is performed for the first column. If and only if they are equal, a check is
 * executed for the part of the next column. If needed, this step is repeated for all column
 * parts of both keys.
 * Since all indices have to support range queries, we chose to use iterators as means to get all
 * values that are requested from an index.
 * Further, this allows to use many stl functions like std::transform, std::for_each, std::find ...
 * In addition to that, we save space and time as it is not needed to copy values (compared to returning vectors of
 * values).
 * Instead, each index is able to return an index for the lower and one for the upper bound of the requested value(s).
 * As each index has a different way of iterating over its data structures, it has to implement its iterator as well.
 * We might use the impl-pattern similar to the TableScan, but this will be in a future commit.
 *
 * Find more information about this in our wiki: https://github.com/hyrise/hyrise/wiki/BaseIndex and
 *                                               https://github.com/hyrise/hyrise/wiki/Indexes
 **/

class BaseIndex : private Noncopyable {
 public:
  // For now we use an iterator over a vector of chunkoffsets as the GroupKeyIndex works like this
  using Iterator = std::vector<ChunkOffset>::const_iterator;

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
   * Creates an index on all given segments. Since all indices are composite indices the order of
   * the provided segments matters. Creating two indices with the same segments, but in different orders
   * leads to very different indices.
   */

  BaseIndex() = delete;
  explicit BaseIndex(const SegmentIndexType type);
  BaseIndex(BaseIndex&&) = default;
  BaseIndex& operator=(BaseIndex&&) = default;
  virtual ~BaseIndex() = default;

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
  bool is_index_for(const std::vector<std::shared_ptr<const BaseSegment>>& segments) const;

  /**
   * Searches for the first entry within the chunk that is equal or greater than the given values.
   * The number of given values has to be less or equal to number of indexed segments. Additionally
   * the order of values and segments has to match. If less values are provided the search is performed
   * as if all entries of the table are truncated to the segments that got reference values.
   *
   * Calls _lower_bound() of the most derived class.
   * See also upper_bound()
   * @param values are used to query the index.
   * @return An Iterator on the position of the first element equal or greater then provided values.
   */
  Iterator lower_bound(const std::vector<AllTypeVariant>& values) const;

  /**
   * Searches for the first entry within the chunk that is greater than the given values.
   * The number of given values has to be less or equal to number of indexed segments. Additionally
   * the order of values and segments has to match. If less values are provided the search is performed
   * as if all entries of the table are truncated to the segments that got reference values.
   *
   * Calls _upper_bound() of the most derived class.
   * See also lower_bound()
   * @param values are used to query the index.
   * @return An Iterator on the position of the first element greater then provided values.
   */
  Iterator upper_bound(const std::vector<AllTypeVariant>& values) const;

  /**
   * Returns an Iterator to the position of the smallest indexed element. This is useful for range queries
   * with no specified begin.
   * Iterating from cbegin() to cend() will result in a position list with ordered values.
   * Calls _cbegin() of the most derived class.
   * @return an Iterator on the position of first element of the Index.
   */
  Iterator cbegin() const;

  /**
   * Returns an Iterator past the position of the greatest indexed element. This is useful for open
   * end range queries.
   * Iterating from cbegin() to cend() will result in a position list with ordered values.
   * Calls _cend() of the most derived class.
   * @return an Iterator on the end of the index (one after the last element).
   */
  Iterator cend() const;

  SegmentIndexType type() const;

  /**
   * Returns the memory consumption of this Index in bytes
   */
  size_t memory_consumption() const;

 protected:
  /**
   * Seperate the public interface of the index from the interface for programmers implementing own
   * indices. Each method has to fullfill the contract of the corresponding public methods.
   */
  virtual Iterator _lower_bound(const std::vector<AllTypeVariant>&) const = 0;
  virtual Iterator _upper_bound(const std::vector<AllTypeVariant>&) const = 0;
  virtual Iterator _cbegin() const = 0;
  virtual Iterator _cend() const = 0;
  virtual std::vector<std::shared_ptr<const BaseSegment>> _get_indexed_segments() const = 0;
  virtual size_t _memory_consumption() const = 0;

 private:
  const SegmentIndexType _type;
};
}  // namespace opossum
