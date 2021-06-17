#pragma once

#include <memory>
#include <vector>

#include "abstract_index.hpp"
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

//ToDo(pi) rename ordered
class AbstractRangeIndex : public AbstractIndex<ChunkOffset> {
  friend class GroupKeyIndexTest;

 public:
  // For now we use an iterator over a vector of chunkoffsets as the GroupKeyIndex works like this
  using Iterator = std::vector<ChunkOffset>::const_iterator;

  /**
   * Creates an index on all given segments. Since all indexes are composite indexes the order of
   * the provided segments matters. Creating two indexes with the same segments, but in different orders
   * leads to very different indexes.
   */

  AbstractRangeIndex() = delete;
  explicit AbstractRangeIndex(const SegmentIndexType type);
  AbstractRangeIndex(AbstractRangeIndex&&) = default;
  virtual ~AbstractRangeIndex() = default;

  /**
   * Searches for the first entry within the chunk that is equal or greater than the given values.
   * The number of given values has to be less or equal to the number of indexed segments. Additionally,
   * the order of values and segments has to match. If less values are provided, the search is performed
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
   * The number of given values has to be less or equal to number of indexed segments. Additionally,
   * the order of values and segments has to match. If less values are provided, the search is performed
   * as if all entries of the table are truncated to the segments that got reference values.
   *
   * Calls _upper_bound() of the most derived class.
   * See also lower_bound()
   * @param values are used to query the index.
   * @return An Iterator on the position of the first element greater then provided values.
   */
  Iterator upper_bound(const std::vector<AllTypeVariant>& values) const;

 protected:
  /**
   * Seperate the public interface of the index from the interface for programmers implementing own
   * indexes. Each method has to fullfill the contract of the corresponding public methods.
   */
  virtual Iterator _lower_bound(const std::vector<AllTypeVariant>&) const = 0;
  virtual Iterator _upper_bound(const std::vector<AllTypeVariant>&) const = 0;
};
}  // namespace opossum
