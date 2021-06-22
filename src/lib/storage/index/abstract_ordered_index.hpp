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

class AbstractOrderedIndex : public AbstractIndex<ChunkOffset> {
  friend class GroupKeyIndexTest;

 public:
  // For now we use an iterator over a vector of chunkoffsets as the GroupKeyIndex works like this
  using Iterator = std::vector<ChunkOffset>::const_iterator;

  /**
   * Creates an index on all given segments. Since all indexes are composite indexes the order of
   * the provided segments matters. Creating two indexes with the same segments, but in different orders
   * leads to very different indexes.
   */

  AbstractOrderedIndex() = delete;
  explicit AbstractOrderedIndex(const SegmentIndexType type);
  AbstractOrderedIndex(AbstractOrderedIndex&&) = default;
  virtual ~AbstractOrderedIndex() = default;

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
