#pragma once

#include <memory>
#include <vector>

#include "all_type_variant.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

class BaseColumn;

/**
 * BaseIndex is the abstract super class for all index types, e.g. GroupKeyIndex , CompositeGroupKeyIndex,
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
 * Find more information about this in our wiki: https://github.com/hyrise/zweirise/wiki/BaseIndex and
 *                                               https://github.com/hyrise/zweirise/wiki/Indexes
 **/

class BaseIndex : private Noncopyable {
 public:
  // For now we use an iterator over a vector of chunkoffsets as the GroupKeyIndex works like this
  using Iterator = std::vector<ChunkOffset>::const_iterator;

  /**
   * Creates an index on all given columns. Since all indices are composite indices the order of
   * the provided columns matters. Creating two indices with the same columns, but in different orders
   * leads to very different indices.
   */

  BaseIndex() = default;
  BaseIndex(BaseIndex &&) = default;
  BaseIndex &operator=(BaseIndex &&) = default;
  virtual ~BaseIndex() = default;

  /**
   * Checks whether the given columns are covered by the index. This is the case when the order of the given columns
   * and the columns of the index match, and the given columns are either exactly or a subset of the index columns.
   *
   * For example:
   * We have an index on columns DAB.
   * The index is considered to be applicable for columns D, DA and DAB.
   * The index is NOT considerd to be applicable for columns A, DABC, BAD etc.
   * @return true if the given columns are covered by the index.
   */
  bool is_index_for(const std::vector<std::shared_ptr<BaseColumn>> &columns) const {
    auto index_columns = _get_index_columns();
    if (columns.size() > index_columns.size()) return false;
    if (columns.empty()) return false;

    for (size_t i = 0; i < columns.size(); ++i) {
      if (columns[i] != index_columns[i]) return false;
    }
    return true;
  }

  /**
   * Searches for the first entry within the chunk that is equal or greater than the given values.
   * The number of given values has to be less or equal to number of indexed columns. Additionally
   * the order of values and columns has to match. If less values are provided the search is performed
   * as if all entries of the table are truncated to the columns, that got reference values.
   *
   * Calls _lower_bound() of the most derived class.
   * See also upper_bound()
   * @param values are used to query the index.
   * @return An Iterator on the position of the first element equal or greater then provided values.
   */
  Iterator lower_bound(const std::vector<AllTypeVariant> &values) const {
    DebugAssert((_get_index_columns().size() >= values.size()),
                "BaseIndex: The amount of queried columns has to be less or equal to the number of indexed columns.");

    return _lower_bound(values);
  }

  /**
   * Searches for the first entry within the chunk that is greater than the given values.
   * The number of given values has to be less or equal to number of indexed columns. Additionally
   * the order of values and columns has to match. If less values are provided the search is performed
   * as if all entries of the table are truncated to the columns, that got reference values.
   *
   * Calls _upper_bound() of the most derived class.
   * See also lower_bound()
   * @param values are used to query the index.
   * @return An Iterator on the position of the first element greater then provided values.
   */
  Iterator upper_bound(const std::vector<AllTypeVariant> &values) const {
    DebugAssert((_get_index_columns().size() >= values.size()),
                "BaseIndex: The amount of queried columns has to be less or equal to the number of indexed columns.");

    return _upper_bound(values);
  }

  /**
   * Returns an Iterator to the position of the smallest indexed element. This is useful for range queries
   * with no specified begin.
   * Iterating from cbegin() to cend() will result in a position list with ordered values.
   * Calls _cbegin() of the most derived class.
   * @return an Iterator on the position of first element of the Index.
   */
  Iterator cbegin() const { return _cbegin(); }

  /**
   * Returns an Iterator past the position of the greatest indexed element. This is useful for open
   * end range queries.
   * Iterating from cbegin() to cend() will result in a position list with ordered values.
   * Calls _cend() of the most derived class.
   * @return an Iterator on the end of the index (one after the last element).
   */
  Iterator cend() const { return _cend(); }

 protected:
  /**
   * Seperate the public interface of the index from the interface for programmers implementing own
   * indices. Each method has to fullfill the contract of the corresponding public methods.
   */
  virtual Iterator _lower_bound(const std::vector<AllTypeVariant> &) const = 0;
  virtual Iterator _upper_bound(const std::vector<AllTypeVariant> &) const = 0;
  virtual Iterator _cbegin() const = 0;
  virtual Iterator _cend() const = 0;
  virtual std::vector<std::shared_ptr<BaseColumn>> _get_index_columns() const = 0;
};
}  // namespace opossum
