#pragma once

#include <algorithm>
#include <memory>
#include <vector>

#include "../types.hpp"
#include "base_column.hpp"
#include "types.hpp"

namespace opossum {

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
 **/

class BaseIndex {
 public:
  // For now we use an iterator over a vector of chunkoffsets as the GroupKeyIndex works like this
  using Iterator = std::vector<ChunkOffset>::const_iterator;

  /**
   * Creates an index on all given columns. Since all indices are composite indices the order of
   * the provided columns matters. Creating two indices with the same columns, but in different orders
   * leads to very different indices.
   */
  explicit BaseIndex(const std::vector<std::shared_ptr<BaseColumn>> &index_columns) : _index_columns(index_columns) {}

  BaseIndex(const BaseIndex &) = delete;
  BaseIndex &operator=(const BaseIndex &) = delete;
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
    if (columns.size() > _index_columns.size()) return false;
    if (columns.empty()) return false;

    for (size_t i = 0; i < columns.size(); ++i) {
      if (columns[i] != _index_columns[i]) return false;
    }
    return true;
  }

  /**
   * Searches for the first entry within the chunk that is equal or greater than the given values.
   * The number of given values has to be less or equal to number of indexed columns. Additionally
   * the order of values and columns has to match. If less values are provided the missing values
   * are filled with values less or equal to the smallest value of the column.
   * Calls _lower_bound() of the most derived class.
   * See also upper_bound()
   * @param values are used to query the index.
   * @return An Iterator on the position of the first element equal or greater then provided values.
   */
  Iterator lower_bound(const std::vector<AllTypeVariant> &values) const { return _lower_bound(values); }

  /**
   * Searches for the first entry within the chunk that is greater than the given values.
   * The number of given values has to be less or equal to number of indexed columns. Additionally
   * the order of values and columns has to match. If less values are provided the missing values
   * are filled with values greater or equal to the largest value of the column.
   * Calls _upper_bound() of the most derived class.
   * See also lower_bound()
   * @param values are used to query the index.
   * @return An Iterator on the position of the first element greater then provided values.
   */
  Iterator upper_bound(const std::vector<AllTypeVariant> &values) const { return _upper_bound(values); }

  /**
   * Returns an Iterator to the position of the smallest indexed element. This is useful for range queries
   * with no specified begin.
   * Iterating from begin() to end() will result in a position list with ordered values.
   * Calls _begin() of the most derived class.
   * @return an Iterator on the position of first element of the Index.
   */
  Iterator begin() const { return _begin(); }

  /**
   * Returns an Iterator past the position of the greatest indexed element. This is useful for open
   * end range queries.
   * Iterating from begin() to end() will result in a position list with ordered values.
   * Calls _end() of the most derived class.
   * @return an Iterator on the end of the index (one after the last element).
   */
  Iterator end() const { return _end(); }

 protected:
  /**
   * Seperate the public interface of the index from the interface for programmers implementing own
   * indices. Each method has to fullfill the contract of the corresponding public methods.
   */
  virtual Iterator _lower_bound(const std::vector<AllTypeVariant> &) const = 0;
  virtual Iterator _upper_bound(const std::vector<AllTypeVariant> &) const = 0;
  virtual Iterator _begin() const = 0;
  virtual Iterator _end() const = 0;

  std::vector<std::shared_ptr<BaseColumn>> _index_columns;
};
}  // namespace opossum
