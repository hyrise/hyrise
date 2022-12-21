#pragma once

#include <memory>
#include <unordered_set>
#include <utility>
#include <vector>

#include "all_type_variant.hpp"
#include "types.hpp"

namespace hyrise {

enum class TableIndexType { PartialHash };

/**
 * Basic forward iterator type for iteration over RowIDs, e.g. for table indexes. The default implementation of the
 * virtual properties is meant to represent the iterator of an empty collection. Therefore it shall not be dereferenced
 * (like an end iterator), increments do not make a change, and equality comparisons with other instances of this
 * class always result in true.
 */
class BaseTableIndexIterator {
 public:
  using iterator_category = std::forward_iterator_tag;
  using value_type = const RowID;
  using difference_type = std::ptrdiff_t;
  using pointer = const RowID*;
  using reference = const RowID&;

  BaseTableIndexIterator(const BaseTableIndexIterator& it) = default;
  BaseTableIndexIterator() = default;
  virtual ~BaseTableIndexIterator() = default;
  virtual reference operator*() const;
  virtual BaseTableIndexIterator& operator++();
  virtual bool operator==(const BaseTableIndexIterator& other) const;
  virtual bool operator!=(const BaseTableIndexIterator& other) const;
  virtual std::shared_ptr<BaseTableIndexIterator> clone() const;
};

/**
 * Wrapper class that implements an iterator interface and holds a pointer to a BaseTableIndexIterator. This wrapper is
 * required to allow runtime polymorphism without the need to directly pass pointers to iterators throughout the
 * codebase. It also provides copy construction and assignment facilities to easily duplicate other IteratorWrappers
 * including their underlying iterators. This is especially important, because the iterator type is a forward iterator
 * instead of a random access iterator, so if an iterator instance has to be retained before a manipulating call, e.g.,
 * when calling it on std::distance, a copy has to be made beforehand.
 */
class IteratorWrapper {
 public:
  using iterator_category = std::forward_iterator_tag;
  using value_type = const RowID;
  using difference_type = std::ptrdiff_t;
  using pointer = const RowID*;
  using reference = const RowID&;

  explicit IteratorWrapper(std::shared_ptr<BaseTableIndexIterator>&& table_index_iterator_ptr);
  IteratorWrapper(const IteratorWrapper& other);
  IteratorWrapper& operator=(const IteratorWrapper& other);
  reference operator*() const;
  IteratorWrapper& operator++();
  bool operator==(const IteratorWrapper& other) const;
  bool operator!=(const IteratorWrapper& other) const;

 private:
  std::shared_ptr<BaseTableIndexIterator> _impl;
};

/**
 * This is a superclass for all table indexes. It allows indexing segments of multiple chunks of a table column. It is
 * assumed that all table index types support equality lookup queries (equals and not-equals queries). The
 * IteratorWrapper type is used as the return type for all lookup types.
 */
class AbstractTableIndex : private Noncopyable {
 public:
  using Iterator = IteratorWrapper;
  using IteratorPair = std::pair<Iterator, Iterator>;

  AbstractTableIndex() = delete;
  explicit AbstractTableIndex(const TableIndexType type);
  AbstractTableIndex(AbstractTableIndex&&) = delete;
  virtual ~AbstractTableIndex() = default;

  /**
   * The following four methods are used to access any table index. Each of them accepts a generic function object that
   * expects a begin and end iterator to the underlying data structure as parameter. When accessing the index with one
   * of these methods, one or two pairs of iterators are passed to the function object. For more information, please
   * refer to the method description.
   * 
   * Unfortunately, it is not possible to locate the implementations of these methods inside the source file because of
   * the separate compilation model. In this case, it is not possible to use explicit template instantiation since
   * otherwise all used function objects would have to be instantiated in abstract_table_index.cpp.
   */

  /**
   * Acquires iterators to the first and last indexed non-NULL elements and passes them to the passed functor.
   * 
   * @param functor is a generic function object accepting two iterators as arguments
  */
  template <typename Functor>
  void access_values_with_iterators(const Functor& functor) const {
    auto lock = std::shared_lock<std::shared_mutex>(_data_access_mutex);
    functor(_cbegin(), _cend());
  }

  /**
   * Acquires iterators to the first and last indexed NULL elements and passes them to the passed functor.
   * 
   * @param functor is a generic function object accepting two iterators as arguments
  */
  template <typename Functor>
  void access_null_values_with_iterators(const Functor& functor) const {
    auto lock = std::shared_lock<std::shared_mutex>(_data_access_mutex);
    functor(_null_cbegin(), _null_cend());
  }

  /**
   * Searches for all positions of the entry within the table index and acquires a pair of Iterators containing the
   * start and end iterator for the stored RowIDs of the element inside the table index. These are then passed to the
   * functor.
   * 
   * @param functor is a generic function object accepting two iterators as arguments
   * @param value is the entry searched for
  */
  template <typename Functor>
  void range_equals_with_iterators(const Functor& functor, const AllTypeVariant& value) const {
    auto lock = std::shared_lock<std::shared_mutex>(_data_access_mutex);
    const auto [index_begin, index_end] = _range_equals(value);
    functor(index_begin, index_end);
  }

  /**
   * Searches for all positions that do not equal to the entry in the table index and acquires a pair of IteratorPairs
   * containing two iterator ranges: the range from the beginning of the map until the first occurence of a value
   * equals to the searched entry and the range from the end of the value until the end of the map. After this, the
   * functor is called twice, each time with one of the pairs.
   * 
   * @param functor is a generic function object accepting two iterators as arguments
   * @param value is the entry searched for
  */
  template <typename Functor>
  void range_not_equals_with_iterators(const Functor& functor, const AllTypeVariant& value) const {
    auto lock = std::shared_lock<std::shared_mutex>(_data_access_mutex);
    const auto [not_equals_range_left, not_equals_range_right] = _range_not_equals(value);
    functor(not_equals_range_left.first, not_equals_range_left.second);
    functor(not_equals_range_right.first, not_equals_range_right.second);
  }

  bool indexed_null_values() const;

  TableIndexType type() const;

  /**
   * Returns the estimated memory usage of this Index in bytes.
   */
  size_t estimate_memory_usage() const;

  /**
   * Checks whether the given column id is covered by the index.
   *
   * @return true if the given column is covered by the index.
   */
  bool is_index_for(const ColumnID column_id) const;

  /**
   * Returns the chunk ids covered by the index.
   *
   * @return An ordered set of the chunk ids.
   */
  std::unordered_set<ChunkID> get_indexed_chunk_ids() const;

  /**
   * Returns the ColumnID covered by the index.
   *
   * @return The ColumnID covered by the index.
   */
  ColumnID get_indexed_column_id() const;

 protected:
  /**
   * Returns an Iterator to the position of the first indexed non-NULL element.
   * Iterating from cbegin() to cend() will result in a position list.
   * Calls _cbegin() of the derived class.
   * @return An Iterator on the position of first non-NULL element of the Index.
   */
  virtual Iterator _cbegin() const = 0;

  /**
   * Returns an Iterator past the position of the last indexed non-NULL element.
   * Iterating from cbegin() to cend() will result in a position list.
   * Calls _cend() of the derived class.
   * @return An Iterator on the end of the non-NULL elements (one after the last element).
   */
  virtual Iterator _cend() const = 0;

  /**
   * Returns an Iterator to the first NULL.
   * Iterating from null_cbegin() to null_cend() will result in a position list with all NULL values.
   *
   * @return An Iterator on the position of the first NULL.
   */
  virtual Iterator _null_cbegin() const = 0;

  /**
   * Returns an Iterator past the position of the last NULL.
   * Iterating from null_cbegin() to null_cend() will result in a position list with all NULL values.
   *
   * @return An Iterator on the end of the NULLs (one after the last NULL).
   */
  virtual Iterator _null_cend() const = 0;

  /**
   * Searches for all positions of the entry within the table index.
   *
   * Calls _equals() of the derived class.
   * @param value used to query the index.
   * @return A pair of Iterators containing the start and end iterator for the stored RowIDs of the element inside the
   * table index.
   */
  virtual IteratorPair _range_equals(const AllTypeVariant& value) const = 0;

  /**
   * Searches for all positions that do not equal to the entry in the table index.
   *
   * Calls _not_equals() of the derived class.
   * @param value used to query the index.
   * @return A pair of IteratorPairs containing two iterator ranges: the range from the beginning of the map until the
   * first occurence of a value equals to the searched entry and the range from the end of the value until the end of
   * the map.
   */
  virtual std::pair<IteratorPair, IteratorPair> _range_not_equals(const AllTypeVariant& value) const = 0;
  virtual bool _is_index_for(const ColumnID column_id) const = 0;
  virtual std::unordered_set<ChunkID> _get_indexed_chunk_ids() const = 0;
  virtual ColumnID _get_indexed_column_id() const = 0;
  virtual size_t _estimate_memory_usage() const = 0;
  mutable std::shared_mutex _data_access_mutex;

 private:
  const TableIndexType _type;
};
}  // namespace hyrise
