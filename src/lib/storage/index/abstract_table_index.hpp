#pragma once

#include <memory>
#include <unordered_set>
#include <utility>
#include <vector>

#include "all_type_variant.hpp"
#include "table_index_type.hpp"
#include "types.hpp"

namespace opossum {

/**
 * Basic forward iterator type for iteration over RowIDs, e.g. for table indexes. The default implementation of the
 * virtual properties is meant to represent the iterator of an empty collection. Therefore it shall not be dereferenced
 * (like and end-iterator), increments do not make a change, and equality comparisons with other instances of this
 * class always result in true.
 */
class BaseTableIndexIterator {
 public:
  using iterator_category = std::forward_iterator_tag;
  using value_type = const RowID;
  using difference_type = std::ptrdiff_t;
  using pointer = const RowID*;
  using reference = const RowID&;

  BaseTableIndexIterator(const BaseTableIndexIterator& itr) = default;
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
 * This is a concept parallel to chunk based indexes' superclass AbstractChunkIndex.
 * It allows indexing multiple chunks of a table column.
 * It is assumed that all table index types support equality lookup queries (equals and not-equals queries).
 * The IteratorWrapper type is used as the return type for all lookup types.
 */
class AbstractTableIndex : private Noncopyable {
 public:
  using Iterator = IteratorWrapper;
  using IteratorPair = std::pair<Iterator, Iterator>;

  AbstractTableIndex() = delete;
  explicit AbstractTableIndex(const TableIndexType type);
  AbstractTableIndex(AbstractTableIndex&&) = default;
  virtual ~AbstractTableIndex() = default;

  /**
   * Searches for all positions of the entry within the table index.
   *
   * Calls _equals() of the most derived class.
   * @param value used to query the index.
   * @return A pair of Iterators containing the start and end iterator for the stored RowIDs of the element inside the
   * table index.
   */
  IteratorPair range_equals(const AllTypeVariant& value) const;

  /**
   * Searches for all positions that do not equal to the entry in the table index.
   *
   * Calls _not_equals() of the most derived class.
   * @param value used to query the index.
   * @return A pair of IteratorPairs containing two iterator ranges: the range from the beginning of the map until the
   * first occurence of a value equals to the searched entry and the range from the end of the value until the end of
   * the map.
   */
  std::pair<IteratorPair, IteratorPair> range_not_equals(const AllTypeVariant& value) const;

  /**
   * Returns an Iterator to the position of the first indexed non-NULL element.
   * Iterating from cbegin() to cend() will result in a position list.
   * Calls _cbegin() of the most derived class.
   * @return An Iterator on the position of first non-NULL element of the Index.
   */
  Iterator cbegin() const;

  /**
   * Returns an Iterator past the position of the last indexed non-NULL element.
   * Iterating from cbegin() to cend() will result in a position list.
   * Calls _cend() of the most derived class.
   * @return An Iterator on the end of the non-NULL elements (one after the last element).
   */
  Iterator cend() const;

  /**
   * Returns an Iterator to the first NULL.
   * Iterating from null_cbegin() to null_cend() will result in a position list with all NULL values.
   *
   * @return An Iterator on the position of the first NULL.
   */
  Iterator null_cbegin() const;

  /**
   * Returns an Iterator past the position of the last NULL.
   * Iterating from null_cbegin() to null_cend() will result in a position list with all NULL values.
   *
   * @return An Iterator on the end of the NULLs (one after the last NULL).
   */
  Iterator null_cend() const;

  TableIndexType type() const;

  /**
   * Returns the memory usage of this Index in bytes.
   */
  size_t memory_usage() const;

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

 protected:
  virtual Iterator _cbegin() const = 0;
  virtual Iterator _cend() const = 0;
  virtual Iterator _null_cbegin() const = 0;
  virtual Iterator _null_cend() const = 0;
  virtual IteratorPair _range_equals(const AllTypeVariant& value) const = 0;
  virtual std::pair<IteratorPair, IteratorPair> _range_not_equals(const AllTypeVariant& value) const = 0;
  virtual bool _is_index_for(const ColumnID column_id) const = 0;
  virtual std::unordered_set<ChunkID> _get_indexed_chunk_ids() const = 0;
  virtual size_t _memory_usage() const = 0;

 private:
  const TableIndexType _type;
};

}  // namespace opossum
