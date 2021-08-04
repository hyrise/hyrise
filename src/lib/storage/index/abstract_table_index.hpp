#pragma once

#include <memory>
#include <utility>
#include <vector>

#include "all_type_variant.hpp"
#include "segment_index_type.hpp"
#include "types.hpp"

#include "tsl/robin_map.h"

namespace opossum {

/**
 * Basic forward iterator type for iteration over RowIDs, e.g. for table indexes. The default implementation of the
 * virtual properties is meant to represent the iterator of an empty collection. Therefore it shall not be dereferenced
 * (like and end-iterator), increments do not make a change, and equality comparisons with other instances of this class
 * always result in true.
 */
class BaseTableIndexIterator : public std::iterator<std::forward_iterator_tag, const RowID> {
 public:
  virtual ~BaseTableIndexIterator() = default;

  virtual reference operator*() const { throw std::logic_error("cannot dereference on empty iterator"); }
  virtual BaseTableIndexIterator& operator++() { return *this; }
  virtual bool operator==(const BaseTableIndexIterator& other) const { return true; }
  virtual bool operator!=(const BaseTableIndexIterator& other) const { return false; }

  virtual std::shared_ptr<BaseTableIndexIterator> clone() const {
    return std::make_shared<BaseTableIndexIterator>();
  }
};

/**
 * Forward iterator that iterates over a tsl::robin_map which maps a DataType to a vector of RowIDs. The iteration
 * process is as if the map would have been flattened and then been iterated.
 *
 * @tparam DataType The key type of the underlying map.
 */
template <typename DataType>
class TableIndexIterator : public BaseTableIndexIterator {
 public:
  using MapIteratorType = typename tsl::robin_map<DataType, std::vector<RowID>>::const_iterator;

  explicit TableIndexIterator(MapIteratorType itr) : _map_iterator(itr), _vector_index(0) {}

  reference operator*() const override { return _map_iterator->second[_vector_index]; }

  TableIndexIterator& operator++() override {
    if (++_vector_index >= _map_iterator->second.size()) {
      _map_iterator++;
      _vector_index = 0;
    }
    return *this;
  }

  bool operator==(const BaseTableIndexIterator& other) const override {
    auto obj = dynamic_cast<const TableIndexIterator*>(&other);
    return obj && _map_iterator == obj->_map_iterator && _vector_index == obj->_vector_index;
  }

  bool operator!=(const BaseTableIndexIterator& other) const override {
    auto obj = dynamic_cast<const TableIndexIterator*>(&other);
    return !obj || _map_iterator != obj->_map_iterator || _vector_index != obj->_vector_index;
  }

  std::shared_ptr<BaseTableIndexIterator> clone() const override {
    return std::make_shared<TableIndexIterator<DataType>>(*this);
  }

 private:
  MapIteratorType _map_iterator;
  size_t _vector_index;
};

/**
 * Wrapper class that implements an iterator interface and holds a pointer to a BaseTableIndexIterator. This wrapper is
 * required to allow runtime polymorphism without the need to directly pass pointers to iterators throughout the
 * codebase. It also provides copy construction and assignment facilities to easily duplicate other IteratorWrappers
 * including their underlying iterators. This is especially important, because the iterator type is a forward iterator
 * instead of a random access iterator, so if an iterator instance has to be retained before a manipulating call e.g.
 * when calling it on std::distance, a copy has to be made beforehand.
 */
class IteratorWrapper : public std::iterator<std::forward_iterator_tag, const RowID> {
 public:
  explicit IteratorWrapper(std::shared_ptr<BaseTableIndexIterator>&& ptr) : _impl(std::move(ptr)) {}

  IteratorWrapper(const IteratorWrapper& other) : _impl(other._impl->clone()) {}
  IteratorWrapper& operator=(const IteratorWrapper& other) {
    _impl = other._impl->clone();
    return *this;
  }

  reference operator*() const { return _impl->operator*(); }
  IteratorWrapper& operator++() {
    _impl->operator++();
    return *this;
  }
  bool operator==(const IteratorWrapper& other) const { return _impl->operator==(*other._impl); }
  bool operator!=(const IteratorWrapper& other) const { return _impl->operator!=(*other._impl); }

 private:
  std::shared_ptr<BaseTableIndexIterator> _impl;
};

/**
 * This is a concept parallel to chunk based indexes superclass AbstractIndex.
 * It allows indexing multiple chunks of a table column.
 * It is assumed that all table index types support lookup queries, i.e. equals and not-equals queries.
 * The IteratorWrapper type is used as the return type for all query methods.
 */
class AbstractTableIndex : private Noncopyable {
 public:
  using Iterator = IteratorWrapper;
  using IteratorPair = std::pair<Iterator, Iterator>;

  AbstractTableIndex() = delete;
  explicit AbstractTableIndex(const SegmentIndexType type);
  AbstractTableIndex(AbstractTableIndex&&) = default;
  virtual ~AbstractTableIndex() = default;

  /**
   * Searches for all positions of the entry within the table index.
   *
   * Calls _equals() of the most derived class.
   * @param value used to query the index.
   * @return A pair of Iterators containing the start and end iterator for the stored RowIDs of the element inside the table index.
   */
  IteratorPair equals(const AllTypeVariant& value) const;

  /**
   * Searches for all positions that do not equal to the entry in the table index.
   *
   * Calls _not_equals() of the most derived class.
   * @param value used to query the index.
   * @return A pair of IteratorPairs containing two iterator ranges: the range from the beginning of the map until the first occurence of a value equals to the searched entry and the range from the end of the value until the end of the map.
   */
  std::pair<IteratorPair, IteratorPair> not_equals(const AllTypeVariant& value) const;

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

  SegmentIndexType type() const;

  /**
   * Returns the memory consumption of this Index in bytes
   */
  size_t memory_consumption() const;

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
  std::set<ChunkID> get_indexed_chunk_ids() const;

 protected:
  virtual Iterator _cbegin() const = 0;
  virtual Iterator _cend() const = 0;
  virtual Iterator _null_cbegin() const = 0;
  virtual Iterator _null_cend() const = 0;
  virtual IteratorPair _equals(const AllTypeVariant& value) const = 0;
  virtual std::pair<IteratorPair, IteratorPair> _not_equals(const AllTypeVariant& value) const = 0;
  virtual bool _is_index_for(const ColumnID column_id) const = 0;
  virtual std::set<ChunkID> _get_indexed_chunk_ids() const = 0;
  virtual size_t _memory_consumption() const = 0;

 private:
  const SegmentIndexType _type;
};

}  // namespace opossum
