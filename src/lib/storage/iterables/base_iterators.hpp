#pragma once

#include <boost/iterator/iterator_facade.hpp>

#include <vector>
#include <utility>

#include "column_value.hpp"

#include "types.hpp"


namespace opossum {

// See table_scan.cpp:170 about how to generate such a list from a reference column
using ChunkOffsetsList = std::vector<std::pair<ChunkOffset, ChunkOffset>>;

using ChunkOffsetsIterator = ChunkOffsetsList::const_iterator;

/**
 * @brief base class of all iterators used by iterables
 *
 * Example Usage
 *
 * class Iterator : public BaseIterator<Iterator, Value> {
 *  private:
 *   friend class boost::iterator_core_access;  // the following methods need to be accessible by the base class
 *  
 *   void increment() { ... }
 *   bool equal(const Iterator& other) const { return false; }
 *   Value dereference() const { return Value{}; }
 * };
 */
template <typename Derived, typename Value>
using BaseIterator = boost::iterator_facade<Derived, Value, boost::forward_traversal_tag, Value>;

/**
 * @brief base class of all referenced iterators used by iterables
 * 
 * This iterator should be used whenever a reference column is “dereferenced”,
 * i.e., its underlying value or dictionary column is iterated over.
 * index_into_referenced is the index into the underlying data structure.
 * index_of_referencing is the current index in the reference column.
 *
 * Example Usage
 *
 * class Iterator : public BaseIndexedIterator<Iterator, Value> {
 *  public:
 *   Iterator(const ChunkOffsetIterator& chunk_offset_it)
 *       : BaseIndexedIterator<Iterator, Value>{chunk_offset_it} {}
 *
 *  private:
 *   friend class boost::iterator_core_access;  // the following methods need to be accessible by the base class
 *
 *   // don’t forget to check if index_into_referenced() == INVALID_CHUNK_OFFSET (i.e. NULL)
 *   Value dereference() const { return Value{}; }
 * };
 */
template <typename Derived, typename Value>
class BaseIndexedIterator : public BaseIterator<Derived, Value> {
 public:
  explicit BaseIndexedIterator(const ChunkOffsetsIterator& chunk_offsets_it) : _chunk_offsets_it{chunk_offsets_it} {}

 protected:
  /**
   * @return index / chunk offset of referencing column
   */
  const ChunkOffset& index_of_referencing() const { return _chunk_offsets_it->first; }

  /**
   * @return index / chunk offset into referenced column
   */
  const ChunkOffset& index_into_referenced() const { return _chunk_offsets_it->second; }

 private:
  friend class boost::iterator_core_access;

  void increment() { ++_chunk_offsets_it; }
  bool equal(const BaseIndexedIterator& other) const { return (_chunk_offsets_it == other._chunk_offsets_it); }

 private:
  ChunkOffsetsIterator _chunk_offsets_it;
};

}  // namespace opossum
