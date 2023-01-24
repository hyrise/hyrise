#pragma once

#include "flat_map_iterator.hpp"
#include "partial_hash_index_impl.hpp"

namespace hyrise {

/**
 * Represents a table index using a hash map where all hashed values are mapped to the RowIDs of their occurrences in
 * the original Chunks. It can be constructed for a set of chunks of a table column and can later be modified by adding
 * additional chunks or removing already indexed chunks.
 */
class PartialHashIndex {
  friend PartialHashIndexTest;

 public:
  PartialHashIndex() = delete;
  PartialHashIndex(const std::vector<std::pair<ChunkID, std::shared_ptr<Chunk>>>& chunks_to_index, const ColumnID);

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

  /**
   * Inserts entries for the given chunks into this index. If index entries already exist for a given chunk, entries
   * for that chunk are not inserted again.
   *
   * @return The number of chunks for which index entries were inserted.
   */
  size_t insert_entries(const std::vector<std::pair<ChunkID, std::shared_ptr<Chunk>>>&);

  /**
   * Removes the given chunks from this index. If a chunk is not indexed, nothing will happen.
   *
   * @return The number of removed chunks.
   */
  size_t remove_entries(const std::vector<ChunkID>&);

  /**
   * Checks whether null values are indexed.
   *
   * @return true if null values are indexed, false otherwise.
   */
  bool indexed_null_values() const;

  /**
   * Checks whether the given column id is covered by the index.
   *
   * @return true if the given column is covered by the index.
   */
  bool is_index_for(const ColumnID column_id) const;

  std::unordered_set<ChunkID> get_indexed_chunk_ids() const;

  /**
   * @return The ColumnID covered by the index.
   */
  ColumnID get_indexed_column_id() const;

  size_t estimate_memory_usage() const;

 protected:
  Iterator _cbegin() const;

  Iterator _cend() const;

  Iterator _null_cbegin() const;

  Iterator _null_cend() const;

  IteratorPair _range_equals(const AllTypeVariant& value) const;

  std::pair<IteratorPair, IteratorPair> _range_not_equals(const AllTypeVariant& value) const;

  mutable std::shared_mutex _data_access_mutex;

 private:
  const ColumnID _column_id;
  std::unique_ptr<BasePartialHashIndexImpl> _impl;
};

}  // namespace hyrise
