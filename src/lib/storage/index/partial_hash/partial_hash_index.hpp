#pragma once

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
  // using Iterator = IteratorWrapper;
  // using IteratorPair = std::pair<Iterator, Iterator>;

  PartialHashIndex() = delete;
  PartialHashIndex(const std::vector<std::pair<ChunkID, std::shared_ptr<Chunk>>>& chunks_to_index, const ColumnID);

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

  std::unordered_set<ChunkID> get_indexed_chunk_ids() const;

  /**
   * Returns the ColumnID covered by the index.
   *
   * @return The ColumnID covered by the index.
   */
  ColumnID get_indexed_column_id() const;

 protected:
  // template <typename DataType>
  // TableIndexIterator<DataType> _cbegin() const;

  // template <typename DataType>
  // TableIndexIterator<DataType> _cend() const;

  // template <typename DataType>
  // TableIndexIterator<DataType> _null_cbegin() const;

  // template <typename DataType>
  // TableIndexIterator<DataType> _null_cend() const;

  size_t _estimate_memory_usage() const;

  template <typename DataType>
  std::pair<TableIndexIterator<DataType>, TableIndexIterator<DataType>> _range_equals(const AllTypeVariant& value) const;

  template <typename DataType>
  std::pair<std::pair<TableIndexIterator<DataType>, TableIndexIterator<DataType>>, std::pair<TableIndexIterator<DataType>, TableIndexIterator<DataType>>> _range_not_equals(const AllTypeVariant& value) const;

  mutable std::shared_mutex _data_access_mutex;

 private:
  const ColumnID _column_id;
  std::shared_ptr<BasePartialHashIndexImpl> _impl;
};

}  // namespace hyrise
