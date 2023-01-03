#pragma once

#include "partial_hash_index_impl.hpp"
#include "storage/index/abstract_table_index.hpp"

namespace hyrise {

/**
 * Represents a table index using a hash map where all hashed values are mapped to the RowIDs of their occurrences in
 * the original Chunks. It can be constructed for a set of chunks of a table column and can later be modified by adding
 * additional chunks or removing already indexed chunks.
 */
class PartialHashIndex : public AbstractTableIndex {
  friend PartialHashIndexTest;

 public:
  PartialHashIndex() = delete;
  PartialHashIndex(const std::vector<std::pair<ChunkID, std::shared_ptr<Chunk>>>& chunks_to_index, const ColumnID);

  /**
   * Inserts entries for the given chunks into this index. If index entries already exist for a given chunk, entries
   * for that chunk are not inserted again.
   *
   * @return The number of chunks for which index entries were inserted.
   */
  size_t insert_entries(const std::vector<std::pair<ChunkID, std::shared_ptr<Chunk>>>&) override final;

  /**
   * Removes the given chunks from this index. If a chunk is not indexed, nothing will happen.
   *
   * @return The number of removed chunks.
   */
  size_t remove_entries(const std::vector<ChunkID>&) override final;

 protected:
  Iterator _cbegin() const override final;
  Iterator _cend() const override final;
  Iterator _null_cbegin() const override final;
  Iterator _null_cend() const override final;
  size_t _estimate_memory_usage() const override final;

  IteratorPair _range_equals(const AllTypeVariant& value) const override final;
  std::pair<IteratorPair, IteratorPair> _range_not_equals(const AllTypeVariant& value) const override final;

  bool _is_index_for(const ColumnID column_id) const override final;
  std::unordered_set<ChunkID> _get_indexed_chunk_ids() const override final;
  ColumnID _get_indexed_column_id() const override final;

 private:
  const ColumnID _column_id;
  std::shared_ptr<BasePartialHashIndexImpl> _impl;
};

}  // namespace hyrise
