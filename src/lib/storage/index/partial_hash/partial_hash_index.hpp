#pragma once

#include <unordered_set>
#include <utility>

#include "partial_hash_index_impl.hpp"
#include "storage/index/abstract_table_index.hpp"

namespace hyrise {

class PartialHashIndexTest;

/**
 * Represents a table index using a hash map where all hashed values are mapped to the RowIDs of their occurrences in
 * the original Chunks. This allows for faster lookup time while index build times are generally increased compared
 * to chunk-based indexes. It can be constructed for a set of chunks of a table column and can later be modified by
 * adding additional chunks or removing already indexed chunks.
 */
class PartialHashIndex : public AbstractTableIndex {
  friend PartialHashIndexTest;

 public:
  PartialHashIndex() = delete;
  PartialHashIndex(const std::vector<std::pair<ChunkID, std::shared_ptr<Chunk>>>& chunks_to_index, const ColumnID);
  // This constructor is only called if a table index is to be created for a table without any chunks. In this case,
  // the indexed column's data type cannot be retrieved from a segment since no segment exists. Thus, the DataType has
  // to be provided explicitly.
  PartialHashIndex(const DataType data_type, const ColumnID);

  /**
   * Inserts entries for the given chunks into this index. If index entries already exist for a given chunk, entries
   * for that chunk are not inserted again.
   *
   * @return The number of chunks for which index entries were inserted.
   */
  size_t insert_entries(const std::vector<std::pair<ChunkID, std::shared_ptr<Chunk>>>&);

 protected:
  Iterator _cbegin() const final;
  Iterator _cend() const final;
  Iterator _null_cbegin() const final;
  Iterator _null_cend() const final;
  size_t _estimate_memory_usage() const final;

  IteratorPair _range_equals(const AllTypeVariant& value) const final;
  std::pair<IteratorPair, IteratorPair> _range_not_equals(const AllTypeVariant& value) const final;

  bool _is_index_for(const ColumnID column_id) const final;
  std::unordered_set<ChunkID> _get_indexed_chunk_ids() const final;
  ColumnID _get_indexed_column_id() const final;

 private:
  const ColumnID _column_id;
  std::shared_ptr<BasePartialHashIndexImpl> _impl;
};

}  // namespace hyrise
