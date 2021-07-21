#pragma once

#include <utility>

#include "partial_hash_index_impl.hpp"
#include "storage/index/abstract_table_index.hpp"

namespace opossum {

class PartialHashIndexTest;

/**
 * Represents a table index using the tsl::robin_map hash map where all hashed values are mapped to the RowIDs of their
 * occurrences in the original Chunks. This allows for faster lookup time while index build times are generally
 * increased compared to chunk-based indexes.
 * It can be constructed for a set of chunks of a table column and can later be modified by adding additional chunks
 * or removing already indexed chunks.
 */
class PartialHashIndex : public AbstractTableIndex {
  friend PartialHashIndexTest;

 public:
  /**
   * Predicts the memory consumption in bytes of creating this index.
   * See AbstractIndex::estimate_memory_consumption()
   * The introduction of PMR strings increased this significantly (in one test from 320 to 896). If you are interested
   * in reducing the memory footprint of these indexes, this is probably the first place you should look.
   */
  static size_t estimate_memory_consumption(ChunkOffset row_count, ChunkOffset distinct_count, uint32_t value_bytes);

  PartialHashIndex() = delete;
  PartialHashIndex(const std::vector<std::pair<ChunkID, std::shared_ptr<Chunk>>>&, const ColumnID);

  /**
   * Adds the given chunks to this index. If a chunk is already indexed, it is not indexed again.
   *
   * @return The number of added chunks.
   */
  size_t add(const std::vector<std::pair<ChunkID, std::shared_ptr<Chunk>>>&);

  /**
   * Removes the given chunks from this index. If a chunk is not indexed, nothing will happen.
   *
   * @return The number of removed chunks.
   */
  size_t remove(const std::vector<ChunkID>&);

 protected:
  Iterator _cbegin() const override;
  Iterator _cend() const override;
  Iterator _null_cbegin() const override;
  Iterator _null_cend() const override;
  size_t _memory_consumption() const override;

  IteratorPair _equals(const AllTypeVariant& value) const override;
  std::pair<IteratorPair, IteratorPair> _not_equals(const AllTypeVariant& value) const override;

  bool _is_index_for(const ColumnID column_id) const override;
  std::set<ChunkID> _get_indexed_chunk_ids() const override;

 private:
  ColumnID _column_id;
  bool _is_initialized = false;
  std::shared_ptr<BasePartialHashIndexImpl> _impl;
};

}  // namespace opossum
