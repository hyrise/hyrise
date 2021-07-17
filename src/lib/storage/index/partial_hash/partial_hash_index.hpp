#pragma once

#include <utility>

#include "partial_hash_index_impl.hpp"
#include "storage/index/abstract_table_index.hpp"

namespace opossum {

class PartialHashIndexTest;

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

  size_t add(const std::vector<std::pair<ChunkID, std::shared_ptr<Chunk>>>&);
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
  // returns sorted array
  std::set<ChunkID> _get_indexed_chunk_ids() const override;

 private:
  ColumnID _column_id;  // TODO(pi): Decide whether we store column id here or use tablestatistics on the table
  bool _is_initialized = false;
  std::shared_ptr<BasePartialHashIndexImpl> _impl;
};

}  // namespace opossum
