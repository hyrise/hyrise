#pragma once

#include "storage/table.hpp"
#include "storage/index/abstract_index.hpp"

namespace opossum {

class AbstractTableIndex : public AbstractIndex<RowID> {

 public:
  using Iterator = std::vector<RowID>::const_iterator;

  /**
 * Predicts the memory consumption in bytes of creating this index.
 * See AbstractIndex::estimate_memory_consumption()
 * The introduction of PMR strings increased this significantly (in one test from 320 to 896). If you are interested
 * in reducing the memory footprint of these indexes, this is probably the first place you should look.
 */
  static size_t estimate_memory_consumption(ChunkOffset row_count, ChunkOffset distinct_count, uint32_t value_bytes);

  //AbstractTableIndex() = delete;
  explicit AbstractTableIndex(const IndexType type);
  AbstractTableIndex(AbstractTableIndex&&) = default;
  virtual ~AbstractTableIndex() = default;

  std::pair<Iterator, Iterator> equals(const AllTypeVariant& value) const;

  bool is_index_for(const ColumnID column_id) const;

  //TODO(pi): change
  virtual void change_indexed_column(const ColumnID column_id) = 0;

  std::set<ChunkID> get_indexed_chunk_ids() const;

 protected:
  virtual std::pair<Iterator, Iterator> _equals(const AllTypeVariant& value) const = 0;
  virtual bool _is_index_for(const ColumnID column_id) const = 0;
  virtual std::set<ChunkID> _get_indexed_chunk_ids() const = 0;


};

}  // namespace opossum
