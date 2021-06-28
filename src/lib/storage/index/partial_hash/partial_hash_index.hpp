#pragma once

#include <utility>

#include "storage/table.hpp"
#include "storage/index/abstract_ordered_index.hpp"
#include <tsl/robin_map.h>

namespace opossum {

class PartialHashIndexTest;

class PartialHashIndex : public AbstractTableIndex {
  friend PartialHashIndexTest;

 public:
  using Iterator = std::vector<RowID>::const_iterator;

  /**
 * Predicts the memory consumption in bytes of creating this index.
 * See AbstractIndex::estimate_memory_consumption()
 * The introduction of PMR strings increased this significantly (in one test from 320 to 896). If you are interested
 * in reducing the memory footprint of these indexes, this is probably the first place you should look.
 */
  static size_t estimate_memory_consumption(ChunkOffset row_count, ChunkOffset distinct_count, uint32_t value_bytes);

  PartialHashIndex() = delete;
  PartialHashIndex(const std::vector<std::pair<ChunkID, std::shared_ptr<Chunk>>>&, const ColumnID);





 protected:
  Iterator _cbegin() const override;
  Iterator _cend() const override;
  std::vector<std::shared_ptr<const AbstractSegment>> _get_indexed_segments() const override;
  size_t _memory_consumption() const override;

  std::pair<Iterator, Iterator> _equals(const AllTypeVariant& value) const override;

  bool _is_index_for(const ColumnID column_id) const override {
    return column_id == _column_id;
  }
 private:
  tsl::robin_map<AllTypeVariant, std::vector<RowID>> _map; // ToDo(pi) check unordered map? sortiert wegen greater than etc.
  // TODO(pi): Decide whether we store column id here or use tablestatistics on the table
  ColumnID _column_id;
  std::vector<RowID> _row_ids;
  std::vector<std::shared_ptr<const AbstractSegment>> _indexed_segments;

};

}  // namespace opossum
