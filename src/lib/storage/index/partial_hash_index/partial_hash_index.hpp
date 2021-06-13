#pragma once

#include <storage/table.hpp>
#include "storage/index/abstract_range_index.hpp"
#include "tsl/robin_map.h"

namespace opossum {

class PartialHashIndexTest;

class PartialHashIndex : public AbstractIndex {
  friend PartialHashIndexTest;

 public:
  using Iterator = std::vector<ChunkOffset>::const_iterator;

  /**
 * Predicts the memory consumption in bytes of creating this index.
 * See AbstractRangeIndex::estimate_memory_consumption()
 * The introduction of PMR strings increased this significantly (in one test from 320 to 896). If you are interested
 * in reducing the memory footprint of these indexes, this is probably the first place you should look.
 */
  static size_t estimate_memory_consumption(ChunkOffset row_count, ChunkOffset distinct_count, uint32_t value_bytes);

  PartialHashIndex() = delete;
  PartialHashIndex(const std::shared_ptr<Table>, const std::vector<ChunkID>&, const ColumnID);

  Iterator equal(const AllTypeVariant& value) const;

 protected:
  Iterator _cbegin() const override;
  Iterator _cend() const override;
  std::vector<std::shared_ptr<const AbstractSegment>> _get_indexed_segments() const override;
  size_t _memory_consumption() const override;

 private:
  tsl::robin_map<AllTypeVariant, std::vector<RowID>> _map; // ToDo(pi) change alloator
  std::vector<std::shared_ptr<const AbstractSegment>> _indexed_segments;
};

}  // namespace opossum
