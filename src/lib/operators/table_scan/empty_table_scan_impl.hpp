#pragma once

#include <functional>
#include <memory>
#include <utility>
#include <vector>

#include "base_single_column_table_scan_impl.hpp"

#include "all_type_variant.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

/**
 * @brief Table Scan that always returns an empty result.
 *
 * - Currently this is used for scans that compare anything with NULL (except for IS [NOT] NULL)
 * - Predicates like "col = NULL" will always result in NULL, i.e., no matches
 * 
 */
class EmptyTableScanImpl : public BaseSingleColumnTableScanImpl {
 public:
  EmptyTableScanImpl(std::shared_ptr<const Table> in_table, const ColumnID left_column_id, const ScanType& scan_type)
      : BaseSingleColumnTableScanImpl{in_table, left_column_id, scan_type} {}

  PosList scan_chunk(ChunkID) override { return PosList{}; }

  void handle_value_column(const BaseValueColumn&, std::shared_ptr<ColumnVisitableContext>) override {}

  void handle_dictionary_column(const BaseDictionaryColumn&, std::shared_ptr<ColumnVisitableContext>) override {}
};

}  // namespace opossum
