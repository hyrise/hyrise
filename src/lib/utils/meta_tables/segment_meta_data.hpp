#pragma once

#include "storage/table.hpp"

namespace opossum {

// Methods for collecting information about all stored segments,
// used at MetaSegmentsTable and MetaSegmentsAccurateTable.

/**
 * Fills the table with table name, chunk and column ID, column name, data type,
 * encoding, compression and estimated size. With full mode, also the number of disctinct values is included.
 */
void gather_segment_meta_data(const std::shared_ptr<Table>& meta_table, const MemoryUsageCalculationMode mode);

size_t get_distinct_value_count(const std::shared_ptr<BaseSegment>& segment);

}  // namespace opossum
