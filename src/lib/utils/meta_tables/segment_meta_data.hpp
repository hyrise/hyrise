#pragma once

#include "storage/table.hpp"

namespace opossum {

size_t get_distinct_value_count(const std::shared_ptr<BaseSegment>& segment);

void gather_segment_meta_data(const std::shared_ptr<Table>& meta_table, const MemoryUsageCalculationMode mode);

}  // namespace opossum
