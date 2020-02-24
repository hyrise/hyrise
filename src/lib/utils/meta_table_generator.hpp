#pragma once

namespace opossum {

void gather_segment_meta_data(const std::shared_ptr<Table>& meta_table, const MemoryUsageCalculationMode mode);

void init_workload_meta_data();
void gather_workload_meta_data(const std::shared_ptr<Table>& meta_table);

}  // namespace opossum
