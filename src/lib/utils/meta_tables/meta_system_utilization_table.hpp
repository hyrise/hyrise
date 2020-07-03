#pragma once

#include "utils/meta_tables/abstract_meta_table.hpp"

namespace opossum {

/**
 * This is a class for showing dynamic system information such as CPU and RAM load.
 */
class MetaSystemUtilizationTable : public AbstractMetaTable {
 public:
  MetaSystemUtilizationTable();

  const std::string& name() const final;

 protected:
  friend class MetaSystemUtilizationTest;
  std::shared_ptr<Table> _on_generate() const final;

  struct LoadAvg {
    float load_1_min;
    float load_5_min;
    float load_15_min;
  };

  struct SystemMemoryUsage {
    uint64_t free_memory;
    uint64_t available_memory;
  };

  struct ProcessMemoryUsage {
    uint64_t virtual_memory;
    uint64_t physical_memory;
  };

  static LoadAvg _get_load_avg();
  static uint64_t _get_total_time();
  static uint64_t _get_system_cpu_time();
  static uint64_t _get_process_cpu_time();
  static SystemMemoryUsage _get_system_memory_usage();
  static ProcessMemoryUsage _get_process_memory_usage();
  static std::optional<size_t> _get_allocated_memory();
  static std::vector<int64_t> _parse_value_string(std::string& input_string);
};

}  // namespace opossum
