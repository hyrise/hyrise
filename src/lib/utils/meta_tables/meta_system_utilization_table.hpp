#pragma once

#include "utils/meta_tables/abstract_meta_system_table.hpp"

namespace opossum {

/**
 * This is a class for showing dynamic system information such as CPU and RAM load.
 */
class MetaSystemUtilizationTable : public AbstractMetaSystemTable {
 public:
  MetaSystemUtilizationTable();

  const std::string& name() const final;

 protected:
  std::shared_ptr<Table> _on_generate() const final;

  struct LoadAvg {
    float load_1_min;
    float load_5_min;
    float load_15_min;
  };

  struct SystemMemoryUsage {
    int64_t free_memory;
    int64_t available_memory;
  };

  struct ProcessMemoryUsage {
    int64_t virtual_memory;
    int64_t physical_memory;
  };

  static LoadAvg _get_load_avg();
  static int64_t _get_total_time();
  static int64_t _get_system_cpu_time();
  static int64_t _get_process_cpu_time();
  static SystemMemoryUsage _get_system_memory_usage();
  static std::vector<int64_t> _get_values(std::string& input_string);
  static ProcessMemoryUsage _get_process_memory_usage();
};

}  // namespace opossum
