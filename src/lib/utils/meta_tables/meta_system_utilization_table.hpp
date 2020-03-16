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

  void init();

 protected:
  std::shared_ptr<Table> _on_generate();

  struct LoadAvg {
    float load_1_min;
    float load_5_min;
    float load_15_min;
  };

  struct SystemMemoryUsage {
    int64_t total_ram;
    int64_t total_swap;
    int64_t total_memory;
    int64_t free_ram;
    int64_t free_swap;
    int64_t free_memory;
  };

  struct ProcessMemoryUsage {
    int64_t virtual_memory;
    int64_t physical_memory;
  };

  LoadAvg _get_load_avg();
  float _get_system_cpu_usage();
  float _get_process_cpu_usage();
  SystemMemoryUsage _get_system_memory_usage();
  int64_t _parse_line(std::string input_string);
  ProcessMemoryUsage _get_process_memory_usage();
};

}  // namespace opossum
