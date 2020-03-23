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
    int64_t free_memory;
    int64_t available_memory;
  };

  struct ProcessMemoryUsage {
    int64_t virtual_memory;
    int64_t physical_memory;
  };

#ifdef __linux__
  struct SystemCPUTime {
    uint64_t user_time;
    uint64_t user_nice_time;
    uint64_t kernel_time;
    uint64_t idle_time;
  } _last_system_cpu_time{};

  struct ProcessCPUTime {
    clock_t clock_time;
    clock_t kernel_time;
    clock_t user_time;
  } _last_process_cpu_time{};
#endif

#ifdef __APPLE__
  struct SystemCPUTicks {
    uint64_t total_ticks;
    uint64_t idle_ticks;
  } _last_system_cpu_ticks{};

  struct ProcessCPUTime {
    uint64_t system_clock;
    uint64_t process_clock;
  } _last_process_cpu_time{};
#endif

  static LoadAvg _get_load_avg();
  float _get_system_cpu_usage();
  float _get_process_cpu_usage();
  static SystemMemoryUsage _get_system_memory_usage();
  static std::vector<int64_t> _get_values(std::string& input_string);
  static ProcessMemoryUsage _get_process_memory_usage();
};

}  // namespace opossum
