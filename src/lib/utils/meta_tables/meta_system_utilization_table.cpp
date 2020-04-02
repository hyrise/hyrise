#ifdef __linux__

#include <numa.h>
#include <sys/sysinfo.h>
#include <sys/times.h>
#include <fstream>
#include <sstream>

#endif

#ifdef __APPLE__

#include <mach/mach.h>
#include <mach/mach_error.h>
#include <mach/mach_host.h>
#include <mach/mach_init.h>
#include <mach/mach_time.h>
#include <mach/vm_map.h>
#include <mach/vm_statistics.h>
#include <sys/resource.h>
#include <sys/sysctl.h>
#include <time.h>

#endif

#include <sys/types.h>

#include <chrono>

#include "meta_system_utilization_table.hpp"

#include "hyrise.hpp"

namespace opossum {

MetaSystemUtilizationTable::MetaSystemUtilizationTable()
    : AbstractMetaSystemTable(TableColumnDefinitions{{"cpu_system_ticks", DataType::Long, false},
                                                     {"cpu_process_ticks", DataType::Long, false},
                                                     {"total_ticks", DataType::Long, false},
                                                     {"load_average_1_min", DataType::Float, false},
                                                     {"load_average_5_min", DataType::Float, false},
                                                     {"load_average_15_min", DataType::Float, false},
                                                     {"system_memory_free", DataType::Long, false},
                                                     {"system_memory_available", DataType::Long, false},
                                                     {"process_virtual_memory", DataType::Long, false},
                                                     {"process_physical_memory_RSS", DataType::Long, false}}) {}

const std::string& MetaSystemUtilizationTable::name() const {
  static const auto name = std::string{"system_utilization"};
  return name;
}

std::shared_ptr<Table> MetaSystemUtilizationTable::_on_generate() {
  auto output_table = std::make_shared<Table>(_column_definitions, TableType::Data, std::nullopt, UseMvcc::Yes);

  const auto system_cpu_ticks = _get_system_cpu_ticks();
  const auto process_cpu_ticks = _get_process_cpu_ticks();
  const auto total_ticks = _get_total_ticks();
  const auto load_avg = _get_load_avg();
  const auto system_memory_usage = _get_system_memory_usage();
  const auto process_memory_usage = _get_process_memory_usage();

  output_table->append({system_cpu_ticks, process_cpu_ticks, total_ticks, load_avg.load_1_min, load_avg.load_5_min,
                        load_avg.load_15_min, system_memory_usage.free_memory, system_memory_usage.available_memory,
                        process_memory_usage.virtual_memory, process_memory_usage.physical_memory});

  return output_table;
}

/*
  * Returns the load average values for 1min, 5min, and 15min.
*/
MetaSystemUtilizationTable::LoadAvg MetaSystemUtilizationTable::_get_load_avg() {
#ifdef __linux__
  std::ifstream load_avg_file;
  load_avg_file.open("/proc/loadavg", std::ifstream::in);

  std::string load_avg_value;
  std::vector<float> load_avg_values;
  for (int value_index = 0; value_index < 3; ++value_index) {
    std::getline(load_avg_file, load_avg_value, ' ');
    load_avg_values.push_back(std::stof(load_avg_value));
  }
  load_avg_file.close();

  return {load_avg_values[0], load_avg_values[1], load_avg_values[2]};
#endif

#ifdef __APPLE__
  loadavg load_avg;
  size_t size = sizeof(load_avg);
  if (sysctlbyname("vm.loadavg", &load_avg, &size, nullptr, 0) != 0) {
    Fail("Unable to call sysctl vm.loadavg");
  }

  return {static_cast<float>(load_avg.ldavg[0]) / static_cast<float>(load_avg.fscale),
          static_cast<float>(load_avg.ldavg[1]) / static_cast<float>(load_avg.fscale),
          static_cast<float>(load_avg.ldavg[2]) / static_cast<float>(load_avg.fscale)};
#endif

  Fail("Method not implemented for this platform");
}

/*
  * Returns the number of clock ticks since an arbitrary point in the past.
*/
int64_t MetaSystemUtilizationTable::_get_total_ticks() {
#ifdef __linux__
  struct tms time_sample {};
  return times(&time_sample);
#endif

#ifdef __APPLE__
  return clock_gettime_nsec_np(CLOCK_UPTIME_RAW);
#endif

  Fail("Method not implemented for this platform");
}

/*
 * Returns the number of clock ticks that ALL processes have spend on the CPU 
 * since an arbitrary point in the past. 
*/
int64_t MetaSystemUtilizationTable::_get_system_cpu_ticks() {
#ifdef __linux__
  std::ifstream stat_file;
  stat_file.open("/proc/stat", std::ifstream::in);
  std::string cpu_line;
  std::getline(stat_file, cpu_line);
  stat_file.close();

  const auto cpu_ticks = _get_values(cpu_line);

  const auto user_ticks = cpu_ticks.at(0);
  const auto user_nice_ticks = cpu_ticks.at(1);
  const auto kernel_ticks = cpu_ticks.at(2);

  const auto active_ticks = user_ticks + user_nice_ticks + kernel_ticks;
  int cpu_count;
  if (numa_available() != -1) {
    cpu_count = numa_num_task_cpus();
  } else {
    cpu_count = _get_cpu_count();
  }

  return active_ticks / cpu_count;
#endif

#ifdef __APPLE__
  host_cpu_load_info_data_t cpu_info;
  mach_msg_type_number_t count = HOST_CPU_LOAD_INFO_COUNT;
  if (host_statistics(mach_host_self(), HOST_CPU_LOAD_INFO, reinterpret_cast<host_info_t>(&cpu_info), &count) !=
      KERN_SUCCESS) {
    Fail("Unable to access host_statistics");
  }

  int64_t total_ticks = 0;
  for (int cpu_state = 0; cpu_state <= CPU_STATE_MAX; ++cpu_state) {
    total_ticks += cpu_info.cpu_ticks[cpu_state];
  }
  const auto idle_ticks = cpu_info.cpu_ticks[CPU_STATE_IDLE];
  const auto active_ticks = total_ticks - idle_ticks;

  // TODO(j-tr): make this ns not ticks
  return active_ticks;
#endif

  Fail("Method not implemented for this platform");
}

/*
 * Returns the number of clock ticks that THIS process has spend on the CPU 
 * since an arbitrary point in the past. 
*/
int64_t MetaSystemUtilizationTable::_get_process_cpu_ticks() {
#ifdef __linux__
  struct tms time_sample {};
  times(&time_sample);

  const auto kernel_ticks = time_sample.tms_stime;
  const auto user_ticks = time_sample.tms_utime;

  const auto active_ticks = user_ticks + kernel_ticks;
  int cpu_count;
  if (numa_available() != -1) {
    cpu_count = numa_num_task_cpus();
  } else {
    cpu_count = _get_cpu_count();
  }

  return active_ticks / cpu_count;
#endif

#ifdef __APPLE__
  const auto active_time = clock_gettime_nsec_np(CLOCK_PROCESS_CPUTIME_ID);

  return active_time;
#endif

  Fail("Method not implemented for this platform");
}

/*
 * Returns a struct that contains the avaiable and free memory size in bytes.
 * - Free memory is unallocated memory.
 * - Availlable memory includes free memory and currently allocated memory that 
 *   could be made available (e.g. buffers, caches ...)
*/
MetaSystemUtilizationTable::SystemMemoryUsage MetaSystemUtilizationTable::_get_system_memory_usage() {
#ifdef __linux__
  std::ifstream meminfo_file;
  meminfo_file.open("/proc/meminfo", std::ifstream::in);

  MetaSystemUtilizationTable::SystemMemoryUsage memory_usage{};
  std::string meminfo_line;
  while (std::getline(meminfo_file, meminfo_line)) {
    if (meminfo_line.rfind("MemFree", 0) == 0) {
      memory_usage.free_memory = _get_values(meminfo_line)[0] * 1024;
    } else if (meminfo_line.rfind("MemAvailable", 0) == 0) {
      memory_usage.available_memory = _get_values(meminfo_line)[0] * 1024;
    }
  }
  meminfo_file.close();

  return memory_usage;
#endif

#ifdef __APPLE__
  int64_t physical_memory;
  size_t size = sizeof(physical_memory);
  if (sysctlbyname("hw.memsize", &physical_memory, &size, nullptr, 0) != 0) {
    Fail("Unable to call sysctl hw.memsize");
  }

  vm_size_t page_size;
  vm_statistics64_data_t vm_statistics;
  mach_msg_type_number_t count = sizeof(vm_statistics) / sizeof(natural_t);
  if (host_page_size(mach_host_self(), &page_size) != KERN_SUCCESS ||
      host_statistics64(mach_host_self(), HOST_VM_INFO, reinterpret_cast<host_info64_t>(&vm_statistics), &count) !=
          KERN_SUCCESS) {
    Fail("Unable to access host_page_size or host_statistics64");
  }

  MetaSystemUtilizationTable::SystemMemoryUsage memory_usage;
  memory_usage.free_memory = vm_statistics.free_count * page_size;
  memory_usage.available_memory = (vm_statistics.inactive_count + vm_statistics.free_count) * page_size;

  return memory_usage;
#endif

  Fail("Method not implemented for this platform");
}

/*
 * Returns a struct that contains the virtual and physical memory used by this process in bytes.
 * - Virtual Memory is the total memory usage of the process
 * - Physical Memory is the resident set size (RSS), the portion of memory that is held in RAM
*/
MetaSystemUtilizationTable::ProcessMemoryUsage MetaSystemUtilizationTable::_get_process_memory_usage() {
#ifdef __linux__
  std::ifstream self_status_file;
  self_status_file.open("/proc/self/status", std::ifstream::in);

  MetaSystemUtilizationTable::ProcessMemoryUsage memory_usage{};
  std::string self_status_line;
  while (std::getline(self_status_file, self_status_line)) {
    if (self_status_line.rfind("VmSize", 0) == 0) {
      memory_usage.virtual_memory = _get_values(self_status_line)[0] * 1024;
    } else if (self_status_line.rfind("VmRSS", 0) == 0) {
      memory_usage.physical_memory = _get_values(self_status_line)[0] * 1024;
    }
  }

  self_status_file.close();

  return memory_usage;
#endif

#ifdef __APPLE__
  struct task_basic_info info;
  mach_msg_type_number_t count = TASK_BASIC_INFO_COUNT;
  if (task_info(mach_task_self(), TASK_BASIC_INFO, reinterpret_cast<task_info_t>(&info), &count) != KERN_SUCCESS) {
    Fail("Unable to access task_info");
  }

  return {static_cast<int64_t>(info.virtual_size), static_cast<int64_t>(info.resident_size)};
#endif

  Fail("Method not implemented for this platform");
}

#ifdef __linux__
std::vector<int64_t> MetaSystemUtilizationTable::_get_values(std::string& input_string) {
  std::stringstream input_stream;
  input_stream << input_string;
  std::vector<int64_t> output_values;

  std::string token;
  int64_t value;
  while (!input_stream.eof()) {
    input_stream >> token;
    if (std::stringstream(token) >> value) {
      output_values.push_back(value);
    }
  }

  return output_values;
}
#endif

}  // namespace opossum
