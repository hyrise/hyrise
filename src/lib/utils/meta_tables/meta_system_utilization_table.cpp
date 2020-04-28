#ifdef __linux__
#include <numa.h>
#endif

#ifdef __APPLE__
#include <mach/mach.h>
#endif

#include <chrono>
#include <fstream>

#include "meta_system_utilization_table.hpp"

namespace opossum {

MetaSystemUtilizationTable::MetaSystemUtilizationTable()
    : AbstractMetaSystemTable(TableColumnDefinitions{{"cpu_system_time", DataType::Long, false},
                                                     {"cpu_process_time", DataType::Long, false},
                                                     {"total_time", DataType::Long, false},
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

std::shared_ptr<Table> MetaSystemUtilizationTable::_on_generate() const {
  auto output_table = std::make_shared<Table>(_column_definitions, TableType::Data, std::nullopt, UseMvcc::Yes);

  const auto system_cpu_ticks = _get_system_cpu_time();
  const auto process_cpu_ticks = _get_process_cpu_time();
  const auto total_ticks = _get_total_time();
  const auto load_avg = _get_load_avg();
  const auto system_memory_usage = _get_system_memory_usage();
  const auto process_memory_usage = _get_process_memory_usage();

  output_table->append({static_cast<int64_t>(system_cpu_ticks), static_cast<int64_t>(process_cpu_ticks),
                        static_cast<int64_t>(total_ticks), load_avg.load_1_min, load_avg.load_5_min,
                        load_avg.load_15_min, static_cast<int64_t>(system_memory_usage.free_memory),
                        static_cast<int64_t>(system_memory_usage.available_memory),
                        static_cast<int64_t>(process_memory_usage.virtual_memory),
                        static_cast<int64_t>(process_memory_usage.physical_memory)});

  return output_table;
}

/*
  * Returns the load average values for 1min, 5min, and 15min.
*/
MetaSystemUtilizationTable::LoadAvg MetaSystemUtilizationTable::_get_load_avg() {
#ifdef __linux__
  std::ifstream load_avg_file;
  load_avg_file.open("/proc/loadavg", std::ifstream::in);
  DebugAssert(load_avg_file.is_open(), "Failed to open /proc/loadavg");

  std::array<float, 3> load_avg_values{};
  std::string load_avg_string;
  for (auto& load_avg_value : load_avg_values) {
    std::getline(load_avg_file, load_avg_string, ' ');
    load_avg_value = std::stof(load_avg_string);
  }
  load_avg_file.close();

  return {load_avg_values[0], load_avg_values[1], load_avg_values[2]};
#endif

#ifdef __APPLE__
  loadavg load_avg;
  // loadavg contains three integer load average values ldavg[3] that need to be divided
  // by the scaling factor fscale in order to obtain the correct load average values.
  size_t size = sizeof(load_avg);
  [[maybe_unused]] const auto ret = sysctlbyname("vm.loadavg", &load_avg, &size, nullptr, 0);
  DebugAssert(ret == 0, "Failed to call sysctl vm.loadavg");

  return {static_cast<float>(load_avg.ldavg[0]) / static_cast<float>(load_avg.fscale),
          static_cast<float>(load_avg.ldavg[1]) / static_cast<float>(load_avg.fscale),
          static_cast<float>(load_avg.ldavg[2]) / static_cast<float>(load_avg.fscale)};
#endif

  Fail("Method not implemented for this platform");
}

/*
  * Returns the time in ns since epch.
*/
uint64_t MetaSystemUtilizationTable::_get_total_time() {
  auto time = std::chrono::steady_clock::now().time_since_epoch();
  return std::chrono::duration_cast<std::chrono::nanoseconds>(time).count();
}

/*
 * Returns the time in ns that ALL processes have spent on the CPU
 * since an arbitrary point in the past.
*/
uint64_t MetaSystemUtilizationTable::_get_system_cpu_time() {
#ifdef __linux__
  std::ifstream stat_file;
  stat_file.open("/proc/stat", std::ifstream::in);
  DebugAssert(stat_file.is_open(), "Failed to open /proc/stat");

  std::string cpu_line;
  std::getline(stat_file, cpu_line);
  stat_file.close();

  const auto cpu_ticks = _get_values(cpu_line);

  const auto user_ticks = cpu_ticks.at(0);
  const auto user_nice_ticks = cpu_ticks.at(1);
  const auto kernel_ticks = cpu_ticks.at(2);

  const auto active_ticks = user_ticks + user_nice_ticks + kernel_ticks;
  size_t cpu_count;
  if (numa_available() != -1) {
    cpu_count = numa_num_task_cpus();
  } else {
    cpu_count = _get_cpu_count();
  }

  // The amount of time in /proc/stat is measured in units of clock ticks.
  // sysconf(_SC_CLK_TCK) can be used to convert it to ns.
  const auto active_ns = (active_ticks * std::nano::den) / (sysconf(_SC_CLK_TCK) * cpu_count);

  return active_ns;
#endif

#ifdef __APPLE__
  host_cpu_load_info_data_t cpu_info;
  mach_msg_type_number_t count = HOST_CPU_LOAD_INFO_COUNT;
  [[maybe_unused]] const auto ret =
      host_statistics(mach_host_self(), HOST_CPU_LOAD_INFO, reinterpret_cast<host_info_t>(&cpu_info), &count);
  DebugAssert(ret == KERN_SUCCESS, "Failed to get host_statistics");

  const auto active_ticks =
      cpu_info.cpu_ticks[CPU_STATE_SYSTEM] + cpu_info.cpu_ticks[CPU_STATE_USER] + cpu_info.cpu_ticks[CPU_STATE_NICE];

  // The amount of time from HOST_CPU_LOAD_INFO is measured in units of clock ticks.
  // sysconf(_SC_CLK_TCK) can be used to convert it to ns.
  const auto active_ns = active_ticks * std::nano::den / (sysconf(_SC_CLK_TCK) * _get_cpu_count());

  return active_ns;
#endif

  Fail("Method not implemented for this platform");
}

/*
 * Returns the time in ns that THIS process has spent on the CPU
 * since an arbitrary point in the past.
*/
uint64_t MetaSystemUtilizationTable::_get_process_cpu_time() {
  // CLOCK_PROCESS_CPUTIME_ID:
  // Per-process CPU-time clock (measures CPU time consumed by all threads in the process).
#ifdef __linux__
  struct timespec time_spec {};
  [[maybe_unused]] const auto ret = clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &time_spec);
  DebugAssert(ret == 0, "Failed in clock_gettime");

  size_t cpu_count;
  if (numa_available() != -1) {
    cpu_count = numa_num_task_cpus();
  } else {
    cpu_count = _get_cpu_count();
  }

  const auto active_ns = (time_spec.tv_sec * std::nano::den + time_spec.tv_nsec) / cpu_count;

  return active_ns;
#endif

#ifdef __APPLE__
  const auto active_ns = clock_gettime_nsec_np(CLOCK_PROCESS_CPUTIME_ID);
  DebugAssert(active_ns != 0, "Failed in clock_gettime_nsec_np");

  return active_ns / _get_cpu_count();
#endif

  Fail("Method not implemented for this platform");
}

/*
 * Returns a struct that contains the available and free memory size in bytes.
 * - Free memory is unallocated memory.
 * - Available memory includes free memory and currently allocated memory that
 *   could be made available (e.g. buffers, caches ...).
 *   This is not equivalent to the total memory size, since certain data can not
 *   be paged at any time.
*/
MetaSystemUtilizationTable::SystemMemoryUsage MetaSystemUtilizationTable::_get_system_memory_usage() {
#ifdef __linux__
  std::ifstream meminfo_file;
  meminfo_file.open("/proc/meminfo", std::ifstream::in);
  DebugAssert(meminfo_file.is_open(), "Unable to open /proc/meminfo");

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
  [[maybe_unused]] auto ret = sysctlbyname("hw.memsize", &physical_memory, &size, nullptr, 0);
  DebugAssert(ret == 0, "Failed to call sysctl hw.memsize");

  vm_size_t page_size;
  vm_statistics64_data_t vm_statistics;
  mach_msg_type_number_t count = sizeof(vm_statistics) / sizeof(natural_t);
  ret = host_page_size(mach_host_self(), &page_size);
  DebugAssert(ret == KERN_SUCCESS, "Failed to get page size");
  ret = host_statistics64(mach_host_self(), HOST_VM_INFO, reinterpret_cast<host_info64_t>(&vm_statistics), &count);
  DebugAssert(ret == KERN_SUCCESS, "Failed to get host_statistics64");

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
  DebugAssert(self_status_file.is_open(), "Failed to open /proc/self/status");

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
  [[maybe_unused]] const auto ret = task_info(mach_task_self(), TASK_BASIC_INFO, reinterpret_cast<task_info_t>(&info), &count);
  DebugAssert(ret == KERN_SUCCESS, "Failed to get task_info");

  return {info.virtual_size, info.resident_size};
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
