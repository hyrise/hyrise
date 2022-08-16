#include <chrono>
#include <fstream>

#ifdef __APPLE__
#include <mach/mach.h>
#include <sys/sysctl.h>
#endif

#ifdef HYRISE_WITH_JEMALLOC
#include <jemalloc/jemalloc.h>
#endif

#include "hyrise.hpp"
#include "meta_system_utilization_table.hpp"

namespace hyrise {

MetaSystemUtilizationTable::MetaSystemUtilizationTable()
    : AbstractMetaTable(TableColumnDefinitions{{"cpu_system_time", DataType::Long, false},
                                               {"cpu_process_time", DataType::Long, false},
                                               {"total_time", DataType::Long, false},
                                               {"load_average_1_min", DataType::Float, false},
                                               {"load_average_5_min", DataType::Float, false},
                                               {"load_average_15_min", DataType::Float, false},
                                               {"system_memory_free", DataType::Long, false},
                                               {"system_memory_available", DataType::Long, false},
                                               {"process_virtual_memory", DataType::Long, false},
                                               {"process_RSS", DataType::Long, false},
                                               {"allocated_memory", DataType::Long, true},
                                               {"cpu_affinity_count", DataType::Int, false}}) {}

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
  const auto allocated_memory = _get_allocated_memory();
  const auto allocated_memory_variant =
      allocated_memory ? AllTypeVariant{static_cast<int64_t>(*allocated_memory)} : AllTypeVariant{NULL_VALUE};
  const auto cpu_affinity_count = Hyrise::get().topology.num_cpus();

  output_table->append({static_cast<int64_t>(system_cpu_ticks), static_cast<int64_t>(process_cpu_ticks),
                        static_cast<int64_t>(total_ticks), load_avg.load_1_min, load_avg.load_5_min,
                        load_avg.load_15_min, static_cast<int64_t>(system_memory_usage.free_memory),
                        static_cast<int64_t>(system_memory_usage.available_memory),
                        static_cast<int64_t>(process_memory_usage.virtual_memory),
                        static_cast<int64_t>(process_memory_usage.physical_memory), allocated_memory_variant,
                        static_cast<int32_t>(cpu_affinity_count)});

  return output_table;
}

/**
  * Returns the load average values for 1min, 5min, and 15min.
 */
MetaSystemUtilizationTable::LoadAvg MetaSystemUtilizationTable::_get_load_avg() {
  std::array<double, 3> load_avg{};
  const int nelem = getloadavg(load_avg.data(), 3);
  Assert(nelem == 3, "Failed to read load averages");
  return {static_cast<float>(load_avg[0]), static_cast<float>(load_avg[1]), static_cast<float>(load_avg[2])};
}

/**
  * Returns the time in ns since epoch.
*/
uint64_t MetaSystemUtilizationTable::_get_total_time() {
  auto time = std::chrono::steady_clock::now().time_since_epoch();
  return std::chrono::nanoseconds{time}.count();
}

/**
 * Returns the time in ns that ALL processes have spent on the CPU
 * since an arbitrary point in the past.
 * This might be used to differentiate between CPU time consumed by this process 
 * and by other processes on the same machine.
*/
uint64_t MetaSystemUtilizationTable::_get_system_cpu_time() {
#ifdef __linux__
  std::ifstream stat_file;
  std::string cpu_line;
  try {
    stat_file.open("/proc/stat", std::ifstream::in);

    std::getline(stat_file, cpu_line);
    stat_file.close();
  } catch (std::ios_base::failure& fail) {
    Fail("Failed to read /proc/stat (" + fail.what() + ")");
  }

  const auto cpu_ticks = _parse_value_string(cpu_line);

  const auto user_ticks = cpu_ticks.at(0);
  const auto user_nice_ticks = cpu_ticks.at(1);
  const auto kernel_ticks = cpu_ticks.at(2);

  const auto active_ticks = user_ticks + user_nice_ticks + kernel_ticks;

  // The amount of time in /proc/stat is measured in units of clock ticks.
  // sysconf(_SC_CLK_TCK) can be used to convert it to ns.
  const auto active_ns = (active_ticks * std::nano::den) / sysconf(_SC_CLK_TCK);

  return active_ns;
#endif

#ifdef __APPLE__
  host_cpu_load_info_data_t cpu_info;
  mach_msg_type_number_t count = HOST_CPU_LOAD_INFO_COUNT;
  const auto ret =
      host_statistics(mach_host_self(), HOST_CPU_LOAD_INFO, reinterpret_cast<host_info_t>(&cpu_info), &count);
  Assert(ret == KERN_SUCCESS, "Failed to get host_statistics");

  const auto active_ticks =
      cpu_info.cpu_ticks[CPU_STATE_SYSTEM] + cpu_info.cpu_ticks[CPU_STATE_USER] + cpu_info.cpu_ticks[CPU_STATE_NICE];

  // The amount of time from HOST_CPU_LOAD_INFO is measured in units of clock ticks.
  // sysconf(_SC_CLK_TCK) can be used to convert it to ns.
  const auto active_ns = active_ticks * std::nano::den / sysconf(_SC_CLK_TCK);

  return active_ns;
#endif

  Fail("Method not implemented for this platform");
}

/**
 * Returns the time in ns that THIS process has spent on the CPU
 * since an arbitrary point in the past.
 */
uint64_t MetaSystemUtilizationTable::_get_process_cpu_time() {
  // CLOCK_PROCESS_CPUTIME_ID:
  // A clock that measures (user and system) CPU time consumed by (all of the threads in) the calling process.
#ifdef __linux__
  struct timespec time_spec {};

  const auto ret = clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &time_spec);
  Assert(ret == 0, "Failed in clock_gettime");

  const auto active_ns = (time_spec.tv_sec * std::nano::den + time_spec.tv_nsec);

  return active_ns;
#endif

#ifdef __APPLE__
  const auto active_ns = clock_gettime_nsec_np(CLOCK_PROCESS_CPUTIME_ID);
  Assert(active_ns != 0, "Failed in clock_gettime_nsec_np");

  return active_ns;
#endif

  Fail("Method not implemented for this platform");
}

/**
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
  MetaSystemUtilizationTable::SystemMemoryUsage memory_usage{};
  try {
    meminfo_file.open("/proc/meminfo", std::ifstream::in);

    std::string meminfo_line;
    while (std::getline(meminfo_file, meminfo_line)) {
      if (meminfo_line.starts_with("MemFree")) {
        memory_usage.free_memory = _parse_value_string(meminfo_line)[0] * 1024;
      } else if (meminfo_line.starts_with("MemAvailable")) {
        memory_usage.available_memory = _parse_value_string(meminfo_line)[0] * 1024;
      }
    }
    meminfo_file.close();
  } catch (std::ios_base::failure& fail) {
    Fail("Failed to read /proc/meminfo (" + fail.what() + ")");
  }

  return memory_usage;
#endif

#ifdef __APPLE__
  int64_t physical_memory;
  size_t size = sizeof(physical_memory);
  auto ret = sysctlbyname("hw.memsize", &physical_memory, &size, nullptr, 0);
  Assert(ret == 0, "Failed to call sysctl hw.memsize");

  // see reference: https://stackoverflow.com/a/1911863
  vm_size_t page_size;
  vm_statistics64_data_t vm_statistics;
  mach_msg_type_number_t count = sizeof(vm_statistics) / sizeof(natural_t);
  ret = host_page_size(mach_host_self(), &page_size);
  Assert(ret == KERN_SUCCESS, "Failed to get page size");
  ret = host_statistics64(mach_host_self(), HOST_VM_INFO, reinterpret_cast<host_info64_t>(&vm_statistics), &count);
  Assert(ret == KERN_SUCCESS, "Failed to get host_statistics64");

  auto memory_usage = MetaSystemUtilizationTable::SystemMemoryUsage{};
  memory_usage.free_memory = vm_statistics.free_count * page_size;
  memory_usage.available_memory = (vm_statistics.inactive_count + vm_statistics.free_count) * page_size;

  return memory_usage;
#endif

  Fail("Method not implemented for this platform");
}

/**
 * Returns a struct that contains the virtual and physical memory used by this process in bytes.
 * - Virtual Memory is the total memory usage of the process
 * - Physical Memory is the resident set size (RSS), the portion of memory that is held in RAM
 */
MetaSystemUtilizationTable::ProcessMemoryUsage MetaSystemUtilizationTable::_get_process_memory_usage() {
#ifdef __linux__
  std::ifstream self_status_file;
  MetaSystemUtilizationTable::ProcessMemoryUsage memory_usage{};
  try {
    self_status_file.open("/proc/self/status", std::ifstream::in);

    std::string self_status_line;
    while (std::getline(self_status_file, self_status_line)) {
      if (self_status_line.starts_with("VmSize")) {
        memory_usage.virtual_memory = _parse_value_string(self_status_line)[0] * 1024;
      } else if (self_status_line.starts_with("VmRSS")) {
        memory_usage.physical_memory = _parse_value_string(self_status_line)[0] * 1024;
      }
    }

    self_status_file.close();
  } catch (std::ios_base::failure& fail) {
    Fail("Failed to read /proc/self/status (" + fail.what() + ")");
  }

  return memory_usage;
#endif

#ifdef __APPLE__
  struct task_basic_info info {};

  mach_msg_type_number_t count = TASK_BASIC_INFO_COUNT;
  const auto ret = task_info(mach_task_self(), TASK_BASIC_INFO, reinterpret_cast<task_info_t>(&info), &count);
  Assert(ret == KERN_SUCCESS, "Failed to get task_info");

  return {info.virtual_size, info.resident_size};
#endif

  Fail("Method not implemented for this platform");
}

/**
 * This returns the actually allocated memory. It differs from _get_process_memory_usage in that it only returns
 * memory that has been allocated. The reported virtual memory consumption is usually higher than the amount of
 * allocated memory as free'd memory is not immediately returned to the system (either due to internal page
 * fragmentation or because jemalloc keeps empty pages for future use). In rare cases, it can also be higher than
 * the amount of virtual memory if memory has been allocated but not committed yet.
 *
 * jemalloc's memory allocation uses different size classes, each with a different number of bytes to allocate. If a
 * data structure's allocator requests a specific amount of bytes that exceeds a certain size class by only one byte,
 * the next larger size class is used and the full amount of bytes of this class is allocated. The spacing between
 * size classes doubles every 4th class. Consequently, the larger the size class that the requested amount of memory
 * slightly exceeds, the larger the difference between the actually allocated and the requested memory.
 *
 * Example:
 *   Assumed size classes: ... 20 KiB, 24 KiB, ..., 256 KiB, 320 KiB, ...
 *
 *                                           |  case 1        | case 2         |
 *   ----------------------------------------+----------------+----------------+
 *   requested memory                        |  20 KiB + 1 B  | 256 KiB + 1 B  |
 *   used size class, i.e., allocated memory |  24 KiB        | 320 KiB        |
 *   delta: allocated mem. - requested mem.  |  4 KiB - 1 B   | 64 KiB - 1 B   |
 *
 * Reference: https://www.freebsd.org/cgi/man.cgi?jemalloc(3)
 */
std::optional<size_t> MetaSystemUtilizationTable::_get_allocated_memory() {
#ifdef HYRISE_WITH_JEMALLOC
  if (HYRISE_DEBUG) {
    // Check that jemalloc was built with statistics support

    bool stats_enabled;
    size_t stats_enabled_size{sizeof(stats_enabled)};

    auto error_code = mallctl("config.stats", &stats_enabled, &stats_enabled_size, nullptr, 0);
    Assert(!error_code, "Cannot check if jemalloc was built with --stats_enabled");
    Assert(stats_enabled, "Hyrise's jemalloc was not build with --stats_enabled");
  }

  // Before retrieving the statistics, we need to update jemalloc's epoch to get current values. See the mallctl
  // documentation for details.
  {
    uint64_t epoch = 1;
    auto epoch_size = sizeof(epoch);
    auto error_code = mallctl("epoch", &epoch, &epoch_size, &epoch, epoch_size);
    Assert(!error_code, "Setting epoch failed");
  }

  size_t allocated;
  auto allocated_size = sizeof(allocated);

  auto error_code = mallctl("stats.allocated", &allocated, &allocated_size, nullptr, 0);
  Assert(!error_code, std::string{"mallctl failed with error code "} + std::to_string(error_code));

  return allocated;
#else
  // Hyrise is compiled with jemalloc unless tsan is used (see src/lib/CMakeLists.txt). To maintain compatibility with
  // other allocators, we return nullopt here.
  return std::nullopt;
#endif
}

#ifdef __linux__
std::vector<int64_t> MetaSystemUtilizationTable::_parse_value_string(std::string& input_string) {
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

}  // namespace hyrise
