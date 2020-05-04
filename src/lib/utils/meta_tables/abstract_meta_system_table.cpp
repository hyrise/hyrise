#ifdef __APPLE__
#include <mach/mach.h>
#endif

#include <fstream>

#include "utils/meta_tables/abstract_meta_system_table.hpp"

namespace opossum {

AbstractMetaSystemTable::AbstractMetaSystemTable(const TableColumnDefinitions& column_definitions)
    : AbstractMetaTable(column_definitions) {}

// Returns the number of logical processors
size_t AbstractMetaSystemTable::_get_cpu_count() {
#ifdef __linux__
  std::ifstream cpu_info_file;
  cpu_info_file.open("/proc/cpuinfo", std::ifstream::in);
  DebugAssert(cpu_info_file.is_open(), "Failed to open /proc/cpuinfo");

  size_t processors = 0;
  std::string cpu_info_line;
  while (std::getline(cpu_info_file, cpu_info_line)) {
    if (cpu_info_line.rfind("processor", 0) == 0) ++processors;
  }

  cpu_info_file.close();

  return processors;
#endif

#ifdef __APPLE__
  size_t processors;
  size_t size = sizeof(processors);
  const auto ret = sysctlbyname("hw.ncpu", &processors, &size, nullptr, 0);
  DebugAssert(ret == 0, "Failed to call sysctl hw.ncpu");

  return processors;
#endif

  Fail("Method not implemented for this platform");
}

}  // namespace opossum
