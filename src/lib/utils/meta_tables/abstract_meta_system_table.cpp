#include <fstream>

#ifdef __APPLE__

#include <mach/mach.h>

#endif

#include "utils/meta_tables/abstract_meta_system_table.hpp"

#include "hyrise.hpp"

namespace opossum {

AbstractMetaSystemTable::AbstractMetaSystemTable(const TableColumnDefinitions& column_definitions)
    : AbstractMetaTable(column_definitions) {}

int AbstractMetaSystemTable::_get_cpu_count() {
#ifdef __linux__

  std::ifstream cpu_info_file;
  cpu_info_file.open("/proc/cpuinfo", std::ifstream::in);

  uint32_t processors = 0;
  std::string cpu_info_line;
  while (std::getline(cpu_info_file, cpu_info_line)) {
    if (cpu_info_line.rfind("processor", 0) == 0) ++processors;
  }

  cpu_info_file.close();

  return processors;
#endif

#ifdef __APPLE__

  uint32_t processors;
  size_t size = sizeof(processors);
  if (sysctlbyname("hw.ncpu", &processors, &size, nullptr, 0) != 0) {
    Fail("Unable to call sysctl hw.ncpu");
  }

  return processors;
#endif

  Fail("Method not implemented for this platform");
}

}  // namespace opossum
