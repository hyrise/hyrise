#ifdef __linux__

#include <numa.h>
#include <sys/sysinfo.h>

#endif

#ifdef __APPLE__

#include <sys/sysctl.h>
#include <sys/types.h>

#endif

#include <fstream>
#include <iostream>

#include "meta_system_information_table.hpp"

#include "hyrise.hpp"

namespace opossum {

MetaSystemInformationTable::MetaSystemInformationTable()
    : AbstractMetaSystemTable(TableColumnDefinitions{{"cpu_count", DataType::Int, false},
                                                     {"system_memory_total_bytes", DataType::Long, false},
                                                     {"numa_cpu_count", DataType::Int, false},
                                                     {"cpu_model", DataType::String, false}}) {}

const std::string& MetaSystemInformationTable::name() const {
  static const auto name = std::string{"system_information"};
  return name;
}

std::shared_ptr<Table> MetaSystemInformationTable::_on_generate() {
  auto output_table = std::make_shared<Table>(_column_definitions, TableType::Data, std::nullopt, UseMvcc::Yes);

  auto cpus = _get_cpu_count();

  auto numa_cpus = cpus;
#ifdef __linux__
  if (numa_available() != -1) {
    numa_cpus = numa_num_task_cpus();
  }
#endif

  int64_t ram;
#ifdef __linux__
  struct sysinfo memory_info;
  sysinfo(&memory_info);

  ram = memory_info.totalram * memory_info.mem_unit;
#endif

#ifdef __APPLE__
  size_t size = sizeof(ram);
  if (sysctlbyname("hw.memsize", &ram, &size, nullptr, 0) != 0) {
    Fail("Unable to call sysctl hw.memsize");
  }
#endif
  const auto cpu_model = _cpu_model();

  output_table->append({cpus, ram, numa_cpus, cpu_model});

  return output_table;
}

const pmr_string MetaSystemInformationTable::_cpu_model() const {
#ifdef __linux__
  std::ifstream cpuinfo_file;
  cpuinfo_file.open("/proc/cpuinfo", std::ifstream::in);

  std::string cpuinfo_line;
  while (std::getline(cpuinfo_file, cpuinfo_line)) {
    if (cpuinfo_line.rfind("model name", 0) == 0) {
      cpuinfo_line.erase(0, cpuinfo_line.find(": ") + 2);
      cpuinfo_file.close();
      return pmr_string{cpuinfo_line};
    }
  }
  return "No CPU Model";
#endif

#ifdef __APPLE__

  size_t buffer_size = 256;
  char buffer[256];
  if (sysctlbyname("machdep.cpu.brand_string", &buffer, &buffer_size, nullptr, 0) != 0) {
    Fail("Unable to call sysctl machdep.cpu.brand_string");
  }
  return buffer;
#endif
}

}  // namespace opossum
