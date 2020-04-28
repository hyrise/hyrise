#ifdef __linux__
#include <numa.h>
#include <sys/sysinfo.h>
#endif

#include <fstream>

#include "meta_system_information_table.hpp"

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

std::shared_ptr<Table> MetaSystemInformationTable::_on_generate() const {
  auto output_table = std::make_shared<Table>(_column_definitions, TableType::Data, std::nullopt, UseMvcc::Yes);

  const auto cpus = _get_cpu_count();

  auto numa_cpus = cpus;
#ifdef __linux__
  if (numa_available() != -1) {
    auto* cpu_mask = numa_allocate_cpumask();
    numa_sched_getaffinity(0, cpu_mask);
    numa_cpus = numa_bitmask_weight(cpu_mask);
    numa_free_cpumask(cpu_mask);
  }
#endif

  uint64_t ram;
#ifdef __linux__
  struct sysinfo memory_info {};
  [[maybe_unused]] const auto ret = sysinfo(&memory_info);
  DebugAssert(ret == 0, "Failed to get sysinfo");

  ram = memory_info.totalram * memory_info.mem_unit;
#endif

#ifdef __APPLE__
  size_t size = sizeof(ram);
  [[maybe_unused]] const auto ret = sysctlbyname("hw.memsize", &ram, &size, nullptr, 0);
  DebugAssert(ret == 0, "Failed to call sysctl hw.memsize");
#endif

  const auto cpu_model = _cpu_model();

  output_table->append({static_cast<int32_t>(cpus), static_cast<int64_t>(ram), static_cast<int32_t>(numa_cpus),
                        static_cast<pmr_string>(cpu_model)});

  return output_table;
}

// Returns the CPU model string
std::string MetaSystemInformationTable::_cpu_model() {
#ifdef __linux__
  std::ifstream cpuinfo_file;
  cpuinfo_file.open("/proc/cpuinfo", std::ifstream::in);
  DebugAssert(cpuinfo_file.is_open(), "Failed to open /proc/cpuinfo");

  std::string cpuinfo_line;
  while (std::getline(cpuinfo_file, cpuinfo_line)) {
    if (cpuinfo_line.rfind("model name", 0) == 0) {
      cpuinfo_line.erase(0, cpuinfo_line.find(": ") + 2);
      cpuinfo_file.close();
      return cpuinfo_line;
    }
  }
  Fail("Could not read CPU model.");
#endif

#ifdef __APPLE__
  size_t buffer_size = 256;
  char buffer[256];
  const auto ret = sysctlbyname("machdep.cpu.brand_string", &buffer, &buffer_size, nullptr, 0);
  DebugAssert(ret == 0, "Failed to call sysctl machdep.cpu.brand_string");

  return std::string(buffer);
#endif
}

}  // namespace opossum
