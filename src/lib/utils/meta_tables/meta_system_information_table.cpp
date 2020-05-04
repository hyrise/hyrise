#ifdef __linux__
#include <sched.h>
#include <sys/sysinfo.h>
#endif

#include <fstream>

#include "meta_system_information_table.hpp"

namespace opossum {

MetaSystemInformationTable::MetaSystemInformationTable()
    : AbstractMetaSystemTable(TableColumnDefinitions{{"cpu_count", DataType::Int, false},
                                                     {"system_memory_total_bytes", DataType::Long, false},
                                                     {"cpu_affinity_count", DataType::Int, false},
                                                     {"cpu_model", DataType::String, false}}) {}

const std::string& MetaSystemInformationTable::name() const {
  static const auto name = std::string{"system_information"};
  return name;
}

std::shared_ptr<Table> MetaSystemInformationTable::_on_generate() const {
  auto output_table = std::make_shared<Table>(_column_definitions, TableType::Data, std::nullopt, UseMvcc::Yes);

  const auto cpus = _get_cpu_count();

#ifdef __linux__
  auto* cpuset = CPU_ALLOC(cpus);
  const auto size = CPU_ALLOC_SIZE(cpus);
  sched_getaffinity(0, size, cpuset);
  const auto cpu_affinity_count = CPU_COUNT_S(size, cpuset);
  CPU_FREE(cpuset);
#endif
#ifdef __APPLE__
  const auto cpu_affinity_count = cpus;
#endif

  uint64_t ram;
#ifdef __linux__
  struct sysinfo memory_info {};
  const auto ret = sysinfo(&memory_info);
  Assert(ret == 0, "Failed to get sysinfo");

  ram = memory_info.totalram * memory_info.mem_unit;
#endif

#ifdef __APPLE__
  size_t size = sizeof(ram);
  const auto ret = sysctlbyname("hw.memsize", &ram, &size, nullptr, 0);
  Assert(ret == 0, "Failed to call sysctl hw.memsize");
#endif

  const auto cpu_model = _cpu_model();

  output_table->append({static_cast<int32_t>(cpus), static_cast<int64_t>(ram), static_cast<int32_t>(cpu_affinity_count),
                        static_cast<pmr_string>(cpu_model)});

  return output_table;
}

// Returns the CPU model string
std::string MetaSystemInformationTable::_cpu_model() {
#ifdef __linux__
  std::ifstream cpuinfo_file;
  try {
    cpuinfo_file.open("/proc/cpuinfo", std::ifstream::in);

    std::string cpuinfo_line;
    while (std::getline(cpuinfo_file, cpuinfo_line)) {
      if (cpuinfo_line.starts_with("model name")) {
        cpuinfo_line.erase(0, cpuinfo_line.find(": ") + 2);
        cpuinfo_file.close();
        return cpuinfo_line;
      }
    }
  } catch (std::ios_base::failure& fail) {
    Fail("Failed to read /proc/cpuinfo");
  }

  Fail("Could not read CPU model.");
#endif

#ifdef __APPLE__
  size_t buffer_size = 256;
  char buffer[256];
  const auto ret = sysctlbyname("machdep.cpu.brand_string", &buffer, &buffer_size, nullptr, 0);
  Assert(ret == 0, "Failed to call sysctl machdep.cpu.brand_string");

  return std::string(buffer);
#endif

  Fail("Method not implemented for this platform");
}

}  // namespace opossum
