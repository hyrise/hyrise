#include <fstream>

#ifdef __linux__
#include <sys/sysinfo.h>
#endif

#ifdef __APPLE__
#include <mach/mach.h>
#endif

#include "hyrise.hpp"
#include "meta_system_information_table.hpp"

namespace opossum {

MetaSystemInformationTable::MetaSystemInformationTable()
    : AbstractMetaTable(TableColumnDefinitions{{"cpu_count", DataType::Int, false},
                                               {"system_memory_total_bytes", DataType::Long, false},
                                               {"cpu_model", DataType::String, false}}) {}

const std::string& MetaSystemInformationTable::name() const {
  static const auto name = std::string{"system_information"};
  return name;
}

std::shared_ptr<Table> MetaSystemInformationTable::_on_generate() const {
  auto output_table = std::make_shared<Table>(_column_definitions, TableType::Data, std::nullopt, UseMvcc::Yes);

  const auto cpus = _cpu_count();
  const auto ram = _ram_size();
  const auto cpu_model = _cpu_model();

  output_table->append({static_cast<int32_t>(cpus), static_cast<int64_t>(ram), static_cast<pmr_string>(cpu_model)});

  return output_table;
}

// Returns the number of logical processors
size_t MetaSystemInformationTable::_cpu_count() {
#ifdef __linux__
  std::ifstream cpu_info_file;
  size_t processors = 0;
  try {
    cpu_info_file.open("/proc/cpuinfo", std::ifstream::in);
    std::string cpu_info_line;
    while (std::getline(cpu_info_file, cpu_info_line)) {
      if (cpu_info_line.starts_with("processor")) ++processors;
    }

    cpu_info_file.close();
  } catch (std::ios_base::failure& fail) {
    Fail("Failed to read /proc/cpuinfo (" + fail.what() + ")");
  }

  return processors;
#endif

#ifdef __APPLE__
  size_t processors;
  size_t size = sizeof(processors);
  const auto ret = sysctlbyname("hw.ncpu", &processors, &size, nullptr, 0);
  Assert(ret == 0, "Failed to call sysctl hw.ncpu");

  return processors;
#endif

  Fail("Method not implemented for this platform");
}

// Returns the physical memory size
size_t MetaSystemInformationTable::_ram_size() {
#ifdef __linux__
  struct sysinfo memory_info {};
  const auto ret = sysinfo(&memory_info);
  Assert(ret == 0, "Failed to get sysinfo");

  return memory_info.totalram * memory_info.mem_unit;
#endif

#ifdef __APPLE__
  uint64_t ram;
  size_t size = sizeof(ram);
  const auto ret = sysctlbyname("hw.memsize", &ram, &size, nullptr, 0);
  Assert(ret == 0, "Failed to call sysctl hw.memsize");

  return ram;
#endif

  Fail("Method not implemented for this platform");
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
    Fail("Failed to read /proc/cpuinfo (" + fail.what() + ")");
  }

  Fail("Could not read CPU model.");
#endif

#ifdef __APPLE__
  size_t buffer_size = 256;
  auto buffer = std::array<char, 256>{};
  const auto ret = sysctlbyname("machdep.cpu.brand_string", &buffer, &buffer_size, nullptr, 0);
  Assert(ret == 0, "Failed to call sysctl machdep.cpu.brand_string");

  return std::string(&buffer[0]);
#endif

  Fail("Method not implemented for this platform");
}

}  // namespace opossum
