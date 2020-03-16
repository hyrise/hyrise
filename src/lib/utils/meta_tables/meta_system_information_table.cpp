#ifdef __linux__

#include <numa.h>
#include <sys/sysinfo.h>

#endif

#include "meta_system_information_table.hpp"

#include "hyrise.hpp"

namespace opossum {

MetaSystemInformationTable::MetaSystemInformationTable()
    : AbstractMetaSystemTable(TableColumnDefinitions{{"cpu_count", DataType::Int, false},
                                                     {"ram", DataType::Long, false},
                                                     {"numa_cpu_count", DataType::Int, false}}) {}

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

  output_table->append({cpus, ram, numa_cpus});

  return output_table;
}

}  // namespace opossum
