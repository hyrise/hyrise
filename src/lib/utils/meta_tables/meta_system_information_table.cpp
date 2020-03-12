#include "meta_system_information_table.hpp"

#include <chrono>
#include <fstream>
#include <iostream>

#include "sys/types.h"

#if defined __linux__

#include "sys/sysinfo.h"
#include "sys/times.h"

#elif defined __APPLE__

#include "mach/mach.h"
#include "mach/mach_error.h"
#include "mach/mach_host.h"
#include "mach/mach_init.h"
#include "mach/mach_time.h"
#include "mach/vm_map.h"
#include "mach/vm_statistics.h"
#include "sys/resource.h"
#include "sys/sysctl.h"

#endif


namespace opossum {

MetaSystemInformationTable::MetaSystemInformationTable()
    : AbstractMetaTable(TableColumnDefinitions{{"cpu_count", DataType::Int, false}, {"cpu_clock_speed", DataType::Int, false}, {"system_memory_total_bytes", DataType::Long, false}}) {}

const std::string& MetaSystemInformationTable::name() const {
  static const auto name = std::string{"system_information"};
  return name;
}

std::shared_ptr<Table> MetaSystemInformationTable::_on_generate() {
  auto output_table = std::make_shared<Table>(_column_definitions, TableType::Data, std::nullopt, UseMvcc::Yes);

  const MetaSystemUtilizationTable::SystemMemoryUsage system_memory_usage = _get_system_memory_usage();

  output_table->append({16, 2600, system_memory_usage.total_ram});

  return output_table;
}

MetaSystemUtilizationTable::SystemMemoryUsage MetaSystemInformationTable::_get_system_memory_usage() {
#if defined __linux__

  struct sysinfo memory_info;
  sysinfo(&memory_info);

  MetaSystemUtilizationTable::SystemMemoryUsage memory_usage;
  memory_usage.total_ram = memory_info.totalram * memory_info.mem_unit;
  memory_usage.total_swap = memory_info.totalswap * memory_info.mem_unit;
  memory_usage.free_ram = memory_info.freeram * memory_info.mem_unit;
  memory_usage.free_swap = memory_info.freeswap * memory_info.mem_unit;
  memory_usage.total_memory = memory_usage.total_ram + memory_usage.total_swap;
  memory_usage.free_memory = memory_usage.free_ram + memory_usage.free_swap;

  return memory_usage;

#elif defined __APPLE__

  int64_t physical_memory;
  size_t size = sizeof(physical_memory);
  if (sysctlbyname("hw.memsize", &physical_memory, &size, nullptr, 0) != 0) {
    Fail("Unable to call sysctl hw.memsize");
  }

  // Attention: total swap might change if more swap is needed
  xsw_usage swap_usage;
  size = sizeof(swap_usage);
  if (sysctlbyname("vm.swapusage", &swap_usage, &size, nullptr, 0) != 0) {
    Fail("Unable to call sysctl vm.swapusage");
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
  memory_usage.total_ram = physical_memory;
  memory_usage.total_swap = swap_usage.xsu_total;
  memory_usage.free_swap = swap_usage.xsu_avail;
  memory_usage.free_ram = vm_statistics.free_count * page_size;

  // auto used = (vm_statistics.active_couunt + vm_statistice.inactive_count + vm_statistics.wire_count) * page_size;

  return memory_usage;

#endif

  Fail("Method not implemented for this platform");
}

}  // namespace opossum
