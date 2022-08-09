#include "meta_temporary_memory_usage_table.hpp"

#include <chrono>
#include <magic_enum.hpp>

#include "hyrise.hpp"

namespace opossum {

MetaTemporaryMemoryUsageTable::MetaTemporaryMemoryUsageTable()
    : AbstractMetaTable(TableColumnDefinitions{{"operator_type", DataType::String, false},
                                               {"operator_data_structure", DataType::String, false},
                                               {"timestamp", DataType::Long, false},
                                               {"memory_consumption_in_bytes", DataType::Long, false}}) {}

const std::string& MetaTemporaryMemoryUsageTable::name() const {
  static const auto name = std::string{"temporary_memory_usage"};
  return name;
}

std::shared_ptr<Table> MetaTemporaryMemoryUsageTable::_on_generate() const {
  auto output_table = std::make_shared<Table>(_column_definitions, TableType::Data);
  const auto& memory_resources = Hyrise::get().memory_resource_manager.memory_resources();

  for (const auto& resource : memory_resources) {
    const auto operator_type = pmr_string{magic_enum::enum_name(resource.operator_type)};
    const auto operator_data_structure = pmr_string{resource.operator_data_structure};
    for (const auto& [timestamp, memory_consumption_in_bytes] : resource.resource_pointer->memory_timeseries()) {
      const auto timestamp_ns = std::chrono::nanoseconds{timestamp.time_since_epoch()}.count();
      output_table->append({operator_type, operator_data_structure, timestamp_ns, memory_consumption_in_bytes});
    }
  }

  return output_table;
}

}  // namespace opossum
