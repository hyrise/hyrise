#include "meta_temporary_memory_usage_table.hpp"

#include <magic_enum.hpp>

#include "hyrise.hpp"

namespace opossum {

MetaTemporaryMemoryUsageTable::MetaTemporaryMemoryUsageTable()
    : AbstractMetaTable(TableColumnDefinitions{{"operator_type", DataType::String, false},
                                               {"operator_data_structure", DataType::String, false},
                                               {"timestamp", DataType::Long, false},
                                               {"amount", DataType::Long, false}}) {}

const std::string& MetaTemporaryMemoryUsageTable::name() const {
  static const auto name = std::string{"memory"};
  return name;
}

std::shared_ptr<Table> MetaTemporaryMemoryUsageTable::_on_generate() const {
  auto output_table = std::make_shared<Table>(_column_definitions, TableType::Data, std::nullopt, UseMvcc::Yes);
  const auto memory_resources = Hyrise::get().memory_resource_manager.memory_resources();

  for (const auto& resource : memory_resources) {
    const auto operator_type = pmr_string{static_cast<std::string>(magic_enum::enum_name(resource.operator_type))};
    const auto operator_data_structure = pmr_string{resource.operator_data_structure};
    for (const auto& [timestamp, amount] : resource.resource_ptr->memory_timeseries()) {
      output_table->append({operator_type, operator_data_structure, timestamp, static_cast<int64_t>(amount)});
    }
  }

  return output_table;
}

}  // namespace opossum
