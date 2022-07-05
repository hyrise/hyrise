#include "meta_memory_table.hpp"

#include "hyrise.hpp"

namespace opossum {

MetaMemoryTable::MetaMemoryTable()
    : AbstractMetaTable(TableColumnDefinitions{{"operator_type", DataType::String, false},
                                               {"operator_data_structure", DataType::String, false},
                                               {"timestamp", DataType::Long, false},
                                               {"amount", DataType::Long, false}}) {}

const std::string& MetaMemoryTable::name() const {
  static const auto name = std::string{"memory"};
  return name;
}

std::shared_ptr<Table> MetaMemoryTable::_on_generate() const {
  auto output_table = std::make_shared<Table>(_column_definitions, TableType::Data, std::nullopt, UseMvcc::Yes);
  const auto memory_resources = Hyrise::get().memory_resource_manager.memory_resources();

  for (const auto& [key, memory_resource_ptr] : memory_resources) {
    const auto& [operator_type, operator_data_structure] = key;
    const auto operator_type_str = pmr_string{static_cast<std::string>(magic_enum::enum_name(operator_type))};
    for (const auto& [timestamp, amount] : memory_resource_ptr->memory_timeseries()) {
      output_table->append(operator_type_str, pmr_string{operator_data_structure}, timestamp, static_cast<int64_t>(amount)});
    }
  }

  return output_table;
}

}  // namespace opossum
