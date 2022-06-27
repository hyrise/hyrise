#include "meta_memory_table.hpp"

#include "hyrise.hpp"

namespace opossum {

MetaMemoryTable::MetaMemoryTable()
    : AbstractMetaTable(TableColumnDefinitions{{"purpose", DataType::String, false},
                                               {"timestamp", DataType::Long, false},
                                               {"amount", DataType::Long, false}}) {}

const std::string& MetaMemoryTable::name() const {
  static const auto name = std::string{"memory"};
  return name;
}

std::shared_ptr<Table> MetaMemoryTable::_on_generate() const {
  auto output_table = std::make_shared<Table>(_column_definitions, TableType::Data, std::nullopt, UseMvcc::Yes);
  const auto memory_resources = Hyrise::get().memory_resource_manager.memory_resources();

  for (const auto& [purpose, memory_resource_ptr] : memory_resources) {
    for (const auto& [timestamp, amount] : memory_resource_ptr->memory_timeseries()) {
      output_table->append({pmr_string{purpose}, timestamp, static_cast<int64_t>(amount)});
    }
  }

  return output_table;
}

}  // namespace opossum
