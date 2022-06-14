#include "meta_memory_table.hpp"

#include "hyrise.hpp"

namespace opossum {

MetaMemoryTable::MetaMemoryTable()
    : AbstractMetaTable(TableColumnDefinitions{{"purpose", DataType::String, false},
                                               {"amount", DataType::Long, false}}) {}

const std::string& MetaMemoryTable::name() const {
  static const auto name = std::string{"memory"};
  return name;
}

std::shared_ptr<Table> MetaMemoryTable::_on_generate() const {
  auto output_table = std::make_shared<Table>(_column_definitions, TableType::Data, std::nullopt, UseMvcc::Yes);

  for (const auto& [purpose, amount] : Hyrise::get().memory_resource_manager.get_current_memory_usage()) {
    output_table->append({pmr_string{purpose}, static_cast<int64_t>(amount)});
  }

  return output_table;
}

}  // namespace opossum
