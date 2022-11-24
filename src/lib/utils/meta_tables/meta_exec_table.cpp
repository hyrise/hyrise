#include "meta_exec_table.hpp"

#include <boost/algorithm/string.hpp>

#include "hyrise.hpp"

namespace hyrise {

MetaExecTable::MetaExecTable()
    : AbstractMetaTable(TableColumnDefinitions{{"plugin_name", DataType::String, false},
                                               {"function_name", DataType::String, false}}) {}

const std::string& MetaExecTable::name() const {
  static const auto name = std::string{"exec"};
  return name;
}

bool MetaExecTable::can_insert() const {
  return true;
}

std::shared_ptr<Table> MetaExecTable::_on_generate() const {
  auto output_table = std::make_shared<Table>(_column_definitions, TableType::Data, std::nullopt, UseMvcc::Yes);

  for (const auto& [key, _] : Hyrise::get().plugin_manager.user_executable_functions()) {
    const auto& [plugin_name, function_name] = key;
    output_table->append({pmr_string{plugin_name}, pmr_string{function_name}});
  }

  return output_table;
}

void MetaExecTable::_on_insert(const std::vector<AllTypeVariant>& values) {
  const auto plugin_name = PluginName{boost::get<pmr_string>(values.at(0))};
  const auto function_name = PluginFunctionName{boost::get<pmr_string>(values.at(1))};
  Hyrise::get().plugin_manager.exec_user_function(plugin_name, function_name);
}

}  // namespace hyrise
