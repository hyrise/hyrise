#include "second_test_plugin.hpp"

#include "storage/table.hpp"

namespace hyrise {

std::string SecondTestPlugin::description() const {
  return "This is the Hyrise SecondTestPlugin";
}

void SecondTestPlugin::start() {}

void SecondTestPlugin::stop() {}

std::vector<std::pair<PluginFunctionName, PluginFunctionPointer>>
SecondTestPlugin::provided_user_executable_functions() {
  return {{"OurFreelyChoosableFunctionName", [&]() { this->a_user_executable_function(); }}};
}

void SecondTestPlugin::a_user_executable_function() const {
  TableColumnDefinitions column_definitions;
  column_definitions.emplace_back("col_A", DataType::Int, false);
  auto table = std::make_shared<Table>(column_definitions, TableType::Data);

  storage_manager.add_table("TableOfSecondTestPlugin", table);
}

EXPORT_PLUGIN(SecondTestPlugin)

}  // namespace hyrise
