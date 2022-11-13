#include "test_plugin.hpp"

#include "storage/table.hpp"

namespace hyrise {

std::string TestPlugin::description() const {
  return "This is the Hyrise TestPlugin";
}

void TestPlugin::start() {
  TableColumnDefinitions column_definitions;
  column_definitions.emplace_back("col_1", DataType::Int, false);
  auto table = std::make_shared<Table>(column_definitions, TableType::Data);

  storage_manager.add_table("DummyTable", table);
}

void TestPlugin::stop() {
  Hyrise::get().storage_manager.drop_table("DummyTable");
}

std::vector<std::pair<PluginFunctionName, PluginFunctionPointer>> TestPlugin::provided_user_executable_functions() {
  return {{"OurFreelyChoosableFunctionName", [&]() { this->a_user_executable_function(); }},
          {"SpecialFunction17", [&]() { hyrise::TestPlugin::a_static_user_executable_function(); }}};
}

void TestPlugin::a_user_executable_function() {
  TableColumnDefinitions column_definitions;
  column_definitions.emplace_back("col_A", DataType::Int, false);
  auto table = std::make_shared<Table>(column_definitions, TableType::Data);

  storage_manager.add_table("TableOfTestPlugin_" + std::to_string(_added_tables_count), table);
  ++_added_tables_count;
}

void TestPlugin::a_static_user_executable_function() {
  std::cout << "This is never being called!" << std::endl;
}

EXPORT_PLUGIN(TestPlugin)

}  // namespace hyrise
