#include "test_plugin.hpp"

#include "storage/table.hpp"

namespace opossum {

std::string TestPlugin::description() const { return "This is the Hyrise TestPlugin"; }

void TestPlugin::start() {
  TableColumnDefinitions column_definitions;
  column_definitions.emplace_back("col_1", DataType::Int, false);
  auto table = std::make_shared<Table>(column_definitions, TableType::Data);

  storage_manager.add_table("DummyTable", table);
}

void TestPlugin::stop() { Hyrise::get().storage_manager.drop_table("DummyTable"); }

std::vector<std::pair<PluginFunctionName, PluginFunctionPointer>> TestPlugin::provided_user_executable_functions()
    const {
  return {{"OurFreelyChoosableFunctionName", [&]() { this->a_user_executable_function(); }},
          {"SpecialFunction17", [&]() { this->another_user_executable_function(); }}};
}

void TestPlugin::a_user_executable_function() const {
  TableColumnDefinitions column_definitions;
  column_definitions.emplace_back("col_A", DataType::Int, false);
  auto table = std::make_shared<Table>(column_definitions, TableType::Data);

  storage_manager.add_table("TableOfTestPlugin", table);
}

void TestPlugin::another_user_executable_function() const { std::cout << "This is never being called!" << std::endl; }

EXPORT_PLUGIN(TestPlugin)

}  // namespace opossum
