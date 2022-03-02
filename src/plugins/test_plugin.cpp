#include <iostream>

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

void TestPlugin::test_user_callable_function() const {
  std::cout << "This output was triggered by user interaction." << std::endl;
}

EXPORT_USER_CALLABLE_FUNCTION(TestPlugin, test_user_callable_function)

EXPORT_PLUGIN(TestPlugin)

}  // namespace opossum
