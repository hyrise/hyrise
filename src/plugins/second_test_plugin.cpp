#include "second_test_plugin.hpp"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "all_type_variant.hpp"
#include "storage/table.hpp"
#include "storage/table_column_definition.hpp"
#include "types.hpp"
#include "utils/abstract_plugin.hpp"

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
  auto column_definitions = TableColumnDefinitions{};
  column_definitions.emplace_back("col_A", DataType::Int, false);
  auto table = std::make_shared<Table>(column_definitions, TableType::Data);

  storage_manager.add_table("TableOfSecondTestPlugin", table);
}

EXPORT_PLUGIN(SecondTestPlugin);

}  // namespace hyrise
