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

void SecondTestPlugin::stop() {
  auto& storage_manager = Hyrise::get().storage_manager;
  if (storage_manager.has_table("TableOfSecondTestPlugin")) {
    storage_manager.drop_table("TableOfSecondTestPlugin");
  }
}

std::vector<std::pair<PluginFunctionName, PluginFunctionPointer>>
SecondTestPlugin::provided_user_executable_functions() {
  return {{"OurFreelyChoosableFunctionName", [&]() {
             this->a_user_executable_function();
           }}};
}

void SecondTestPlugin::a_user_executable_function() {
  auto column_definitions = TableColumnDefinitions{};
  column_definitions.emplace_back("col_A", DataType::Int, false);
  const auto table = std::make_shared<Table>(column_definitions, TableType::Data);
  Hyrise::get().storage_manager.add_table("TableOfSecondTestPlugin", table);
}

EXPORT_PLUGIN(SecondTestPlugin);

}  // namespace hyrise
