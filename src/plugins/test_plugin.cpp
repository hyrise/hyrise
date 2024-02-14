#include "test_plugin.hpp"

#include <cstdint>
#include <iostream>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "all_type_variant.hpp"
#include "hyrise.hpp"
#include "storage/table.hpp"
#include "storage/table_column_definition.hpp"
#include "types.hpp"
#include "utils/abstract_plugin.hpp"

namespace hyrise {

std::string TestPlugin::description() const {
  return "This is the Hyrise TestPlugin";
}

void TestPlugin::start() {
  const auto column_definitions = TableColumnDefinitions{{"col_1", DataType::Int, false}};
  const auto table = std::make_shared<Table>(column_definitions, TableType::Data, std::nullopt, UseMvcc::Yes);

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
  const auto column_definitions = TableColumnDefinitions{{"col_A", DataType::Int, false}};
  const auto table = std::make_shared<Table>(column_definitions, TableType::Data, std::nullopt, UseMvcc::Yes);

  storage_manager.add_table("TableOfTestPlugin_" + std::to_string(_added_tables_count), table);
  ++_added_tables_count;
}

void TestPlugin::a_static_user_executable_function() {
  std::cout << "This is never being called!\n";
}

std::optional<PreBenchmarkHook> TestPlugin::pre_benchmark_hook() {
  return [&](auto& benchmark_item_runner) {
    const auto column_definitions = TableColumnDefinitions{{"item_id", DataType::Int, false}};
    const auto table = std::make_shared<Table>(column_definitions, TableType::Data, std::nullopt, UseMvcc::Yes);

    for (const auto item_id : benchmark_item_runner.items()) {
      table->append({static_cast<int32_t>(item_id)});
    }
    storage_manager.add_table("BenchmarkItems", table);
  };
}

std::optional<PostBenchmarkHook> TestPlugin::post_benchmark_hook() {
  return [&](auto& report) {
    storage_manager.drop_table("BenchmarkItems");
    report["dummy"] = 1;
  };
}

EXPORT_PLUGIN(TestPlugin);

}  // namespace hyrise
