#include "test_plugin.hpp"

#include "storage/table.hpp"

namespace opossum {

const std::string TestPlugin::description() const { return "This is the Hyrise TestPlugin"; }

void TestPlugin::start() {
  TableColumnDefinitions column_definitions;
  column_definitions.emplace_back("col_1", DataType::Int);
  auto table = std::make_shared<Table>(column_definitions, TableType::Data);

  sm.add_table("DummyTable", table);
}

void TestPlugin::stop() { StorageManager::get().drop_table("DummyTable"); }

EXPORT_PLUGIN(TestPlugin)

}  // namespace opossum
