#include <iostream>

#include "storage/storage_manager.hpp"
#include "storage/table.hpp"
#include "utils/abstract_plugin.hpp"
#include "utils/singleton.hpp"

namespace opossum {

class TestPlugin : public AbstractPlugin, public Singleton<TestPlugin> {
 public:
  const std::string description() const override { return "This is the Hyrise TestPlugin"; }

  void start() const override {
    TableColumnDefinitions column_definitions;
    column_definitions.emplace_back("col_1", DataType::Int);
    auto table = std::make_shared<Table>(column_definitions, TableType::Data);

    StorageManager::get().add_table("DummyTable", table);
  }

  void stop() const override { StorageManager::get().drop_table("DummyTable"); }
};

EXPORT(TestPlugin)

}  // namespace opossum
