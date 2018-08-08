#include <iostream>

#include "storage/storage_manager.hpp"
#include "storage/table.hpp"
#include "utils/abstract_plugin.hpp"

namespace opossum {

class TestPlugin : public AbstractPlugin {
  
public:
  static AbstractPlugin& get() {
    static TestPlugin instance;
    return instance;
  }

  const std::string description() const override { 
    return "This is the Hyrise TestPlugin";
  }

  void start() const override {
    TableColumnDefinitions column_definitions;
    column_definitions.emplace_back("col_1", DataType::Int);
    auto table = std::make_shared<Table>(column_definitions, TableType::Data);

    auto &sm = StorageManager::get();
    sm.add_table("DummyTable", table);
  }

  void stop() const override {
    auto &sm = StorageManager::get();
    sm.drop_table("DummyTable");
  }

};


// In the end, the plugin needs to be made usable from the outside.
EXPORT(TestPlugin);

}  // namespace opossum
