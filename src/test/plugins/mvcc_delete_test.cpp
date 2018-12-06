#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "operators/sort.hpp"
#include "operators/table_wrapper.hpp"
#include "operators/update.hpp"
#include "storage/storage_manager.hpp"
#include "concurrency/transaction_manager.hpp"
#include "storage/reference_segment.hpp"
#include "utils/load_table.hpp"
#include "expression/expression_functional.hpp"
#include "operators/table_scan.hpp"
#include "operators/projection.hpp"
#include "operators/get_table.hpp"
#include "expression/pqp_column_expression.hpp"
#include "operators/validate.hpp"

#include "../utils/plugin_test_utils.hpp"
#include "utils/plugin_manager.hpp"

namespace opossum {

class MvccDeleteTest : public BaseTest {
 protected:
  void load_and_update_table(std::string name) { //TODO: Make it work.
    auto& sm = StorageManager::get();
    const auto table = load_table("src/test/tables/10_ints.tbl", 10);
    sm.add_table(name, table);

    //const auto& column_a = expression_functional::pqp_column_(ColumnID{0}, DataType::Int, false, "a");
    const auto& transaction_context = TransactionManager::get().new_transaction_context();
    const auto get_table = std::make_shared<GetTable>(name);

    get_table->set_transaction_context(transaction_context);
    get_table->execute();

    //const auto updated_values_projection = std::make_shared<Projection>(get_table, expression_functional::expression_vector(column_a, 0));
    //updated_values_projection->set_transaction_context(transaction_context);
    //updated_values_projection->execute();

    const auto& table_wrapper = std::make_shared<TableWrapper>(table);
    table_wrapper->execute();

    const auto& update = std::make_shared<Update>(name, get_table, table_wrapper);
    update->set_transaction_context(transaction_context);
    update->execute();
    transaction_context->commit();
  }

  void load_plugin() {
    auto& pm = PluginManager::get();
    pm.load_plugin(build_dylib_path("libMvccDelete"));
  }

  void unload_plugin() {
    auto& pm = PluginManager::get();
    pm.unload_plugin(build_dylib_path("MvccDelete"));
  }
};

TEST_F(MvccDeleteTest, LoadUnloadPlugin) {
  load_plugin();
  unload_plugin();
}

TEST_F(MvccDeleteTest, RemoveChunk) {
  load_and_update_table("test_table");
  //TODO: Check for number of Chunks, Values
  load_plugin();
  //TODO: Check for number of Chunks, Values
  unload_plugin();
}

} // namespace opossum