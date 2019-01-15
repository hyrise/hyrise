#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "../utils/plugin_test_utils.hpp"
#include "concurrency/transaction_manager.hpp"
#include "expression/expression_functional.hpp"
#include "expression/pqp_column_expression.hpp"
#include "operators/get_table.hpp"
#include "operators/table_scan.hpp"
#include "operators/update.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"
#include "utils/load_table.hpp"
#include "utils/plugin_manager.hpp"

namespace opossum {

class MvccDeleteTest : public BaseTest {
 protected:
  void load_and_update_table(const std::string& name, const uint8_t val) {
    auto& sm = StorageManager::get();
    const auto table = load_table("resources/test_data/tbl/10_ints.tbl", 10);
    sm.add_table(name, table);

    EXPECT_EQ(table->row_count(), 10);
    EXPECT_EQ(table->chunk_count(), 1);

    const auto& column_a = expression_functional::pqp_column_(ColumnID{0}, DataType::Int, false, "a");
    const auto& transaction_context = TransactionManager::get().new_transaction_context();

    const auto& get_table = std::make_shared<GetTable>(name);
    get_table->set_transaction_context(transaction_context);
    get_table->execute();

    const auto& where_scan =
        std::make_shared<TableScan>(get_table, expression_functional::greater_than_(column_a, val));
    where_scan->set_transaction_context(transaction_context);
    where_scan->execute();

    const auto& update = std::make_shared<Update>(name, where_scan, where_scan);
    update->set_transaction_context(transaction_context);
    update->execute();

    transaction_context->commit();
  }

  void load_plugin() {
    auto& pm = PluginManager::get();
    pm.load_plugin(build_dylib_path("MvccDeletePlugin"));
  }

  void unload_plugin() {
    auto& pm = PluginManager::get();
    pm.unload_plugin("MvccDeletePlugin");
  }
};

TEST_F(MvccDeleteTest, LoadUnloadPlugin) {
  load_plugin();
  unload_plugin();
}

TEST_F(MvccDeleteTest, RemoveInvalidChunk) {
  load_and_update_table("test_table", 0);

  auto& sm = StorageManager::get();
  const auto& table = sm.tables().find("test_table")->second;

  EXPECT_EQ(table->row_count(), 20);
  EXPECT_EQ(table->chunk_count(), 2);

  load_plugin();

  EXPECT_EQ(table->row_count(), 10);
  EXPECT_EQ(table->chunk_count(), 1);

  unload_plugin();

  sm.drop_table("test_table");
}

TEST_F(MvccDeleteTest, RemovePartialInvalidChunk) {
  load_and_update_table("test_table", 1);

  auto& sm = StorageManager::get();
  const auto& table = sm.tables().find("test_table")->second;

  EXPECT_EQ(table->row_count(), 19);
  EXPECT_EQ(table->chunk_count(), 2);

  load_plugin();

  EXPECT_EQ(table->row_count(), 10);
  EXPECT_EQ(table->chunk_count(), 1);

  unload_plugin();

  sm.drop_table("test_table");
}

}  // namespace opossum