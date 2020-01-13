#include <memory>

#include "base_test.hpp"

#include "hyrise.hpp"
#include "operators/print.hpp"
#include "storage/table.hpp"
#include "utils/sqlite_add_indices.hpp"
#include "utils/sqlite_wrapper.hpp"

namespace opossum {

class SQLiteAddIndicesTest : public BaseTest {
 protected:
  void SetUp() override {
    TableColumnDefinitions column_definitions;
    column_definitions.emplace_back("column_1", DataType::Int, false);
    column_definitions.emplace_back("column_2", DataType::String, false);
    Hyrise::get().storage_manager.add_table("table_1", std::make_shared<Table>(column_definitions, TableType::Data, 2));

    stored_table = Hyrise::get().storage_manager.get_table("table_1");
    stored_table->append({13, "Hello,"});
    stored_table->append({37, "world"});

    sqlite_wrapper = std::make_shared<SQLiteWrapper>();
    sqlite_wrapper->create_sqlite_table(*stored_table, "table_1");
  }

  std::shared_ptr<Table> stored_table;
  std::shared_ptr<SQLiteWrapper> sqlite_wrapper;
};

TEST_F(SQLiteAddIndicesTest, AddIndexTest) {
  // DROP INDEX index_1 throws an exception if index_1 does not exist.
  EXPECT_THROW(sqlite_wrapper->main_connection.raw_execute_query("DROP INDEX index_1;", true), std::logic_error);
  auto sqlite_table = sqlite_wrapper->main_connection.execute_query("SELECT * FROM table_1");
  EXPECT_TABLE_EQ_ORDERED(stored_table, sqlite_table);
  const auto schema_file_path = "resources/test_data/sqlite_add_index_schema.sql";
  const auto create_index_file_path = "resources/test_data/sqlite_add_index_create_index.sql";
  add_indices_to_sqlite(schema_file_path, create_index_file_path, sqlite_wrapper);
  sqlite_table = sqlite_wrapper->main_connection.execute_query("SELECT * FROM table_1");
  EXPECT_TABLE_EQ_ORDERED(stored_table, sqlite_table);
  sqlite_wrapper->main_connection.raw_execute_query("DROP INDEX index_1;");
}

}  // namespace opossum
