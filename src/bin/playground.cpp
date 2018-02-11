#include <iostream>

#include "types.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"
#include "sql/sql_pipeline.hpp"
#include "utils/load_table.hpp"
#include "tpch/tpch_queries.hpp"
#include "../test/sql/sqlite_testrunner/sqlite_wrapper.hpp"

using namespace opossum;  // NOLINT

int main() {

  // TPCH query
//  {
//    const auto chunk_size = 100;

//    std::vector<std::string> tpch_table_names(
//      {"customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier"});

//    //auto sqlite_wrapper = std::make_shared<SQLiteWrapper>();

//    for (const auto& tpch_table_name : tpch_table_names) {
//      const auto tpch_table_path = std::string("src/test/tables/tpch/sf-0.001/") + tpch_table_name + ".tbl";
//      StorageManager::get().add_table(tpch_table_name, load_table(tpch_table_path, chunk_size));
//      //sqlite_wrapper->create_table_from_tbl(tpch_table_path, tpch_table_name);
//    }

//    const auto query = tpch_queries[16];
//    //const auto sqlite_result_table = sqlite_wrapper->execute_query(query);

//    // Don't use MVCC
//    SQLPipeline sql_pipeline{query, false};
//    const auto result_table = sql_pipeline.get_result_table();
//  }

  // Simple queries
  {
    auto table_a = load_table("src/test/tables/int_float.tbl", 2);
    StorageManager::get().add_table("table_a", table_a);

    auto table_b = load_table("src/test/tables/int_float2.tbl", 2);
    StorageManager::get().add_table("table_b", table_b);

    //SQLPipeline sql_pipeline{"SELECT * FROM table_a WHERE a = (SELECT MAX(a) FROM table_a)"};
    //SQLPipeline sql_pipeline{"SELECT * FROM table_a WHERE a > b"};
    SQLPipeline sql_pipeline{"SELECT a, (SELECT MAX(b) FROM table_a) FROM table_a"};
    const auto table = sql_pipeline.get_result_table();
  }

  return 0;
}
