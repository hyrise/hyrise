
#include <memory>
#include <string>
#include <tuple>
#include <utility>

#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "sql/sql_query_operator.hpp"
#include "storage/storage_manager.hpp"

namespace opossum {

typedef std::tuple<std::string, size_t, std::string> SQLTestParam;

class SQLPrepareExecuteTest : public BaseTest, public ::testing::WithParamInterface<SQLTestParam> {
 protected:
  void SetUp() override {
    std::shared_ptr<Table> table_a = load_table("src/test/tables/int_float.tbl", 2);
    StorageManager::get().add_table("table_a", std::move(table_a));

    // Load TPC-H tables.
    load_tpch_tables();

    // Disable automatic caching.
    SQLQueryOperator::get_query_plan_cache().clear_and_resize(0);
    SQLQueryOperator::get_parse_tree_cache().clear_and_resize(0);
  }

  void load_tpch_tables() {
    std::shared_ptr<Table> customer = load_table("src/test/tables/tpch/customer.tbl", 1);
    StorageManager::get().add_table("customer", customer);

    std::shared_ptr<Table> orders = load_table("src/test/tables/tpch/orders.tbl", 1);
    StorageManager::get().add_table("orders", orders);

    std::shared_ptr<Table> lineitem = load_table("src/test/tables/tpch/lineitem.tbl", 1);
    StorageManager::get().add_table("lineitem", lineitem);
  }
};

TEST_P(SQLPrepareExecuteTest, GenericQueryTest) {
  const SQLTestParam param = GetParam();
  const std::string query = std::get<0>(param);
  const size_t num_operators = std::get<1>(param);
  const std::string expected_result_file = std::get<2>(param);

  auto op = std::make_shared<SQLQueryOperator>(query, false);
  op->execute();

  ASSERT_FALSE(op->parse_tree_cache_hit());
  ASSERT_FALSE(op->query_plan_cache_hit());

  const SQLQueryPlan& plan = op->get_query_plan();

  ASSERT_EQ(num_operators, plan.size());

  for (const auto& task : plan.tasks()) {
    task->execute();
  }

  if (!expected_result_file.empty()) {
    auto expected_result = load_table(expected_result_file, 1);
    EXPECT_TABLE_EQ(plan.back()->get_operator()->get_output(), expected_result);
  }
}

const SQLTestParam sql_query_tests[] = {
    // Unparameterized
    SQLTestParam{"PREPARE a1 FROM 'SELECT * FROM table_a WHERE a >= 1234;'", 0u, ""},
    SQLTestParam{"PREPARE a2 FROM 'SELECT * FROM table_a WHERE a >= 1234 AND b < 457.9'", 0u, ""},

    SQLTestParam{"EXECUTE a1;", 3u, "src/test/tables/int_float_filtered2.tbl"},
    SQLTestParam{"EXECUTE a2;", 4u, "src/test/tables/int_float_filtered.tbl"},
    SQLTestParam{"EXECUTE a1;", 3u, "src/test/tables/int_float_filtered2.tbl"},
    SQLTestParam{"EXECUTE a2;", 4u, "src/test/tables/int_float_filtered.tbl"},
    SQLTestParam{"EXECUTE a1;", 3u, "src/test/tables/int_float_filtered2.tbl"},
    SQLTestParam{"EXECUTE a2;", 4u, "src/test/tables/int_float_filtered.tbl"},

    // Parameterized
    SQLTestParam{"PREPARE a3 FROM 'SELECT * FROM table_a WHERE a >= ?;'", 0u, ""},

    // TODO(mp): enable these tests as soon as TableStatistics support ValuePlaceholders.
    // Currently throwing exceptions.
    //    SQLTestParam{"PREPARE a4 FROM 'SELECT * FROM table_a WHERE a >= ? AND b < ?'", 0u, ""},
    //
    //    SQLTestParam{"EXECUTE a3 (1234)", 3u, "src/test/tables/int_float_filtered2.tbl"},
    //    SQLTestParam{"EXECUTE a4 (1234, 457.9)", 4u, "src/test/tables/int_float_filtered.tbl"},
    //    SQLTestParam{"EXECUTE a4 (0, 500)", 4u, "src/test/tables/int_float.tbl"},
    //    SQLTestParam{"EXECUTE a4 (1234, 500)", 4u, "src/test/tables/int_float_filtered2.tbl"},

    // TPC-H schema
    SQLTestParam{"PREPARE a5 FROM '"
                 "  SELECT customer.c_custkey, customer.c_name, COUNT(orderitems.\"orders.o_orderkey\")"
                 "    FROM customer"
                 "    JOIN (SELECT * FROM "
                 "      orders"
                 "      JOIN lineitem ON o_orderkey = l_orderkey"
                 "      WHERE orders.o_custkey = ?"
                 "    ) AS orderitems ON c_custkey = orders.o_custkey"
                 "    GROUP BY customer.c_custkey, customer.c_name"
                 "    HAVING COUNT(orderitems.\"orders.o_orderkey\") >= ?"
                 "';",
                 0u, ""},
    SQLTestParam{"EXECUTE a5 (0, 20);", 10u, ""}, SQLTestParam{"EXECUTE a5 (0, 21);", 10u, ""},
    SQLTestParam{"EXECUTE a5 (0, 22);", 10u, ""},
};

INSTANTIATE_TEST_CASE_P(GenericPrepareExecuteTest, SQLPrepareExecuteTest, ::testing::ValuesIn(sql_query_tests));

}  // namespace opossum
