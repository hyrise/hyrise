
#include <memory>
#include <string>
#include <utility>

#include "../base_test.hpp"
#include "SQLParser.h"
#include "gtest/gtest.h"

#include "operators/get_table.hpp"
#include "operators/table_scan.hpp"
#include "scheduler/node_queue_scheduler.hpp"
#include "scheduler/topology.hpp"
#include "sql/sql_query_translator.hpp"
#include "storage/storage_manager.hpp"

namespace opossum {

typedef std::tuple<std::string, size_t, std::string> SQLTestParam;

class SQLSelectTest : public BaseTest, public ::testing::WithParamInterface<SQLTestParam> {
 protected:
  void SetUp() override {
    std::shared_ptr<Table> table_a = load_table("src/test/tables/int_float.tbl", 2);
    StorageManager::get().add_table("table_a", std::move(table_a));

    std::shared_ptr<Table> table_b = load_table("src/test/tables/int_float2.tbl", 2);
    StorageManager::get().add_table("table_b", std::move(table_b));

    std::shared_ptr<Table> table_c = load_table("src/test/tables/int_string.tbl", 4);
    StorageManager::get().add_table("table_c", std::move(table_c));

    std::shared_ptr<Table> table_d = load_table("src/test/tables/string_int.tbl", 3);
    StorageManager::get().add_table("table_d", std::move(table_d));

    std::shared_ptr<Table> test_table2 = load_table("src/test/tables/int_string2.tbl", 2);
    StorageManager::get().add_table("TestTable", test_table2);
  }

  bool compile_query(const std::string query) {
    hsql::SQLParserResult parse_result;
    hsql::SQLParser::parseSQLString(query, &parse_result);

    if (!parse_result.isValid()) {
      return false;
    }

    // Compile the parse result.
    if (!_translator.translate_parse_result(parse_result)) {
      return false;
    }

    _plan = _translator.get_query_plan();
    return true;
  }

  void execute_query_plan() {
    for (const auto& task : _plan.tasks()) {
      task->get_operator()->execute();
    }
  }

  std::shared_ptr<const Table> get_plan_result() {
    return _plan.back()->get_operator()->get_output();
  }

  SQLQueryTranslator _translator;
  SQLQueryPlan _plan;
};

TEST_F(SQLSelectTest, BasicSuccessTest) {
  const std::string query = "SELECT * FROM test;";
  ASSERT_TRUE(compile_query(query));

  const std::string faulty_query = "SELECT * WHERE test;";
  ASSERT_FALSE(compile_query(faulty_query));
}

TEST_F(SQLSelectTest, SelectStarAllTest) {
  const std::string query = "SELECT * FROM table_a;";
  ASSERT_TRUE(compile_query(query));

  auto tasks = _translator.get_query_plan().tasks();
  ASSERT_EQ(1u, _plan.size());

  // Check GetTable task.
  auto get_table_task = tasks[0];
  auto get_table = (const std::shared_ptr<GetTable>&)get_table_task->get_operator();
  ASSERT_EQ("table_a", get_table->table_name());

  // Execute GetTable and check result.
  auto expected_result = load_table("src/test/tables/int_float.tbl", 1);
  get_table->execute();

  EXPECT_TABLE_EQ(get_table->get_output(), expected_result);
}



TEST_P(SQLSelectTest, GenericQueryTest) {
  // Inside a test, access the test parameter with the GetParam() method
  // of the TestWithParam<T> class:
  SQLTestParam param = GetParam();
  std::string query = std::get<0>(param);
  size_t num_operators = std::get<1>(param);
  std::string expected_result_file = std::get<2>(param);

  ASSERT_TRUE(compile_query(query));
  ASSERT_EQ(num_operators, _plan.size());
  execute_query_plan();

  auto expected_result = load_table(expected_result_file, 1);
  EXPECT_TABLE_EQ(get_plan_result(), expected_result);
}

const SQLTestParam sql_query_tests[] = {
  SQLTestParam{"SELECT * FROM table_a WHERE a >= 1234;", 2u, "src/test/tables/int_float_filtered2.tbl"},
  SQLTestParam{"SELECT * FROM table_a WHERE a >= 1234 AND b < 457.9", 3u, "src/test/tables/int_float_filtered.tbl"},
  SQLTestParam{"SELECT a FROM table_a;", 2u, "src/test/tables/int.tbl"},
  // TODO(torpedro): Enable this test, after implementing BETWEEN support in translator.
  // SQLTestParam{"SELECT * FROM TestTable WHERE a BETWEEN 122 AND 124", 2u, "src/test/tables/int_string_filtered.tbl"},
  SQLTestParam{"SELECT a, b FROM table_a ORDER BY a;", 3u, "src/test/tables/int_float_sorted.tbl"},
  SQLTestParam{"SELECT a FROM (SELECT a, b FROM table_a WHERE a > 1 ORDER BY b) WHERE a > 0 ORDER BY a;", 7u, "src/test/tables/int.tbl"},
  SQLTestParam{"SELECT \"left\".a, \"left\".b, \"right\".a, \"right\".b FROM table_a AS \"left\" JOIN table_b AS \"right\" ON a = a;", 4u, "src/test/tables/joinoperators/int_inner_join.tbl"},
  SQLTestParam{"SELECT * FROM table_a AS \"left\" LEFT JOIN table_b AS \"right\" ON a = a;", 3u, "src/test/tables/joinoperators/int_left_join.tbl"},
};

INSTANTIATE_TEST_CASE_P(GenericQueryTest,
                        SQLSelectTest,
                        ::testing::ValuesIn(sql_query_tests));


TEST_F(SQLSelectTest, SelectWithSchedulerTest) {
  const std::string query =
      "SELECT \"left\".a, \"left\".b, \"right\".a, \"right\".b FROM table_a AS \"left\" INNER JOIN table_b AS "
      "\"right\" ON a = a";
  auto expected_result = load_table("src/test/tables/joinoperators/int_inner_join.tbl", 1);

  // TODO(torpedro): Adding 'WHERE \"left\".a >= 0;' causes wrong data. Investigate.
  //                 Probable bug in TableScan.

  CurrentScheduler::set(std::make_shared<NodeQueueScheduler>(Topology::create_fake_numa_topology(8, 4)));

  ASSERT_TRUE(compile_query(query));

  for (const auto& task : _plan.tasks()) {
    task->schedule();
  }

  CurrentScheduler::get()->finish();
  CurrentScheduler::set(nullptr);

  EXPECT_TABLE_EQ(get_plan_result(), expected_result, true);
}

}  // namespace opossum
