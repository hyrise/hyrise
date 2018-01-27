
#include <memory>
#include <string>
#include <tuple>
#include <utility>

#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "sql/sql_query_operator.hpp"
#include "storage/storage_manager.hpp"

namespace opossum {

typedef std::tuple<uint32_t, std::string, std::string> SQLTestParam;

class SQLPrepareExecuteTest : public BaseTest, public ::testing::WithParamInterface<SQLTestParam> {
 protected:
  void SetUp() override {
    StorageManager::get().add_table("table_a", load_table("src/test/tables/int_float.tbl", 2));

    // Disable automatic caching.
    SQLQueryOperator::get_query_plan_cache().resize(0);
    SQLQueryOperator::get_parse_tree_cache().resize(0);
    SQLQueryOperator::get_prepared_statement_cache().clear();
    SQLQueryOperator::get_prepared_statement_cache().resize(1024);

    std::string prepare_statements[] = {"PREPARE a1 FROM 'SELECT * FROM table_a WHERE a >= 1234';",
                                        "PREPARE a2 FROM 'SELECT * FROM table_a WHERE a >= 1234 AND b < 457.9'",
                                        "PREPARE a3 FROM 'SELECT * FROM table_a WHERE a >= ?;';",
                                        "PREPARE a4 FROM 'SELECT * FROM table_a WHERE a >= ? AND b < ?';"};
    for (const auto& prepare_statement : prepare_statements) {
      std::make_shared<SQLQueryOperator>(prepare_statement, false, false)->execute();
    }
  }
};

TEST_P(SQLPrepareExecuteTest, GenericQueryTest) {
  const SQLTestParam param = GetParam();
  const std::string query = std::get<1>(param);
  const std::string expected_result_file = std::get<2>(param);

  auto op = std::make_shared<SQLQueryOperator>(query, false, false);
  op->execute();

  ASSERT_FALSE(op->parse_tree_cache_hit());
  ASSERT_FALSE(op->query_plan_cache_hit());

  const SQLQueryPlan& plan = op->get_query_plan();

  auto tasks = plan.create_tasks();
  for (const auto& task : tasks) {
    task->execute();
  }

  if (!expected_result_file.empty()) {
    auto expected_result = load_table(expected_result_file, 1);
    EXPECT_TABLE_EQ_UNORDERED(plan.tree_roots().back()->get_output(), expected_result);
  }
}

const SQLTestParam sql_query_tests[] = {
    // Unparameterized
    SQLTestParam{__LINE__, "EXECUTE a1;", "src/test/tables/int_float_filtered2.tbl"},
    SQLTestParam{__LINE__, "EXECUTE a2;", "src/test/tables/int_float_filtered.tbl"},
    SQLTestParam{__LINE__, "EXECUTE a1;", "src/test/tables/int_float_filtered2.tbl"},
    SQLTestParam{__LINE__, "EXECUTE a2;", "src/test/tables/int_float_filtered.tbl"},
    SQLTestParam{__LINE__, "EXECUTE a1;", "src/test/tables/int_float_filtered2.tbl"},
    SQLTestParam{__LINE__, "EXECUTE a2;", "src/test/tables/int_float_filtered.tbl"},

    // Parameterized
    SQLTestParam{__LINE__, "EXECUTE a3 (1234)", "src/test/tables/int_float_filtered2.tbl"},
    SQLTestParam{__LINE__, "EXECUTE a4 (1234, 457.9)", "src/test/tables/int_float_filtered.tbl"},
    SQLTestParam{__LINE__, "EXECUTE a4 (0, 500)", "src/test/tables/int_float.tbl"},
    SQLTestParam{__LINE__, "EXECUTE a4 (1234, 500)", "src/test/tables/int_float_filtered2.tbl"},
};

auto formatter = [](const testing::TestParamInfo<SQLTestParam> info) {
  return std::to_string(std::get<0>(info.param));
};
INSTANTIATE_TEST_CASE_P(GenericPrepareExecuteTest, SQLPrepareExecuteTest, ::testing::ValuesIn(sql_query_tests),
                        formatter);

}  // namespace opossum
