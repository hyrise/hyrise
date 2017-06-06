
#include <memory>
#include <string>
#include <utility>

#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "operators/get_table.hpp"
#include "operators/table_scan.hpp"
#include "scheduler/node_queue_scheduler.hpp"
#include "scheduler/topology.hpp"
#include "sql/sql_query_translator.hpp"
#include "storage/storage_manager.hpp"

namespace opossum {

class SQLSelectTest : public BaseTest {
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

  SQLQueryTranslator _translator;
};

TEST_F(SQLSelectTest, BasicSuccessTest) {
  const std::string query = "SELECT * FROM test;";
  ASSERT_TRUE(_translator.translate_query(query));

  const std::string faulty_query = "SELECT * WHERE test;";
  ASSERT_FALSE(_translator.translate_query(faulty_query));
}

TEST_F(SQLSelectTest, SelectStarAllTest) {
  const std::string query = "SELECT * FROM table_a;";
  ASSERT_TRUE(_translator.translate_query(query));

  auto tasks = _translator.get_query_plan().tasks();
  ASSERT_EQ(1u, tasks.size());

  // Check GetTable task.
  auto get_table_task = tasks[0];
  auto get_table = (const std::shared_ptr<GetTable>&)get_table_task->get_operator();
  ASSERT_EQ("table_a", get_table->table_name());

  // Execute GetTable and check result.
  auto expected_result = load_table("src/test/tables/int_float.tbl", 1);
  get_table->execute();

  EXPECT_TABLE_EQ(get_table->get_output(), expected_result);
}

TEST_F(SQLSelectTest, SelectWithSingleCondition) {
  const std::string query = "SELECT * FROM table_a WHERE a >= 1234;";
  ASSERT_TRUE(_translator.translate_query(query));

  auto tasks = _translator.get_query_plan().tasks();
  ASSERT_EQ(2u, tasks.size());

  auto get_table = (const std::shared_ptr<GetTable>&)tasks[0]->get_operator();
  auto table_scan = (const std::shared_ptr<TableScan>&)tasks[1]->get_operator();

  get_table->execute();
  table_scan->execute();

  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_float_filtered2.tbl", 1);
  EXPECT_TABLE_EQ(table_scan->get_output(), expected_result);
}

TEST_F(SQLSelectTest, SelectWithAndCondition) {
  const std::string query = "SELECT * FROM table_a WHERE a >= 1234 AND b < 457.9";
  ASSERT_TRUE(_translator.translate_query(query));

  auto tasks = _translator.get_query_plan().tasks();
  ASSERT_EQ(3u, tasks.size());

  for (const auto task : tasks) {
    task->get_operator()->execute();
  }

  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_float_filtered.tbl", 2);
  EXPECT_TABLE_EQ(tasks.back()->get_operator()->get_output(), expected_result);
}

// TEST_F(SQLSelectTest, SelectWithBetween) {
//   const std::string query = "SELECT * FROM TestTable WHERE a BETWEEN 122 AND 124";
//   ASSERT_TRUE(_translator.translate_query(query));

//   auto tasks = _translator.get_query_plan().tasks();
//   ASSERT_EQ(2u, tasks.size());

//   for (const auto task : tasks) {
//     task->get_operator()->execute();
//   }

//   std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_string_filtered.tbl", 2);
//   EXPECT_TABLE_EQ(tasks.back()->get_operator()->get_output(), expected_result);
// }

TEST_F(SQLSelectTest, SimpleProjectionTest) {
  const std::string query = "SELECT a FROM table_a;";
  ASSERT_TRUE(_translator.translate_query(query));

  auto tasks = _translator.get_query_plan().tasks();
  ASSERT_EQ(2u, tasks.size());

  for (const auto task : tasks) {
    task->get_operator()->execute();
  }

  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int.tbl", 2);
  EXPECT_TABLE_EQ(tasks.back()->get_operator()->get_output(), expected_result);
}

TEST_F(SQLSelectTest, SelectSingleOrderByTest) {
  const std::string query = "SELECT a, b FROM table_a ORDER BY a;";
  ASSERT_TRUE(_translator.translate_query(query));

  auto tasks = _translator.get_query_plan().tasks();
  ASSERT_EQ(3u, tasks.size());

  for (const auto task : tasks) {
    task->get_operator()->execute();
  }

  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_float_sorted.tbl", 2);
  EXPECT_TABLE_EQ(tasks.back()->get_operator()->get_output(), expected_result, true);
}

TEST_F(SQLSelectTest, SelectFromSubSelect) {
  const std::string query = "SELECT a FROM (SELECT a, b FROM table_a WHERE a > 1 ORDER BY b) WHERE a > 0 ORDER BY a;";
  ASSERT_TRUE(_translator.translate_query(query));

  auto tasks = _translator.get_query_plan().tasks();
  ASSERT_EQ(7u, tasks.size());

  for (const auto task : tasks) {
    task->get_operator()->execute();
  }

  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int.tbl", 2);
  EXPECT_TABLE_EQ(tasks.back()->get_operator()->get_output(), expected_result, true);
}

TEST_F(SQLSelectTest, SelectBasicInnerJoinTest) {
  const std::string query =
      "SELECT \"left\".a, \"left\".b, \"right\".a, \"right\".b FROM table_a AS \"left\" JOIN table_b AS \"right\" ON a "
      "= a;";
  ASSERT_TRUE(_translator.translate_query(query));

  auto tasks = _translator.get_query_plan().tasks();
  ASSERT_EQ(4u, tasks.size());

  for (const auto task : tasks) {
    task->get_operator()->execute();
  }

  std::shared_ptr<Table> expected_result = load_table("src/test/tables/joinoperators/int_inner_join.tbl", 1);
  EXPECT_TABLE_EQ(tasks.back()->get_operator()->get_output(), expected_result);
}

TEST_F(SQLSelectTest, SelectBasicLeftJoinTest) {
  const std::string query = "SELECT * FROM table_a AS \"left\" LEFT JOIN table_b AS \"right\" ON a = a;";
  ASSERT_TRUE(_translator.translate_query(query));

  auto tasks = _translator.get_query_plan().tasks();
  ASSERT_EQ(3u, tasks.size());

  for (const auto task : tasks) {
    task->get_operator()->execute();
  }

  std::shared_ptr<Table> expected_result = load_table("src/test/tables/joinoperators/int_left_join.tbl", 1);
  EXPECT_TABLE_EQ(tasks.back()->get_operator()->get_output(), expected_result);
}

TEST_F(SQLSelectTest, SelectWithSchedulerTest) {
  CurrentScheduler::set(std::make_shared<NodeQueueScheduler>(Topology::create_fake_numa_topology(8, 4)));

  const std::string query =
      "SELECT \"left\".a, \"left\".b, \"right\".a, \"right\".b FROM table_a AS \"left\" INNER JOIN table_b AS "
      "\"right\" ON a = a";

  // TODO(torpedro): Adding 'WHERE \"left\".a >= 0;' causes wrong data. Investigate.
  //                 Probable bug in TableScan.

  ASSERT_TRUE(_translator.translate_query(query));

  auto tasks = _translator.get_query_plan().tasks();

  for (const auto& task : tasks) {
    task->schedule();
  }

  CurrentScheduler::get()->finish();
  CurrentScheduler::set(nullptr);

  std::shared_ptr<Table> expected_result = load_table("src/test/tables/joinoperators/int_inner_join.tbl", 1);
  EXPECT_TABLE_EQ(tasks.back()->get_operator()->get_output(), expected_result, true);
}

}  // namespace opossum
