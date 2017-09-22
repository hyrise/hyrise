#include <memory>
#include <string>
#include <utility>

#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "network/response_builder.hpp"
#include "operators/sort.hpp"
#include "scheduler/current_scheduler.hpp"
#include "scheduler/job_task.hpp"
#include "scheduler/node_queue_scheduler.hpp"
#include "scheduler/topology.hpp"
#include "sql/sql_query_operator.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"

namespace opossum {

// The fixture for testing class SQLQueryOperator.
class SQLQueryOperatorTest : public BaseTest {
 protected:
  void SetUp() override {
    CurrentScheduler::set(std::make_shared<NodeQueueScheduler>(Topology::create_fake_numa_topology(8, 4)));

    auto table_a = load_table("src/test/tables/int_float.tbl", 2);
    StorageManager::get().add_table("table_a", std::move(table_a));
    auto table_b = load_table("src/test/tables/int_float2.tbl", 2);
    StorageManager::get().add_table("table_b", std::move(table_b));

    SQLQueryOperator::get_query_plan_cache().clear_and_resize(0);
    SQLQueryOperator::get_parse_tree_cache().clear_and_resize(0);
  }

  void TearDown() override {
    CurrentScheduler::set(nullptr);  // Make sure there is no Scheduler anymore
  }
};

TEST_F(SQLQueryOperatorTest, BasicTest) {
  const std::string query = "SELECT * FROM table_a;";
  auto sql_op = std::make_shared<SQLQueryOperator>(query);
  auto sql_task = std::make_shared<OperatorTask>(sql_op);
  sql_task->schedule();

  CurrentScheduler::get()->finish();

  auto sql_result_task = sql_op->get_result_task();
  auto expected_result = load_table("src/test/tables/int_float.tbl", 2);
  EXPECT_TABLE_EQ(sql_result_task->get_operator()->get_output(), expected_result);
}

TEST_F(SQLQueryOperatorTest, NextTaskTest) {
  const std::string query = "SELECT a, b FROM table_a;";

  auto sql_op = std::make_shared<SQLQueryOperator>(query);
  auto sql_task = std::make_shared<OperatorTask>(sql_op);
  auto sql_result_task = sql_op->get_result_task();

  // Add sort to the result of the SQL query.
  auto sort = std::make_shared<Sort>(sql_result_task->get_operator(), ColumnID{0}, OrderByMode::Ascending);
  auto sort_task = std::make_shared<OperatorTask>(sort);
  sql_result_task->set_as_predecessor_of(sort_task);

  // Schedule.
  sort_task->schedule();
  sql_task->schedule();

  CurrentScheduler::get()->finish();

  auto expected_result = load_table("src/test/tables/int_float_sorted.tbl", 2);
  EXPECT_TABLE_EQ(sort->get_output(), expected_result, true);
}

// Similar to how it's done in request_handler.cpp
TEST_F(SQLQueryOperatorTest, NextAdHocTaskTest) {
  const std::string query = "SELECT a, b FROM table_a;";

  auto sql_op = std::make_shared<SQLQueryOperator>(query);
  auto sql_task = std::make_shared<OperatorTask>(sql_op);
  auto result_task = sql_op->get_result_task();
  auto result_operator = result_task->get_operator();

  auto materialize_job = std::make_shared<opossum::JobTask>([this, result_operator]() {
    // These lines are executed by the opossum scheduler
    auto table = result_operator->get_output();
    // Materialize and fill response
    proto::Response response;
    ResponseBuilder response_builder;
    response_builder.build_response(response, std::move(table));

    // send_response();
  });
  result_task->set_as_predecessor_of(materialize_job);

  // Schedule.
  materialize_job->schedule();
  sql_task->schedule();

  CurrentScheduler::get()->finish();
}

}  // namespace opossum
