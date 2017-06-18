#include <memory>
#include <string>
#include <utility>

#include "../base_test.hpp"
#include "SQLParser.h"
#include "gtest/gtest.h"

#include "scheduler/current_scheduler.hpp"
#include "scheduler/job_task.hpp"
#include "scheduler/node_queue_scheduler.hpp"
#include "scheduler/topology.hpp"
#include "sql/sql_query_plan_cache.hpp"
#include "sql/sql_query_operator.hpp"
#include "storage/storage_manager.hpp"

namespace opossum {

// The fixture for testing class GetTable.
class SQLQueryPlanTest : public BaseTest {
 protected:
  void SetUp() override {
    std::shared_ptr<Table> table_a = load_table("src/test/tables/int_float.tbl", 2);
    StorageManager::get().add_table("table_a", std::move(table_a));

    std::shared_ptr<Table> table_b = load_table("src/test/tables/int_float2.tbl", 2);
    StorageManager::get().add_table("table_b", std::move(table_b));
  }
};

TEST_F(SQLQueryPlanTest, SQLQueryPlanCloneTest) {
  std::string query1 = "SELECT * FROM table_a;";

  SQLQueryOperator op(query1, false);
  op.execute();

  // Get the query plan.
  const SQLQueryPlan& plan = op.get_query_plan();
  auto tasks = plan.tasks();
  ASSERT_EQ(2u, tasks.size());
  EXPECT_EQ("GetTable", tasks[0]->get_operator()->name());
  EXPECT_EQ("SQLResultOperator", tasks[1]->get_operator()->name());

  auto cloned_tasks = plan.cloneTasks();
  ASSERT_EQ(2u, cloned_tasks.size());
  EXPECT_EQ("GetTable", cloned_tasks[0]->get_operator()->name());
  EXPECT_EQ("SQLResultOperator", cloned_tasks[1]->get_operator()->name());

  // Execute both task lists.
  for (auto task : tasks) {
    task->execute();
  }

  for (auto task : cloned_tasks) {
    task->execute();
  }

  // Compare results.
  EXPECT_TABLE_EQ(tasks.back()->get_operator()->get_output(), cloned_tasks.back()->get_operator()->get_output());
}


TEST_F(SQLQueryPlanTest, SQLQueryPlanCloneWithSchedulerTest) {
  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_float_filtered.tbl", 2);

  std::string query1 = "SELECT * FROM table_a WHERE a >= 1234 AND b < 457.9;";

  // Generate query plan.
  SQLQueryOperator op(query1, false);
  op.execute();

  // Get the query plan template.
  const SQLQueryPlan& tmpl = op.get_query_plan();
  auto tmpl_tasks = tmpl.tasks();
  ASSERT_EQ(4u, tmpl_tasks.size());
  EXPECT_EQ("GetTable", tmpl_tasks[0]->get_operator()->name());
  EXPECT_EQ("TableScan", tmpl_tasks[1]->get_operator()->name());
  EXPECT_EQ("TableScan", tmpl_tasks[2]->get_operator()->name());
  EXPECT_EQ("SQLResultOperator", tmpl_tasks[3]->get_operator()->name());

  // Get a copy and schedule all tasks.
  CurrentScheduler::set(std::make_shared<NodeQueueScheduler>(Topology::create_fake_numa_topology(8, 4)));

  auto cloned_tasks = tmpl.cloneTasks();
  for (auto task : cloned_tasks) {
    task->schedule();
  }

  auto cloned_tasks2 = tmpl.cloneTasks();
  for (auto task : cloned_tasks2) {
    task->schedule();
  }

  CurrentScheduler::get()->finish();
  CurrentScheduler::set(nullptr);

  EXPECT_TABLE_EQ(cloned_tasks.back()->get_operator()->get_output(), expected_result);
  EXPECT_TABLE_EQ(cloned_tasks2.back()->get_operator()->get_output(), expected_result);
}

}  // namespace opossum
