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
#include "sql/sql_pipeline_builder.hpp"
#include "sql/sql_pipeline_statement.hpp"
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
  std::string query1 = "SELECT a FROM table_a;";

  auto pipeline_statement = SQLPipelineBuilder{query1}.disable_mvcc().create_pipeline_statement();

  // Get the query plan.
  const auto& plan1 = pipeline_statement.get_query_plan();
  auto tasks = plan1->create_tasks();
  ASSERT_EQ(2u, tasks.size());
  EXPECT_EQ("GetTable", tasks[0]->get_operator()->name());
  EXPECT_EQ("Projection", tasks[1]->get_operator()->name());

  const auto plan2 = plan1->deep_copy();
  auto cloned_tasks = plan2.create_tasks();
  ASSERT_EQ(2u, cloned_tasks.size());
  EXPECT_EQ("GetTable", cloned_tasks[0]->get_operator()->name());
  EXPECT_EQ("Projection", cloned_tasks[1]->get_operator()->name());

  // Execute both task lists.
  for (auto task : tasks) {
    task->execute();
  }

  for (auto task : cloned_tasks) {
    task->execute();
  }

  // Compare results.
  EXPECT_TABLE_EQ_UNORDERED(tasks.back()->get_operator()->get_output(),
                            cloned_tasks.back()->get_operator()->get_output());
}

TEST_F(SQLQueryPlanTest, SQLQueryPlanCloneWithSchedulerTest) {
  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_float_filtered.tbl", 2);

  std::string query1 = "SELECT * FROM table_a WHERE a >= 1234 AND b < 457.9;";

  // Generate query plan.
  auto pipeline_statement = SQLPipelineBuilder{query1}.disable_mvcc().create_pipeline_statement();

  // Get the query plan template.
  const auto& tmpl = pipeline_statement.get_query_plan();

  // Get a copy and schedule all tasks.
  Topology::use_fake_numa_topology(8, 4);
  CurrentScheduler::set(std::make_shared<NodeQueueScheduler>());

  auto cloned_tasks = tmpl->deep_copy().create_tasks();
  for (auto task : cloned_tasks) {
    task->schedule();
  }

  auto cloned_tasks2 = tmpl->deep_copy().create_tasks();
  for (auto task : cloned_tasks2) {
    task->schedule();
  }

  CurrentScheduler::get()->finish();
  CurrentScheduler::set(nullptr);

  EXPECT_TABLE_EQ_UNORDERED(cloned_tasks.back()->get_operator()->get_output(), expected_result);
  EXPECT_TABLE_EQ_UNORDERED(cloned_tasks2.back()->get_operator()->get_output(), expected_result);
}

}  // namespace opossum
