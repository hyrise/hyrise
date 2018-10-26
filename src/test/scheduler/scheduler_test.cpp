#include <memory>
#include <utility>
#include <vector>

#include "base_test.hpp"

#include "expression/binary_predicate_expression.hpp"
#include "expression/expression_functional.hpp"
#include "operators/get_table.hpp"
#include "operators/table_scan.hpp"
#include "scheduler/current_scheduler.hpp"
#include "scheduler/job_task.hpp"
#include "scheduler/node_queue_scheduler.hpp"
#include "scheduler/operator_task.hpp"
#include "scheduler/topology.hpp"
#include "storage/storage_manager.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

class SchedulerTest : public BaseTest {
 protected:
  void stress_linear_dependencies(std::atomic_uint& counter) {
    auto task1 = std::make_shared<JobTask>([&]() {
      auto current_value = 0u;
      auto successful = counter.compare_exchange_strong(current_value, 1u);
      ASSERT_TRUE(successful);
    });
    auto task2 = std::make_shared<JobTask>([&]() {
      auto current_value = 1u;
      auto successful = counter.compare_exchange_strong(current_value, 2u);
      ASSERT_TRUE(successful);
    });
    auto task3 = std::make_shared<JobTask>([&]() {
      auto current_value = 2u;
      auto successful = counter.compare_exchange_strong(current_value, 3u);
      ASSERT_TRUE(successful);
    });

    task1->set_as_predecessor_of(task2);
    task2->set_as_predecessor_of(task3);

    task3->schedule();
    task1->schedule();
    task2->schedule();
  }

  void stress_multiple_dependencies(std::atomic_uint& counter) {
    auto task1 = std::make_shared<JobTask>([&]() { counter += 1u; });
    auto task2 = std::make_shared<JobTask>([&]() { counter += 2u; });
    auto task3 = std::make_shared<JobTask>([&]() {
      auto current_value = 3u;
      auto successful = counter.compare_exchange_strong(current_value, 4u);
      ASSERT_TRUE(successful);
    });

    task1->set_as_predecessor_of(task3);
    task2->set_as_predecessor_of(task3);

    task3->schedule();
    task1->schedule();
    task2->schedule();
  }

  void stress_diamond_dependencies(std::atomic_uint& counter) {
    auto task1 = std::make_shared<JobTask>([&]() {
      auto current_value = 0u;
      auto successful = counter.compare_exchange_strong(current_value, 1u);
      ASSERT_TRUE(successful);
    });
    auto task2 = std::make_shared<JobTask>([&]() { counter += 2u; });
    auto task3 = std::make_shared<JobTask>([&]() { counter += 3u; });
    auto task4 = std::make_shared<JobTask>([&]() {
      auto current_value = 6u;
      auto successful = counter.compare_exchange_strong(current_value, 7u);
      ASSERT_TRUE(successful);
    });

    task1->set_as_predecessor_of(task2);
    task1->set_as_predecessor_of(task3);
    task2->set_as_predecessor_of(task4);
    task3->set_as_predecessor_of(task4);

    task4->schedule();
    task3->schedule();
    task1->schedule();
    task2->schedule();
  }

  void increment_counter_in_subtasks(std::atomic_uint& counter) {
    std::vector<std::shared_ptr<AbstractTask>> tasks;
    for (size_t i = 0; i < 10; i++) {
      auto task = std::make_shared<JobTask>([&]() {
        std::vector<std::shared_ptr<AbstractTask>> jobs;
        for (size_t j = 0; j < 3; j++) {
          auto job = std::make_shared<JobTask>([&]() { counter++; });

          job->schedule();
          jobs.emplace_back(job);
        }

        CurrentScheduler::wait_for_tasks(jobs);
      });
      task->schedule();
      tasks.emplace_back(task);
    }
  }
};

/**
 * Schedule some tasks with subtasks, make sure all of them finish
 */
TEST_F(SchedulerTest, BasicTest) {
  Topology::use_fake_numa_topology(8, 4);
  CurrentScheduler::set(std::make_shared<NodeQueueScheduler>());

  std::atomic_uint counter{0};

  increment_counter_in_subtasks(counter);

  CurrentScheduler::get()->finish();

  ASSERT_EQ(counter, 30u);

  CurrentScheduler::set(nullptr);
}

TEST_F(SchedulerTest, BasicTestWithoutScheduler) {
  std::atomic_uint counter{0};
  increment_counter_in_subtasks(counter);
  ASSERT_EQ(counter, 30u);
}

TEST_F(SchedulerTest, LinearDependenciesWithScheduler) {
  Topology::use_fake_numa_topology(8, 4);
  CurrentScheduler::set(std::make_shared<NodeQueueScheduler>());

  std::atomic_uint counter{0u};

  stress_linear_dependencies(counter);

  CurrentScheduler::get()->finish();

  ASSERT_EQ(counter, 3u);
}

TEST_F(SchedulerTest, MultipleDependenciesWithScheduler) {
  Topology::use_fake_numa_topology(8, 4);
  CurrentScheduler::set(std::make_shared<NodeQueueScheduler>());

  std::atomic_uint counter{0u};

  stress_multiple_dependencies(counter);

  CurrentScheduler::get()->finish();

  ASSERT_EQ(counter, 4u);
}

TEST_F(SchedulerTest, DiamondDependenciesWithScheduler) {
  Topology::use_fake_numa_topology(8, 4);
  CurrentScheduler::set(std::make_shared<NodeQueueScheduler>());

  std::atomic_uint counter{0};

  stress_diamond_dependencies(counter);

  CurrentScheduler::get()->finish();

  ASSERT_EQ(counter, 7u);
}

TEST_F(SchedulerTest, LinearDependenciesWithoutScheduler) {
  std::atomic_uint counter{0u};
  stress_linear_dependencies(counter);
  ASSERT_EQ(counter, 3u);
}

TEST_F(SchedulerTest, MultipleDependenciesWithoutScheduler) {
  std::atomic_uint counter{0u};
  stress_multiple_dependencies(counter);
  ASSERT_EQ(counter, 4u);
}

TEST_F(SchedulerTest, DiamondDependenciesWithoutScheduler) {
  std::atomic_uint counter{0};
  stress_diamond_dependencies(counter);
  ASSERT_EQ(counter, 7u);
}

TEST_F(SchedulerTest, MultipleOperators) {
  Topology::use_fake_numa_topology(8, 4);
  CurrentScheduler::set(std::make_shared<NodeQueueScheduler>());

  auto test_table = load_table("src/test/tables/int_float.tbl", 2);
  StorageManager::get().add_table("table", test_table);

  auto gt = std::make_shared<GetTable>("table");
  auto a = PQPColumnExpression::from_table(*test_table, ColumnID{0});
  auto ts = std::make_shared<TableScan>(gt, greater_than_equals_(a, 1234));

  auto gt_task = std::make_shared<OperatorTask>(gt, CleanupTemporaries::Yes);
  auto ts_task = std::make_shared<OperatorTask>(ts, CleanupTemporaries::Yes);
  gt_task->set_as_predecessor_of(ts_task);

  gt_task->schedule();
  ts_task->schedule();

  CurrentScheduler::get()->finish();

  auto expected_result = load_table("src/test/tables/int_float_filtered2.tbl", 1);
  EXPECT_TABLE_EQ_UNORDERED(ts->get_output(), expected_result);
}

}  // namespace opossum
