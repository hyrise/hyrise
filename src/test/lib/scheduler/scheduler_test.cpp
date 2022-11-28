#include <chrono>
#include <memory>
#include <utility>
#include <vector>

#include "base_test.hpp"

#include "expression/binary_predicate_expression.hpp"
#include "expression/expression_functional.hpp"
#include "hyrise.hpp"
#include "operators/get_table.hpp"
#include "operators/table_scan.hpp"
#include "scheduler/immediate_execution_scheduler.hpp"
#include "scheduler/job_task.hpp"
#include "scheduler/node_queue_scheduler.hpp"
#include "scheduler/operator_task.hpp"
#include "scheduler/task_queue.hpp"

using namespace hyrise::expression_functional;  // NOLINT

namespace hyrise {

class SchedulerTest : public BaseTest {
 protected:
  void stress_linear_dependencies(std::atomic_uint32_t& counter) {
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

  void stress_multiple_dependencies(std::atomic_uint32_t& counter) {
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

  void stress_diamond_dependencies(std::atomic_uint32_t& counter) {
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

  void increment_counter_in_subtasks(std::atomic_uint32_t& counter) {
    std::vector<std::shared_ptr<AbstractTask>> tasks;
    for (auto outer_counter = size_t{0}; outer_counter < 10; outer_counter++) {
      auto task = std::make_shared<JobTask>([&]() {
        std::vector<std::shared_ptr<AbstractTask>> jobs;
        for (auto inner_counter = size_t{0}; inner_counter < 3; inner_counter++) {
          auto job = std::make_shared<JobTask>([&]() { ++counter; });

          job->schedule();
          jobs.emplace_back(job);
        }

        Hyrise::get().scheduler()->wait_for_tasks(jobs);
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
  Hyrise::get().topology.use_fake_numa_topology(8, 4);
  Hyrise::get().set_scheduler(std::make_shared<NodeQueueScheduler>());

  std::atomic_uint32_t counter{0};

  increment_counter_in_subtasks(counter);

  Hyrise::get().scheduler()->finish();

  ASSERT_EQ(counter, 30u);

  Hyrise::get().set_scheduler(std::make_shared<ImmediateExecutionScheduler>());
}

TEST_F(SchedulerTest, BasicTestWithoutScheduler) {
  std::atomic_uint32_t counter{0};
  increment_counter_in_subtasks(counter);
  ASSERT_EQ(counter, 30u);
}

TEST_F(SchedulerTest, LinearDependenciesWithScheduler) {
  Hyrise::get().topology.use_fake_numa_topology(8, 4);
  Hyrise::get().set_scheduler(std::make_shared<NodeQueueScheduler>());

  std::atomic_uint32_t counter{0u};

  stress_linear_dependencies(counter);

  Hyrise::get().scheduler()->finish();

  ASSERT_EQ(counter, 3u);
}

TEST_F(SchedulerTest, Grouping) {
  // Tests the grouping described in AbstractScheduler::schedule_and_wait_for_tasks and
  // NodeQueueScheduler::_group_tasks. Also tests that successor tasks are called immediately after their dependencies
  // finish. Not really a multi-threading test, though.
  Hyrise::get().topology.use_fake_numa_topology(1, 1);
  Hyrise::get().set_scheduler(std::make_shared<NodeQueueScheduler>());

  auto output = std::vector<size_t>{};
  auto tasks = std::vector<std::shared_ptr<AbstractTask>>{};

  constexpr auto TASK_COUNT = 50;

  for (auto task_id = 0; task_id < TASK_COUNT; ++task_id) {
    tasks.emplace_back(std::make_shared<JobTask>([&output, task_id] { output.emplace_back(task_id); }));
  }
  Hyrise::get().scheduler()->schedule_and_wait_for_tasks(tasks);
  Hyrise::get().scheduler()->finish();

  // We expect NUM_GROUPS chains of tasks to be created. As tasks are added to the chains by calling
  // AbstractTask::set_predecessor_of, the first task in the input vector ends up being the last task being called. This
  // results in [40 30 20 10 0 41 31 21 11 1 ...]
  const auto num_groups = NodeQueueScheduler::NUM_GROUPS;
  EXPECT_EQ(TASK_COUNT % num_groups, 0);
  auto expected_output = std::vector<size_t>{};
  for (auto group = 0; group < num_groups; ++group) {
    for (auto task_id = 0; task_id < TASK_COUNT / num_groups; ++task_id) {
      expected_output.emplace_back(tasks.size() - (task_id + 1) * num_groups + group);
    }
  }

  EXPECT_EQ(output, expected_output);
}

TEST_F(SchedulerTest, MultipleDependenciesWithScheduler) {
  Hyrise::get().topology.use_fake_numa_topology(8, 4);
  Hyrise::get().set_scheduler(std::make_shared<NodeQueueScheduler>());

  std::atomic_uint32_t counter{0u};

  stress_multiple_dependencies(counter);

  Hyrise::get().scheduler()->finish();

  ASSERT_EQ(counter, 4u);
}

TEST_F(SchedulerTest, DiamondDependenciesWithScheduler) {
  Hyrise::get().topology.use_fake_numa_topology(8, 4);
  Hyrise::get().set_scheduler(std::make_shared<NodeQueueScheduler>());

  std::atomic_uint32_t counter{0};

  stress_diamond_dependencies(counter);

  Hyrise::get().scheduler()->finish();

  ASSERT_EQ(counter, 7u);
}

TEST_F(SchedulerTest, LinearDependenciesWithoutScheduler) {
  std::atomic_uint32_t counter{0u};
  stress_linear_dependencies(counter);
  ASSERT_EQ(counter, 3u);
}

TEST_F(SchedulerTest, MultipleDependenciesWithoutScheduler) {
  std::atomic_uint32_t counter{0u};
  stress_multiple_dependencies(counter);
  ASSERT_EQ(counter, 4u);
}

TEST_F(SchedulerTest, DiamondDependenciesWithoutScheduler) {
  std::atomic_uint32_t counter{0};
  stress_diamond_dependencies(counter);
  ASSERT_EQ(counter, 7u);
}

TEST_F(SchedulerTest, MultipleOperators) {
  Hyrise::get().topology.use_fake_numa_topology(8, 4);
  Hyrise::get().set_scheduler(std::make_shared<NodeQueueScheduler>());

  auto test_table = load_table("resources/test_data/tbl/int_float.tbl", ChunkOffset{2});
  Hyrise::get().storage_manager.add_table("table", test_table);

  auto gt = std::make_shared<GetTable>("table");
  auto a = PQPColumnExpression::from_table(*test_table, ColumnID{0});
  auto ts = std::make_shared<TableScan>(gt, greater_than_equals_(a, 1234));

  auto gt_task = std::make_shared<OperatorTask>(gt);
  auto ts_task = std::make_shared<OperatorTask>(ts);
  gt_task->set_as_predecessor_of(ts_task);

  gt_task->schedule();
  ts_task->schedule();

  Hyrise::get().scheduler()->finish();

  auto expected_result = load_table("resources/test_data/tbl/int_float_filtered2.tbl", ChunkOffset{1});
  EXPECT_TABLE_EQ_UNORDERED(ts->get_output(), expected_result);
}

TEST_F(SchedulerTest, VerifyTaskQueueSetup) {
  if (std::thread::hardware_concurrency() < 4) {
    // If the machine has less than 4 cores, the calls to use_non_numa_topology()
    // below will implicitly reduce the worker count to the number of cores,
    // therefore failing the assertions.
    GTEST_SKIP();
  }
  Hyrise::get().topology.use_non_numa_topology(4);
  Hyrise::get().set_scheduler(std::make_shared<NodeQueueScheduler>());
  EXPECT_EQ(1, Hyrise::get().scheduler()->queues().size());

  Hyrise::get().topology.use_fake_numa_topology(4);
  Hyrise::get().set_scheduler(std::make_shared<NodeQueueScheduler>());
  EXPECT_EQ(4, Hyrise::get().scheduler()->queues().size());

  Hyrise::get().topology.use_fake_numa_topology(4, 2);
  Hyrise::get().set_scheduler(std::make_shared<NodeQueueScheduler>());
  EXPECT_EQ(2, Hyrise::get().scheduler()->queues().size());

  Hyrise::get().topology.use_fake_numa_topology(4, 4);
  Hyrise::get().set_scheduler(std::make_shared<NodeQueueScheduler>());
  EXPECT_EQ(1, Hyrise::get().scheduler()->queues().size());

  Hyrise::get().scheduler()->finish();
}

TEST_F(SchedulerTest, TaskToNodeAssignment) {
  if (std::thread::hardware_concurrency() < 2) {
    GTEST_SKIP();
  }

  Hyrise::get().topology.use_fake_numa_topology(2, 1);
  auto node_queue_scheduler = std::make_shared<NodeQueueScheduler>();
  Hyrise::get().set_scheduler(node_queue_scheduler);
  EXPECT_EQ(2, node_queue_scheduler->queues().size());

  auto task_1 = std::make_shared<JobTask>([&]() {}, SchedulePriority::Default, false);
  auto task_2 = std::make_shared<JobTask>([&]() {}, SchedulePriority::Default, false);
  auto task_3 = std::make_shared<JobTask>([&]() {}, SchedulePriority::Default, false);
  auto task_4 = std::make_shared<JobTask>([&]() {}, SchedulePriority::Default, false);

  task_1->schedule(NodeID{0});
  task_2->schedule(NodeID{0});
  task_3->schedule(NodeID{0});
  task_4->schedule(NodeID{1});

  node_queue_scheduler->wait_for_all_tasks();

  EXPECT_EQ(node_queue_scheduler->workers()[0]->num_finished_tasks(), 3);
  EXPECT_EQ(node_queue_scheduler->workers()[1]->num_finished_tasks(), 1);
}

TEST_F(SchedulerTest, TaskToIdlingNodeAssigment) {
  if (std::thread::hardware_concurrency() < 2) {
    GTEST_SKIP();
  }

  Hyrise::get().topology.use_fake_numa_topology(2, 1);
  auto node_queue_scheduler = std::make_shared<NodeQueueScheduler>();
  Hyrise::get().set_scheduler(node_queue_scheduler);

  // Just a sufficiently large number to trigger 
  constexpr auto JOB_COUNT = 100;
  constexpr auto LOOP_TIME = std::chrono::microseconds{50};

  auto jobs = std::vector<std::shared_ptr<AbstractTask>>{};
  jobs.reserve(JOB_COUNT);
  for (auto task_count = size_t{0}; task_count < JOB_COUNT; ++task_count) {
    jobs.push_back(std::make_shared<JobTask>([&]() {
      // Jobs loop for a short time to ensure that jobs are not already taken out of the queue before the next job is
      // scheduled.
      const auto start = std::chrono::steady_clock::now();

      auto counter = size_t{0};
      while ((std::chrono::steady_clock::now() - start) < LOOP_TIME) {
        ++counter;
      }
    }));
  }
  node_queue_scheduler->schedule_and_wait_for_tasks(jobs);

  // On a modern CPU, LOOP_TIME should ensure that we almost always end up with 50/50. So >40 is a conservative check.
  EXPECT_GT(node_queue_scheduler->workers()[0]->num_finished_tasks(), 40);
  EXPECT_GT(node_queue_scheduler->workers()[1]->num_finished_tasks(), 40);
}

TEST_F(SchedulerTest, SingleWorkerGuaranteeProgress) {
  Hyrise::get().topology.use_default_topology(1);
  Hyrise::get().set_scheduler(std::make_shared<NodeQueueScheduler>());

  auto task_done = false;
  auto task = std::make_shared<JobTask>([&task_done]() {
    auto subtask = std::make_shared<JobTask>([&task_done]() { task_done = true; });

    subtask->schedule();
    Hyrise::get().scheduler()->wait_for_tasks(std::vector<std::shared_ptr<AbstractTask>>{subtask});
  });

  task->schedule();
  Hyrise::get().scheduler()->wait_for_tasks(std::vector<std::shared_ptr<AbstractTask>>{task});
  EXPECT_TRUE(task_done);

  Hyrise::get().scheduler()->finish();
}

}  // namespace hyrise
