#include <chrono>
#include <memory>
#include <thread>
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
#include "scheduler/shutdown_task.hpp"
#include "scheduler/task_queue.hpp"

namespace hyrise {

using namespace expression_functional;  // NOLINT(build/namespaces)

class SchedulerTest : public BaseTest {
 protected:
  void stress_linear_dependencies(std::atomic_uint32_t& counter) {
    const auto task1 = std::make_shared<JobTask>([&]() {
      auto current_value = 0u;
      auto successful = counter.compare_exchange_strong(current_value, 1);
      ASSERT_TRUE(successful);
    });
    const auto task2 = std::make_shared<JobTask>([&]() {
      auto current_value = 1u;
      auto successful = counter.compare_exchange_strong(current_value, 2);
      ASSERT_TRUE(successful);
    });
    const auto task3 = std::make_shared<JobTask>([&]() {
      auto current_value = 2u;
      auto successful = counter.compare_exchange_strong(current_value, 3);
      ASSERT_TRUE(successful);
    });

    task1->set_as_predecessor_of(task2);
    task2->set_as_predecessor_of(task3);

    task3->schedule();
    task1->schedule();
    task2->schedule();
  }

  void stress_multiple_dependencies(std::atomic_uint32_t& counter) {
    const auto task1 = std::make_shared<JobTask>([&]() { counter += 1u; });
    const auto task2 = std::make_shared<JobTask>([&]() { counter += 2u; });
    const auto task3 = std::make_shared<JobTask>([&]() {
      auto current_value = 3u;
      auto successful = counter.compare_exchange_strong(current_value, 4);
      ASSERT_TRUE(successful);
    });

    task1->set_as_predecessor_of(task3);
    task2->set_as_predecessor_of(task3);

    task3->schedule();
    task1->schedule();
    task2->schedule();
  }

  void stress_diamond_dependencies(std::atomic_uint32_t& counter) {
    const auto task1 = std::make_shared<JobTask>([&]() {
      auto current_value = 0u;
      auto successful = counter.compare_exchange_strong(current_value, 1);
      ASSERT_TRUE(successful);
    });
    const auto task2 = std::make_shared<JobTask>([&]() { counter += 2u; });
    const auto task3 = std::make_shared<JobTask>([&]() { counter += 3u; });
    const auto task4 = std::make_shared<JobTask>([&]() {
      auto current_value = 6u;
      auto successful = counter.compare_exchange_strong(current_value, 7);
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
    auto tasks = std::vector<std::shared_ptr<AbstractTask>>{};
    for (auto outer_counter = size_t{0}; outer_counter < 10; ++outer_counter) {
      auto task = std::make_shared<JobTask>([&]() {
        auto jobs = std::vector<std::shared_ptr<AbstractTask>>{};
        for (auto inner_counter = size_t{0}; inner_counter < 3; ++inner_counter) {
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

  ASSERT_EQ(counter, 30);

  Hyrise::get().set_scheduler(std::make_shared<ImmediateExecutionScheduler>());
}

TEST_F(SchedulerTest, BasicTestWithoutScheduler) {
  std::atomic_uint32_t counter{0};
  increment_counter_in_subtasks(counter);
  ASSERT_EQ(counter, 30);
}

TEST_F(SchedulerTest, LinearDependenciesWithScheduler) {
  Hyrise::get().topology.use_fake_numa_topology(8, 4);
  Hyrise::get().set_scheduler(std::make_shared<NodeQueueScheduler>());

  std::atomic_uint32_t counter{0};

  stress_linear_dependencies(counter);

  Hyrise::get().scheduler()->finish();

  ASSERT_EQ(counter, 3);
}

TEST_F(SchedulerTest, Grouping) {
  // Tests the grouping described in AbstractScheduler::schedule_and_wait_for_tasks and
  // NodeQueueScheduler::_group_tasks. Also tests that successor tasks are called immediately after their dependencies
  // finish. Not really a multi-threading test, though.
  Hyrise::get().topology.use_fake_numa_topology(1, 1);
  std::cout << Hyrise::get().topology << std::endl;
  const auto node_queue_scheduler = std::make_shared<NodeQueueScheduler>();
  Hyrise::get().set_scheduler(node_queue_scheduler);

  auto output = std::vector<size_t>{};
  auto tasks = std::vector<std::shared_ptr<AbstractTask>>{};

  constexpr auto TASK_COUNT = 50;

  for (auto task_id = 0; task_id < TASK_COUNT; ++task_id) {
    tasks.emplace_back(std::make_shared<JobTask>([&output, task_id] { output.emplace_back(task_id); }));
  }
  Hyrise::get().scheduler()->schedule_and_wait_for_tasks(tasks);

  const auto num_groups = node_queue_scheduler->determine_group_count(tasks);
  EXPECT_TRUE(num_groups);
  auto expected_output = std::vector<size_t>{};
  for (auto group_id = size_t{0}; group_id < *num_groups; ++group_id) {
    auto task_id = group_id;
    while (task_id < TASK_COUNT) {
      if (task_id % *num_groups == group_id) {
        expected_output.emplace_back(task_id);
      }
      task_id += *num_groups;
    }
  }

  EXPECT_EQ(output, expected_output);
}

TEST_F(SchedulerTest, MultipleDependenciesWithScheduler) {
  Hyrise::get().topology.use_fake_numa_topology(8, 4);
  Hyrise::get().set_scheduler(std::make_shared<NodeQueueScheduler>());

  std::atomic_uint32_t counter{0};

  stress_multiple_dependencies(counter);

  Hyrise::get().scheduler()->finish();

  ASSERT_EQ(counter, 4);
}

TEST_F(SchedulerTest, DiamondDependenciesWithScheduler) {
  Hyrise::get().topology.use_fake_numa_topology(8, 4);
  Hyrise::get().set_scheduler(std::make_shared<NodeQueueScheduler>());

  std::atomic_uint32_t counter{0};

  stress_diamond_dependencies(counter);

  Hyrise::get().scheduler()->finish();

  ASSERT_EQ(counter, 7);
}

TEST_F(SchedulerTest, LinearDependenciesWithoutScheduler) {
  std::atomic_uint32_t counter{0};
  stress_linear_dependencies(counter);
  ASSERT_EQ(counter, 3);
}

TEST_F(SchedulerTest, MultipleDependenciesWithoutScheduler) {
  std::atomic_uint32_t counter{0};
  stress_multiple_dependencies(counter);
  ASSERT_EQ(counter, 4);
}

TEST_F(SchedulerTest, DiamondDependenciesWithoutScheduler) {
  std::atomic_uint32_t counter{0};
  stress_diamond_dependencies(counter);
  ASSERT_EQ(counter, 7);
}

TEST_F(SchedulerTest, MultipleOperators) {
  Hyrise::get().topology.use_fake_numa_topology(8, 4);
  Hyrise::get().set_scheduler(std::make_shared<NodeQueueScheduler>());

  const auto test_table = load_table("resources/test_data/tbl/int_float.tbl", ChunkOffset{2});
  Hyrise::get().storage_manager.add_table("table", test_table);

  const auto gt = std::make_shared<GetTable>("table");
  const auto a = PQPColumnExpression::from_table(*test_table, ColumnID{0});
  const auto ts = std::make_shared<TableScan>(gt, greater_than_equals_(a, 1234));

  const auto gt_task = std::make_shared<OperatorTask>(gt);
  const auto ts_task = std::make_shared<OperatorTask>(ts);
  gt_task->set_as_predecessor_of(ts_task);

  gt_task->schedule();
  ts_task->schedule();

  Hyrise::get().scheduler()->wait_for_all_tasks();

  const auto expected_result = load_table("resources/test_data/tbl/int_float_filtered2.tbl", ChunkOffset{1});
  EXPECT_TABLE_EQ_UNORDERED(ts->get_output(), expected_result);
}

TEST_F(SchedulerTest, VerifyTaskQueueSetup) {
  if (std::thread::hardware_concurrency() < 4) {
    // If the machine has less than 4 cores, the calls to use_non_numa_topology() below will implicitly reduce the
    // worker count to the number of cores, therefore failing the assertions.
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
  const auto node_queue_scheduler = std::make_shared<NodeQueueScheduler>();
  Hyrise::get().set_scheduler(node_queue_scheduler);
  EXPECT_EQ(2, node_queue_scheduler->queues().size());

  const auto task_1 = std::make_shared<JobTask>([&]() {}, SchedulePriority::Default, false);
  const auto task_2 = std::make_shared<JobTask>([&]() {}, SchedulePriority::Default, false);
  const auto task_3 = std::make_shared<JobTask>([&]() {}, SchedulePriority::Default, false);
  const auto task_4 = std::make_shared<JobTask>([&]() {}, SchedulePriority::Default, false);

  task_1->schedule(NodeID{0});
  task_2->schedule(NodeID{0});
  task_3->schedule(NodeID{0});
  task_4->schedule(NodeID{1});

  node_queue_scheduler->wait_for_all_tasks();

  EXPECT_EQ(node_queue_scheduler->workers()[0]->num_finished_tasks(), 3);
  EXPECT_EQ(node_queue_scheduler->workers()[1]->num_finished_tasks(), 1);
}

TEST_F(SchedulerTest, SingleWorkerGuaranteeProgress) {
  Hyrise::get().topology.use_default_topology(1);
  Hyrise::get().set_scheduler(std::make_shared<NodeQueueScheduler>());

  auto task_done = false;
  auto task = std::make_shared<JobTask>([&task_done]() {
    const auto subtask = std::make_shared<JobTask>([&task_done]() { task_done = true; });

    subtask->schedule();
    Hyrise::get().scheduler()->wait_for_tasks(std::vector<std::shared_ptr<AbstractTask>>{subtask});
  });

  task->schedule();
  Hyrise::get().scheduler()->wait_for_tasks(std::vector<std::shared_ptr<AbstractTask>>{task});
  EXPECT_TRUE(task_done);

  Hyrise::get().scheduler()->finish();
}

TEST_F(SchedulerTest, DetermineQueueIDForTask) {
  Hyrise::get().topology.use_fake_numa_topology(2, 1);
  const auto node_queue_scheduler = std::make_shared<NodeQueueScheduler>();
  Hyrise::get().set_scheduler(node_queue_scheduler);

  EXPECT_EQ(node_queue_scheduler->determine_queue_id(NodeID{1}), NodeID{1});

  // For the case of no load on node ID 0 (which is the case here), tasks are always scheduled on this node.
  EXPECT_EQ(node_queue_scheduler->determine_queue_id(CURRENT_NODE_ID), NodeID{0});

  // The distribution of tasks under high load is tested in the concurrency stress tests.
}

TEST_F(SchedulerTest, NumGroupDetermination) {
  // Test early out for very small number of tasks.
  {
    constexpr auto WORKER_COUNT = size_t{2};

    Hyrise::get().topology.use_fake_numa_topology(WORKER_COUNT, WORKER_COUNT);
    const auto node_queue_scheduler = std::make_shared<NodeQueueScheduler>();
    Hyrise::get().set_scheduler(node_queue_scheduler);

    const auto tasks = std::vector<std::shared_ptr<AbstractTask>>{std::make_shared<JobTask>([&]() {})};
    EXPECT_FALSE(node_queue_scheduler->determine_group_count(tasks));
  }

  // Test that minimally sized topology yields valid group counts.
  {
    Hyrise::get().topology.use_fake_numa_topology(1, 1);
    const auto node_queue_scheduler = std::make_shared<NodeQueueScheduler>();
    Hyrise::get().set_scheduler(node_queue_scheduler);

    // Create a large number of tasks to avoid early out.
    const auto task_count = std::thread::hardware_concurrency() * 10;
    auto tasks = std::vector<std::shared_ptr<AbstractTask>>{};
    tasks.reserve(task_count);
    for (auto task_id = TaskID{0}; task_id < task_count; ++task_id) {
      tasks.push_back(std::make_shared<JobTask>([&]() {}));
    }
    EXPECT_LT(1, node_queue_scheduler->determine_group_count(tasks));
  }
}

TEST_F(SchedulerTest, NumGroupDeterminationDifferentLoads) {
  Hyrise::get().topology.use_fake_numa_topology(4, 4);
  const auto node_queue_scheduler = std::make_shared<NodeQueueScheduler>();
  Hyrise::get().set_scheduler(node_queue_scheduler);

  // Create a large number of tasks to avoid early out.
  const auto task_count = std::thread::hardware_concurrency() * 10;
  auto tasks_1 = std::vector<std::shared_ptr<AbstractTask>>{};
  tasks_1.reserve(task_count);
  for (auto task_id = TaskID{0}; task_id < task_count; ++task_id) {
    tasks_1.push_back(std::make_shared<JobTask>([&]() {}));
  }

  const auto num_groups_without_load = node_queue_scheduler->determine_group_count(tasks_1);
  EXPECT_TRUE(num_groups_without_load);

  // Create load on queue.
  volatile auto block_jobs = std::atomic_bool{true};
  auto tasks_2 = std::vector<std::shared_ptr<AbstractTask>>{};
  for (auto task_id = TaskID{0}; task_id < task_count; ++task_id) {
    tasks_2.push_back(std::make_shared<JobTask>([&]() {
      while (block_jobs) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
      }
    }));
    tasks_2.back()->schedule();
  }

  const auto num_groups_with_load = node_queue_scheduler->determine_group_count(tasks_2);
  EXPECT_TRUE(num_groups_with_load);

  // We should receive a larger group count when the queue load is low.
  EXPECT_GT(*num_groups_without_load, *num_groups_with_load);

  // Shutdown. Finish scheduled jobs.
  block_jobs = false;
  node_queue_scheduler->wait_for_tasks(tasks_2);
}

template <typename Iterator>
void merge_sort(Iterator first, Iterator last) {
  if (std::distance(first, last) == 1) {
    return;
  }

  auto middle = first + (std::distance(first, last) / 2);
  auto tasks = std::vector<std::shared_ptr<AbstractTask>>{};
  tasks.emplace_back(std::make_shared<JobTask>([&]() { merge_sort(first, middle); }));
  tasks.emplace_back(std::make_shared<JobTask>([&]() { merge_sort(middle, last); }));

  Hyrise::get().scheduler()->schedule_and_wait_for_tasks(tasks);

  std::inplace_merge(first, middle, last);
}

// Recursive merge sort. Creates a typical divide-and-conquer fan out pattern of tasks. We use the text book
// implementation that recurses until the vector length is 1 to increase the depth of the fan out.
TEST_F(SchedulerTest, MergeSort) {
  // Sizes up to 20'000 works for MacOS (debug mode, more for release) with its comparatively small stack size. If this
  // test fails on a new platform, check the system's stack size and if ITEM_COUNT needs to be reduced.
  constexpr auto ITEM_COUNT = size_t{5'000};
  Assert(ITEM_COUNT % 5 == 0, "Must be dividable by 5.");

  Hyrise::get().set_scheduler(std::make_shared<NodeQueueScheduler>());

  auto vector_to_sort = std::vector<int64_t>{};
  vector_to_sort.reserve(ITEM_COUNT);
  for (auto i = size_t{0}; i < ITEM_COUNT / 5; ++i) {
    for (auto j = size_t{0}; j < 5; ++j) {
      vector_to_sort.push_back(i * 5 + (4 - j));
    }
  }

  merge_sort(vector_to_sort.begin(), vector_to_sort.end());
  EXPECT_TRUE(std::is_sorted(vector_to_sort.begin(), vector_to_sort.end()));
}

TEST_F(SchedulerTest, ShutdownTaskDecrement) {
  auto counter_1 = std::atomic_int64_t{1};
  auto shutdown_task_1 = ShutdownTask{counter_1};
  // Prepare job for execution (usually done when scheduled and obtained by workers)
  EXPECT_TRUE(shutdown_task_1.try_mark_as_enqueued());
  EXPECT_TRUE(shutdown_task_1.try_mark_as_assigned_to_worker());
  EXPECT_EQ(counter_1.load(), 1);
  shutdown_task_1.execute();
  EXPECT_EQ(counter_1.load(), 0);

  auto counter_2 = std::atomic_int64_t{0};
  auto shutdown_task_2 = ShutdownTask{counter_2};
  EXPECT_TRUE(shutdown_task_2.try_mark_as_enqueued());
  EXPECT_TRUE(shutdown_task_2.try_mark_as_assigned_to_worker());
  EXPECT_EQ(counter_2.load(), 0);
  EXPECT_THROW(shutdown_task_2.execute(), std::logic_error);
}

}  // namespace hyrise
