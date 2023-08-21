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
#include "scheduler/shutdown_task.hpp"
#include "scheduler/task_queue.hpp"

namespace hyrise {

using namespace expression_functional;  // NOLINT(build/namespaces)

class SchedulerTest : public BaseTest {
 protected:
  void stress_linear_dependencies(std::atomic_uint32_t& counter) {
    const auto task1 = std::make_shared<JobTask>([&]() {
      auto current_value = 0u;
      auto successful = counter.compare_exchange_strong(current_value, 1u);
      ASSERT_TRUE(successful);
    });
    const auto task2 = std::make_shared<JobTask>([&]() {
      auto current_value = 1u;
      auto successful = counter.compare_exchange_strong(current_value, 2u);
      ASSERT_TRUE(successful);
    });
    const auto task3 = std::make_shared<JobTask>([&]() {
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
    const auto task1 = std::make_shared<JobTask>([&]() { counter += 1u; });
    const auto task2 = std::make_shared<JobTask>([&]() { counter += 2u; });
    const auto task3 = std::make_shared<JobTask>([&]() {
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
    const auto task1 = std::make_shared<JobTask>([&]() {
      auto current_value = 0u;
      auto successful = counter.compare_exchange_strong(current_value, 1u);
      ASSERT_TRUE(successful);
    });
    const auto task2 = std::make_shared<JobTask>([&]() { counter += 2u; });
    const auto task3 = std::make_shared<JobTask>([&]() { counter += 3u; });
    const auto task4 = std::make_shared<JobTask>([&]() {
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
    for (auto outer_counter = size_t{0}; outer_counter < 10; ++outer_counter) {
      auto task = std::make_shared<JobTask>([&]() {
        std::vector<std::shared_ptr<AbstractTask>> jobs;
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

  constexpr auto TASK_COUNT = 60;

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
  for (auto group = size_t{0}; group < num_groups; ++group) {
    for (auto task_id = size_t{0}; task_id < TASK_COUNT / num_groups; ++task_id) {
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

  Hyrise::get().scheduler()->finish();

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

  task_1->set_node_id(NodeID{0});
  task_2->set_node_id(NodeID{0});
  task_3->set_node_id(NodeID{0});
  task_4->set_node_id(NodeID{1});

  task_1->schedule();
  task_2->schedule();
  task_3->schedule();
  task_4->schedule();

  node_queue_scheduler->wait_for_all_tasks();

  EXPECT_EQ(node_queue_scheduler->workers()[0]->num_finished_tasks(), 3);
  EXPECT_EQ(node_queue_scheduler->workers()[1]->num_finished_tasks(), 1);
}

TEST_F(SchedulerTest, CorrectJobMapping) {
  Hyrise::get().topology.use_fake_numa_topology(3, 1);
  Hyrise::get().set_scheduler(std::make_shared<NodeQueueScheduler>());

  auto tasks = std::vector<std::shared_ptr<AbstractTask>>();
  auto executed_on_node = std::vector<NodeID>(4, NodeID{0});

  auto addTask = [&](NodeID node_id, int pos) {
    auto task = std::make_shared<JobTask>(
        [&, pos]() {
          const auto worker = Worker::get_this_thread_worker();
          executed_on_node[pos] = worker->queue()->node_id();
        },
        SchedulePriority::Default, false);
    task->set_node_id(node_id);
    tasks.emplace_back(std::move(task));
  };

  addTask(NodeID{0}, 0);
  addTask(NodeID{0}, 1);
  addTask(NodeID{2}, 2);
  addTask(NodeID{1}, 3);

  Hyrise::get().scheduler()->schedule_and_wait_for_tasks(tasks);

  const auto expected_node_ids = std::vector<NodeID>{NodeID{0}, NodeID{0}, NodeID{2}, NodeID{1}};

  EXPECT_EQ(executed_on_node, expected_node_ids);
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
  if (std::thread::hardware_concurrency() < 2) {
    GTEST_SKIP();
  }

  Hyrise::get().topology.use_fake_numa_topology(2, 1);
  const auto node_queue_scheduler = std::make_shared<NodeQueueScheduler>();
  Hyrise::get().set_scheduler(node_queue_scheduler);

  EXPECT_EQ(node_queue_scheduler->determine_queue_id(NodeID{1}), NodeID{1});

  // For the case of no load on node ID 0 (which is the case here), tasks are always scheduled on this node.
  EXPECT_EQ(node_queue_scheduler->determine_queue_id(CURRENT_NODE_ID), NodeID{0});

  // The distribution of tasks under high load is tested in the concurrency stress tests.
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

TEST_F(SchedulerTest, NodeQueueSchedulerCreationAndReset) {
  if (std::thread::hardware_concurrency() < 4) {
    // If the machine has less than 4 cores, the calls to use_non_numa_topology() below will implicitly reduce the
    // worker count to the number of cores, therefore failing the assertions.
    GTEST_SKIP();
  }

  const auto thread_count = std::thread::hardware_concurrency();

  Hyrise::get().topology.use_fake_numa_topology(thread_count, thread_count / 4);
  for (auto loop_id = size_t{0}; loop_id < 256; ++loop_id) {
    auto node_queue_scheduler = std::make_shared<NodeQueueScheduler>();
    Hyrise::get().set_scheduler(node_queue_scheduler);
    EXPECT_EQ(node_queue_scheduler->active_worker_count().load(), thread_count);
    Hyrise::get().set_scheduler(std::make_shared<ImmediateExecutionScheduler>());
    EXPECT_EQ(node_queue_scheduler->active_worker_count().load(), 0);
  }
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
