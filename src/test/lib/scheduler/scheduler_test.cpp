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
      auto current_value = uint32_t{0};
      auto successful = counter.compare_exchange_strong(current_value, 1);
      ASSERT_TRUE(successful);
    });
    const auto task2 = std::make_shared<JobTask>([&]() {
      auto current_value = uint32_t{1};
      auto successful = counter.compare_exchange_strong(current_value, 2);
      ASSERT_TRUE(successful);
    });
    const auto task3 = std::make_shared<JobTask>([&]() {
      auto current_value = uint32_t{2};
      auto successful = counter.compare_exchange_strong(current_value, 3);
      ASSERT_TRUE(successful);
    });

    task1->set_as_predecessor_of(task2);
    task2->set_as_predecessor_of(task3);

    task3->schedule();
    task1->schedule();
    task2->schedule();

    Hyrise::get().scheduler()->wait_for_tasks({task1, task2, task3});
  }

  void stress_multiple_dependencies(std::atomic_uint32_t& counter) {
    const auto task1 = std::make_shared<JobTask>([&]() {
      counter += 1;
    });
    const auto task2 = std::make_shared<JobTask>([&]() {
      counter += 2;
    });
    const auto task3 = std::make_shared<JobTask>([&]() {
      auto current_value = uint32_t{3};
      auto successful = counter.compare_exchange_strong(current_value, 4);
      ASSERT_TRUE(successful);
    });

    task1->set_as_predecessor_of(task3);
    task2->set_as_predecessor_of(task3);

    task3->schedule();
    task1->schedule();
    task2->schedule();

    Hyrise::get().scheduler()->wait_for_tasks({task1, task2, task3});
  }

  void stress_diamond_dependencies(std::atomic_uint32_t& counter) {
    const auto task1 = std::make_shared<JobTask>([&]() {
      auto current_value = uint32_t{0};
      auto successful = counter.compare_exchange_strong(current_value, 1);
      ASSERT_TRUE(successful);
    });
    const auto task2 = std::make_shared<JobTask>([&]() {
      counter += 2;
    });
    const auto task3 = std::make_shared<JobTask>([&]() {
      counter += 3;
    });
    const auto task4 = std::make_shared<JobTask>([&]() {
      auto current_value = uint32_t{6};
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

    Hyrise::get().scheduler()->finish();
  }

  void increment_counter_in_subtasks(std::atomic_uint32_t& counter) {
    auto tasks = std::vector<std::shared_ptr<AbstractTask>>{};
    for (auto outer_counter = size_t{0}; outer_counter < 10; ++outer_counter) {
      auto task = std::make_shared<JobTask>([&]() {
        auto jobs = std::vector<std::shared_ptr<AbstractTask>>{};
        for (auto inner_counter = size_t{0}; inner_counter < 3; ++inner_counter) {
          auto job = std::make_shared<JobTask>([&]() {
            ++counter;
          });

          job->schedule();
          jobs.emplace_back(job);
        }

        Hyrise::get().scheduler()->wait_for_tasks(jobs);
      });
      task->schedule();
      tasks.emplace_back(task);
    }
  }

  static void group_tasks(std::shared_ptr<NodeQueueScheduler>& node_queue_scheduler, const std::vector<std::shared_ptr<AbstractTask>>& tasks, const size_t group_count) {
    node_queue_scheduler->_group_tasks(tasks, group_count);
  }
};

/**
 * Schedule some tasks with subtasks, make sure all of them finish
 */
TEST_F(SchedulerTest, BasicTest) {
  Hyrise::get().topology.use_fake_numa_topology(8, 4);
  Hyrise::get().set_scheduler(std::make_shared<NodeQueueScheduler>());

  auto counter = std::atomic_uint32_t{0};

  increment_counter_in_subtasks(counter);

  Hyrise::get().scheduler()->finish();

  ASSERT_EQ(counter, 30);
}

TEST_F(SchedulerTest, BasicTestWithoutScheduler) {
  auto counter = std::atomic_uint32_t{0};
  increment_counter_in_subtasks(counter);
  ASSERT_EQ(counter, 30);
}

TEST_F(SchedulerTest, LinearDependenciesWithScheduler) {
  Hyrise::get().topology.use_fake_numa_topology(8, 4);
  Hyrise::get().set_scheduler(std::make_shared<NodeQueueScheduler>());

  auto counter = std::atomic_uint32_t{0};
  stress_linear_dependencies(counter);
  ASSERT_EQ(counter, 3);
}

TEST_F(SchedulerTest, GroupingSingleWorker) {
  // Tests the grouping described in AbstractScheduler::schedule_and_wait_for_tasks and
  // NodeQueueScheduler::_group_tasks. We check that tasks of each group are executed in order. Note that the execution
  // of groups might happen interleaved as workers use randomness (see worker.cpp).
  Hyrise::get().topology.use_fake_numa_topology(1, 1);
  auto node_queue_scheduler = std::make_shared<NodeQueueScheduler>();
  Hyrise::get().set_scheduler(node_queue_scheduler);

  for (const auto task_count : std::vector<size_t>{17, 50, 51, 52, 53, 54, 55, 56, 97, 111, 2'000}) {
    for (const auto group_count : std::vector<size_t>{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 13, 15}) {
      auto tasks = std::vector<std::shared_ptr<AbstractTask>>{};
      auto start_offset = size_t{0};
      auto expected_task_id = size_t{0};

      for (auto task_id = size_t{0}; task_id < task_count; ++task_id) {
        tasks.emplace_back(std::make_shared<JobTask>([&, task_id] {
          if (expected_task_id >= task_count) {
            ++start_offset;
            expected_task_id = start_offset;
          }

          // EXPECT_EQ(expected_task_id, task_id);

          if (expected_task_id != task_id)
            // std::cerr << "       Comparing expected: " << expected_task_id << " and actual: " << task_id << "\n";
          // else
            std::cerr << "ERROR: Comparing expected: " << expected_task_id << " and actual: " << task_id << "\n";
          expected_task_id += group_count;
        }));
      }

      group_tasks(node_queue_scheduler, tasks, group_count);
      node_queue_scheduler->schedule_tasks(tasks);
      node_queue_scheduler->wait_for_tasks(tasks);
    }
  }
}

TEST_F(SchedulerTest, GroupingMultipleWorkers) {
  auto node_queue_scheduler = std::make_shared<NodeQueueScheduler>();
  Hyrise::get().set_scheduler(node_queue_scheduler);

  const auto worker_count = node_queue_scheduler->workers().size();
  std::cerr << "worker count " << worker_count << "\n";
  if (worker_count < 2) {
    GTEST_SKIP();
  }

  const auto multiplier = 1'000;
  const auto task_count = multiplier * worker_count;

  for (const auto group_count : std::vector<size_t>{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 13, 15}) {
    auto output_counter = std::atomic<size_t>{0};
    auto concurrently_processed_groups = std::atomic<int64_t>{0};

    auto tasks = std::vector<std::shared_ptr<AbstractTask>>{};

    for (auto task_id = size_t{0}; task_id < task_count; ++task_id) {
      tasks.emplace_back(std::make_shared<JobTask>([&] {
        ++output_counter;
        const auto active_groups = ++concurrently_processed_groups;
        ASSERT_LE(active_groups, group_count);
        --concurrently_processed_groups;
      }));
    }

    group_tasks(node_queue_scheduler, tasks, group_count);
    node_queue_scheduler->schedule_tasks(tasks);
    node_queue_scheduler->wait_for_tasks(tasks);

    EXPECT_EQ(output_counter, task_count);
  }
}

TEST_F(SchedulerTest, GroupingMultipleWorkers2) {
  auto node_queue_scheduler = std::make_shared<NodeQueueScheduler>();
  Hyrise::get().set_scheduler(node_queue_scheduler);

  const auto task_count = 5'000;

  auto previous_task_id_per_group = std::vector<size_t>(16, 0);
  auto tasks = std::vector<std::shared_ptr<AbstractTask>>{};

  for (auto task_id = size_t{0}; task_id < task_count; ++task_id) {
    tasks.emplace_back(std::make_shared<JobTask>([&, task_id] {
      const auto group_id = task_id % 16;
      const auto prev_task_id = previous_task_id_per_group[group_id];
      if (prev_task_id > 0) {
        EXPECT_EQ(prev_task_id + 16, task_id);
      }
      previous_task_id_per_group[group_id] = task_id;
    }));
  }

  group_tasks(node_queue_scheduler, tasks, 16);
  node_queue_scheduler->schedule_tasks(tasks);
  node_queue_scheduler->wait_for_tasks(tasks);
}

TEST_F(SchedulerTest, MultipleDependenciesWithScheduler) {
  Hyrise::get().topology.use_fake_numa_topology(8, 4);
  Hyrise::get().set_scheduler(std::make_shared<NodeQueueScheduler>());

  auto counter = std::atomic_uint32_t{0};
  stress_multiple_dependencies(counter);
  ASSERT_EQ(counter, 4);
}

TEST_F(SchedulerTest, DiamondDependenciesWithScheduler) {
  Hyrise::get().topology.use_fake_numa_topology(8, 4);
  Hyrise::get().set_scheduler(std::make_shared<NodeQueueScheduler>());

  auto counter = std::atomic_uint32_t{0};
  stress_diamond_dependencies(counter);
  ASSERT_EQ(counter, 7);
}

TEST_F(SchedulerTest, LinearDependenciesWithoutScheduler) {
  auto counter = std::atomic_uint32_t{0};
  stress_linear_dependencies(counter);
  ASSERT_EQ(counter, 3);
}

TEST_F(SchedulerTest, MultipleDependenciesWithoutScheduler) {
  auto counter = std::atomic_uint32_t{0};
  stress_multiple_dependencies(counter);
  ASSERT_EQ(counter, 4);
}

TEST_F(SchedulerTest, DiamondDependenciesWithoutScheduler) {
  auto counter = std::atomic_uint32_t{0};
  stress_diamond_dependencies(counter);
  ASSERT_EQ(counter, 7);
}

TEST_F(SchedulerTest, SuccessorExpired) {
  if constexpr (!HYRISE_DEBUG) {
    GTEST_SKIP();
  }

  const auto task1 = std::make_shared<JobTask>([&]() {});
  const auto task2 = std::make_shared<JobTask>([&]() {});
  task1->set_as_predecessor_of(task2);

  {
    const auto task3 = std::make_shared<JobTask>([&]() {});
    task2->set_as_predecessor_of(task3);
  }

  task1->schedule();
  // When task2 finishes, it will not be able to obtain its successor task3 as it went out of scope.
  ASSERT_THROW(task2->schedule(), std::bad_weak_ptr);

  Hyrise::get().scheduler()->finish();
}

TEST_F(SchedulerTest, NotAllDependenciesPassedToScheduler) {
  if constexpr (!HYRISE_DEBUG) {
    GTEST_SKIP();
  }

  const auto task1 = std::make_shared<JobTask>([&]() {});
  const auto task2 = std::make_shared<JobTask>([&]() {});
  const auto task3 = std::make_shared<JobTask>([&]() {});

  task1->set_as_predecessor_of(task2);
  task2->set_as_predecessor_of(task3);

  const auto tasks = std::vector<std::shared_ptr<AbstractTask>>{task1, task2};

  // The scheduler should complain that not all dependencies (task3 is a successor of task2) are passed.
  ASSERT_THROW(Hyrise::get().scheduler()->schedule_and_wait_for_tasks(tasks), std::logic_error);
}

TEST_F(SchedulerTest, SameSuccessorMultipleTimes) {
  const auto task1 = std::make_shared<JobTask>([&]() {});
  const auto task2 = std::make_shared<JobTask>([&]() {});

  task1->set_as_predecessor_of(task2);
  task1->set_as_predecessor_of(task2);
  const auto task2_2 = task2;
  task1->set_as_predecessor_of(task2_2);
  ASSERT_EQ(task1->successors().size(), 1);
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
    const auto subtask = std::make_shared<JobTask>([&task_done]() {
      task_done = true;
    });

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
  tasks.emplace_back(std::make_shared<JobTask>([&]() {
    merge_sort(first, middle);
  }));
  tasks.emplace_back(std::make_shared<JobTask>([&]() {
    merge_sort(middle, last);
  }));

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

TEST_F(SchedulerTest, GetThisThreadWorker) {
  const auto node_queue_scheduler = std::make_shared<NodeQueueScheduler>();
  Hyrise::get().topology.use_fake_numa_topology(1, 1);
  Hyrise::get().set_scheduler(node_queue_scheduler);

  // Even though we use the NodeQueueScheduler, calling `get_this_thread_worker()` not from a worker (here, called from
  // the main thread) returns a nullptr.
  EXPECT_EQ(Worker::get_this_thread_worker(), nullptr);

  auto tasks = std::vector<std::shared_ptr<AbstractTask>>{};
  tasks.emplace_back(std::make_shared<JobTask>([&]() {
    EXPECT_NO_THROW(Worker::get_this_thread_worker());
  }));

  Hyrise::get().scheduler()->schedule_and_wait_for_tasks(tasks);
}

TEST_F(SchedulerTest, ExecuteNextFromNonWorker) {
  const auto node_queue_scheduler = std::make_shared<NodeQueueScheduler>();
  Hyrise::get().topology.use_fake_numa_topology(1, 1);
  Hyrise::get().set_scheduler(node_queue_scheduler);

  auto empty_task = std::make_shared<JobTask>([&]() {});
  EXPECT_THROW(node_queue_scheduler->workers()[0]->execute_next(empty_task), std::logic_error);
}

}  // namespace hyrise
