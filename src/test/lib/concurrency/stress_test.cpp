#include <atomic>
#include <chrono>
#include <cmath>
#include <future>
#include <mutex>
#include <numeric>
#include <optional>
#include <shared_mutex>
#include <thread>

#include "base_test.hpp"
#include "benchmark_config.hpp"
#include "hyrise.hpp"
#include "lib/utils/plugin_test_utils.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "operators/insert.hpp"
#include "operators/table_wrapper.hpp"
#include "operators/union_all.hpp"
#include "scheduler/immediate_execution_scheduler.hpp"
#include "scheduler/node_queue_scheduler.hpp"
#include "scheduler/task_queue.hpp"
#include "scheduler/worker.hpp"
#include "sql/sql_pipeline_builder.hpp"
#include "storage/constraints/constraint_utils.hpp"
#include "storage/table.hpp"
#include "storage/table_column_definition.hpp"
#include "tpch/tpch_constants.hpp"
#include "tpch/tpch_table_generator.hpp"
#include "ucc_discovery_plugin.hpp"
#include "utils/atomic_max.hpp"
#include "utils/plugin_manager.hpp"

namespace hyrise {

class StressTest : public BaseTest {
 protected:
  void SetUp() override {
    // Set scheduler so that we can execute multiple SQL statements on separate threads.
    Hyrise::get().set_scheduler(std::make_shared<NodeQueueScheduler>());
  }

  void clear_soft_key_constraints(const std::shared_ptr<Table>& table) {
    // We need to clear the soft key constraints before each test, otherwise they will be added multiple times.
    table->_table_key_constraints.clear();
  }

  static constexpr auto DEFAULT_LOAD_FACTOR = uint32_t{10};

  const uint32_t CPU_COUNT = std::min(std::thread::hardware_concurrency(), uint32_t{32});
  const std::vector<std::vector<uint32_t>> FAKE_SINGLE_NODE_NUMA_TOPOLOGIES = {
      {CPU_COUNT}, {CPU_COUNT, 0, 0}, {0, CPU_COUNT, 0}, {0, 0, CPU_COUNT}};

  const std::vector<std::vector<uint32_t>> FAKE_MULTI_NODE_NUMA_TOPOLOGIES = {
      {CPU_COUNT, CPU_COUNT, 0, 0}, {0, CPU_COUNT, CPU_COUNT, 0}, {0, 0, CPU_COUNT, CPU_COUNT}};
};

TEST_F(StressTest, TestTransactionConflicts) {
  // Update a table with two entries and a chunk size of two. This will lead to a high number of transaction conflicts
  // and many chunks being created.
  const auto table_a = load_table("resources/test_data/tbl/int_float.tbl", ChunkOffset{2});
  Hyrise::get().storage_manager.add_table("table_a", table_a);
  auto initial_sum = int64_t{0};

  {
    auto pipeline = SQLPipelineBuilder{std::string{"SELECT SUM(a) FROM table_a"}}.create_pipeline();
    const auto [_, verification_table] = pipeline.get_result_table();
    initial_sum = *verification_table->get_value<int64_t>(ColumnID{0}, 0);
  }

  auto successful_increments = std::atomic_uint32_t{0};
  auto conflicted_increments = std::atomic_uint32_t{0};
  const auto iterations_per_thread = uint32_t{20};

  // Define the work package.
  const auto run = [&]() {
    auto my_successful_increments = uint32_t{0};
    auto my_conflicted_increments = uint32_t{0};
    for (auto iteration = uint32_t{0}; iteration < iterations_per_thread; ++iteration) {
      const std::string sql = "UPDATE table_a SET a = a + 1 WHERE a = (SELECT MIN(a) FROM table_a);";
      auto pipeline = SQLPipelineBuilder{sql}.create_pipeline();
      const auto [status, _] = pipeline.get_result_table();
      if (status == SQLPipelineStatus::Success) {
        ++my_successful_increments;
      } else {
        ++my_conflicted_increments;
      }
    }
    successful_increments += my_successful_increments;
    conflicted_increments += my_conflicted_increments;
  };

  const auto num_threads = uint32_t{100};
  auto threads = std::vector<std::thread>{};
  threads.reserve(num_threads);

  for (auto thread_num = uint32_t{0}; thread_num < num_threads; ++thread_num) {
    // We want a future to the thread running, so we can kill it after a future.wait(timeout) or the test would freeze.
    threads.emplace_back(run);
  }

  for (auto& thread : threads) {
    thread.join();
  }

  // Verify results.
  auto final_sum = int64_t{0};
  {
    auto pipeline = SQLPipelineBuilder{std::string{"SELECT SUM(a) FROM table_a"}}.create_pipeline();
    const auto [_, verification_table] = pipeline.get_result_table();
    final_sum = *verification_table->get_value<int64_t>(ColumnID{0}, 0);
  }

  // Really pessimistic, but at least 2 statements should have made it.
  EXPECT_GT(successful_increments, 2);

  EXPECT_EQ(successful_increments + conflicted_increments, num_threads * iterations_per_thread);
  EXPECT_EQ(final_sum - initial_sum, successful_increments);
}

TEST_F(StressTest, TestTransactionInsertsSmallChunks) {
  // An update-heavy load on a table with a ridiculously low target chunk size, creating many new chunks. This is
  // different from TestTransactionConflicts, in that each thread has its own logical row and no transaction conflicts
  // occur. In the other test, a failed "mark for deletion" (i.e., swap of the row's tid) would lead to no row begin
  // appended.
  auto column_definitions = TableColumnDefinitions{};
  column_definitions.emplace_back("a", DataType::Int, false);
  column_definitions.emplace_back("b", DataType::Int, false);
  const auto table = std::make_shared<Table>(column_definitions, TableType::Data, ChunkOffset{3}, UseMvcc::Yes);
  Hyrise::get().storage_manager.add_table("table_b", table);

  const auto iterations_per_thread = uint32_t{20};

  // Define the work package - the job id is used so that each thread has its own logical row to work on.
  auto job_id = std::atomic_uint32_t{0};
  const auto run = [&]() {
    const auto my_job_id = job_id++;
    for (auto iteration = uint32_t{0}; iteration < iterations_per_thread; ++iteration) {
      auto pipeline =
          SQLPipelineBuilder{
              iteration == 0 ? std::string{"INSERT INTO table_b (a, b) VALUES ("} + std::to_string(my_job_id) + ", 1)"
                             : std::string{"UPDATE table_b SET b = b + 1 WHERE a = "} + std::to_string(my_job_id)}
              .create_pipeline();
      const auto [status, _] = pipeline.get_result_table();
      EXPECT_EQ(status, SQLPipelineStatus::Success);
    }
  };

  const auto num_threads = uint32_t{100};
  auto threads = std::vector<std::thread>{};
  threads.reserve(num_threads);

  for (auto thread_num = uint32_t{0}; thread_num < num_threads; ++thread_num) {
    threads.emplace_back(run);
  }

  for (auto& thread : threads) {
    thread.join();
  }

  // Verify that the values in column b are correctly incremented.
  {
    auto pipeline = SQLPipelineBuilder{std::string{"SELECT MIN(b) FROM table_b"}}.create_pipeline();
    const auto [_, verification_table] = pipeline.get_result_table();
    EXPECT_EQ(*verification_table->get_value<int32_t>(ColumnID{0}, 0), iterations_per_thread);
  }
}

TEST_F(StressTest, TestTransactionInsertsPackedNullValues) {
  // As ValueSegments store their null flags in a vector<bool>, which is not safe to be modified concurrently, conflicts
  // may (and have) occurred when that vector was written without any type of protection.

  auto column_definitions = TableColumnDefinitions{};
  column_definitions.emplace_back("a", DataType::Int, false);
  column_definitions.emplace_back("b", DataType::Int, true);
  const auto table = std::make_shared<Table>(column_definitions, TableType::Data, Chunk::DEFAULT_SIZE, UseMvcc::Yes);
  Hyrise::get().storage_manager.add_table("table_c", table);

  const auto iterations_per_thread = uint32_t{200};

  // Define the work package - each job writes a=job_id, b=(NULL or 1, depending on job_id).
  auto job_id = std::atomic_uint32_t{0};
  const auto run = [&]() {
    const auto my_job_id = job_id++;
    for (auto iteration = uint32_t{0}; iteration < iterations_per_thread; ++iteration) {
      // b is set to NULL by half of the jobs.
      auto pipeline = SQLPipelineBuilder{std::string{"INSERT INTO table_c (a, b) VALUES ("} +
                                         std::to_string(my_job_id) + ", " + (my_job_id % 2 ? "NULL" : "1") + ")"}
                          .create_pipeline();
      const auto [status, _] = pipeline.get_result_table();
      EXPECT_EQ(status, SQLPipelineStatus::Success);
    }
  };

  const auto num_threads = uint32_t{20};
  auto threads = std::vector<std::thread>{};
  threads.reserve(num_threads);

  for (auto thread_num = uint32_t{0}; thread_num < num_threads; ++thread_num) {
    threads.emplace_back(run);
  }

  // Wait for completion or timeout (should not occur).
  for (auto& thread : threads) {
    thread.join();
  }

  // Check that NULL values in column b are correctly set.
  auto pipeline =
      SQLPipelineBuilder{"SELECT a, COUNT(a), COUNT(b) FROM table_c GROUP BY a ORDER BY a"}.create_pipeline();
  const auto [_, verification_table] = pipeline.get_result_table();
  ASSERT_EQ(verification_table->row_count(), num_threads);

  for (auto row = size_t{0}; row < num_threads; ++row) {
    EXPECT_EQ(*verification_table->get_value<int32_t>(ColumnID{0}, row), row);
    EXPECT_EQ(*verification_table->get_value<int64_t>(ColumnID{1}, row), iterations_per_thread);
    EXPECT_EQ(*verification_table->get_value<int64_t>(ColumnID{2}, row), row % 2 ? 0 : iterations_per_thread);
  }
}

TEST_F(StressTest, NodeQueueSchedulerStressTest) {
  if (std::thread::hardware_concurrency() < 2) {
    GTEST_SKIP();
  }

  // Create a large number of nodes in a fake topology (many workers will share the same thread).
  const auto node_count = std::thread::hardware_concurrency() *
                          (HYRISE_WITH_ADDR_UB_LEAK_SAN || HYRISE_WITH_TSAN ? 1 : DEFAULT_LOAD_FACTOR);

  Hyrise::get().topology.use_fake_numa_topology(node_count, 1);
  const auto node_queue_scheduler = std::make_shared<NodeQueueScheduler>();
  Hyrise::get().set_scheduler(node_queue_scheduler);

  // Just a sufficiently large number to trigger a non-empty queue.
  const auto job_counts = std::vector<size_t>{node_count << 3, node_count << 4, node_count << 3};

  auto num_finished_jobs = std::atomic_uint32_t{0};
  volatile auto start_jobs = std::atomic_bool{false};

  auto job_lists = std::vector<std::vector<std::shared_ptr<AbstractTask>>>{};
  for (const auto job_count : job_counts) {
    job_lists.push_back(std::vector<std::shared_ptr<AbstractTask>>{});
    auto& jobs = job_lists.back();
    jobs.reserve(job_count);

    for (auto task_count = size_t{0}; task_count < job_count; ++task_count) {
      jobs.push_back(std::make_shared<JobTask>([&]() {
        while (!start_jobs) {}
        ++num_finished_jobs;
      }));
      jobs.back()->schedule();
    }
  }

  // In the default case, tasks are added to node 0 when its load is low. In this test, tasks cannot be processed until
  // `start_jobs` is set, leading to a high queue load that cannot be processed. New tasks that are scheduled should
  // thus be assigned to different TaskQueues to distribute the load.
  auto second_worker = std::next(node_queue_scheduler->workers().cbegin());
  EXPECT_TRUE(std::any_of(second_worker, node_queue_scheduler->workers().cend(), [](const auto& worker) {
    return worker->queue()->estimate_load() > 0;
  }));

  // Set flag to allow tasks to continue.
  start_jobs = true;
  for (const auto& jobs : job_lists) {
    node_queue_scheduler->wait_for_tasks(jobs);
  }

  // Three batches of jobs have been concurrently scheduled. Check that the incremented `num_finished_jobs` has the
  // expected value.
  const auto job_count_sum = std::accumulate(job_counts.cbegin(), job_counts.cend(), size_t{0});
  EXPECT_EQ(num_finished_jobs, job_count_sum);
}

TEST_F(StressTest, NodeQueueSchedulerCreationAndReset) {
  if (std::thread::hardware_concurrency() < 4) {
    // If the machine has less than 4 cores, the calls to use_non_numa_topology() below will implicitly reduce the
    // worker count to the number of cores, therefore failing the assertions.
    GTEST_SKIP();
  }

  const auto thread_count = std::thread::hardware_concurrency();

  Hyrise::get().topology.use_fake_numa_topology(thread_count, thread_count / 4);
  for (auto loop_id = size_t{0}; loop_id < 64; ++loop_id) {
    auto node_queue_scheduler = std::make_shared<NodeQueueScheduler>();
    Hyrise::get().set_scheduler(node_queue_scheduler);
    EXPECT_EQ(node_queue_scheduler->active_worker_count().load(), thread_count);
    Hyrise::get().set_scheduler(std::make_shared<ImmediateExecutionScheduler>());
    EXPECT_EQ(node_queue_scheduler->active_worker_count().load(), 0);
  }
}

// Check that spawned jobs increment the semaphore correctly.
// First, create jobs but not schedule them to check if semaphore is zero. Second, we spwan blocked jobs and check the
// semaphore to have the correct value. Third, we unblock all jobs and check that the semaphore is zero again.
//
// We run this test for various fake NUMA topologies as it triggered a bug that was introduced with #2610.
TEST_F(StressTest, NodeQueueSchedulerSemaphoreIncrements) {
  constexpr auto SLEEP_TIME = std::chrono::milliseconds{1};
  const auto job_count = CPU_COUNT * 4 * (HYRISE_WITH_TSAN ? 1 : DEFAULT_LOAD_FACTOR);

  for (const auto& fake_numa_topology : FAKE_SINGLE_NODE_NUMA_TOPOLOGIES) {
    Hyrise::get().topology.use_fake_numa_topology(fake_numa_topology);

    auto node_queue_scheduler = std::make_shared<NodeQueueScheduler>();
    Hyrise::get().set_scheduler(node_queue_scheduler);

    auto counter = std::atomic<uint32_t>{0};
    auto active_task_count = std::atomic<uint32_t>{0};
    auto wait_flag = std::atomic_flag{};

    auto waiting_jobs = std::vector<std::shared_ptr<AbstractTask>>{};
    waiting_jobs.reserve(job_count);
    for (auto job_id = size_t{0}; job_id < job_count; ++job_id) {
      waiting_jobs.emplace_back(std::make_shared<JobTask>([&] {
        ++active_task_count;
        wait_flag.wait(false);
        ++counter;
      }));
    }

    for (const auto& queue : node_queue_scheduler->queues()) {
      if (!queue) {
        continue;
      }
      EXPECT_EQ(queue->semaphore.availableApprox(), 0);
    }
    EXPECT_EQ(counter, 0);

    for (const auto& waiting_job : waiting_jobs) {
      waiting_job->schedule();
    }

    // Wait a bit for workers to pull jobs and decrement semaphore.
    while (active_task_count < CPU_COUNT) {
      std::this_thread::sleep_for(SLEEP_TIME);
    }

    for (const auto& queue : node_queue_scheduler->queues()) {
      if (!queue) {
        continue;
      }
      EXPECT_EQ(queue->semaphore.availableApprox(), job_count - CPU_COUNT);
    }

    const auto previous_value = wait_flag.test_and_set();
    EXPECT_EQ(previous_value, false);
    wait_flag.notify_all();
    Hyrise::get().scheduler()->wait_for_tasks(waiting_jobs);

    for (const auto& queue : node_queue_scheduler->queues()) {
      if (!queue) {
        continue;
      }
      EXPECT_EQ(queue->semaphore.availableApprox(), 0);
    }
    EXPECT_EQ(job_count, counter);
  }
}

// Similar to test above, but here we make tasks dependent of each other which means only non-dependent tasks will be
// scheduled.
TEST_F(StressTest, NodeQueueSchedulerSemaphoreIncrementsDependentTasks) {
  constexpr auto DEPENDENT_JOB_TASKS_LENGTH = uint32_t{10};
  constexpr auto SLEEP_TIME = std::chrono::milliseconds{1};

  // Ensure there is at least one job left after each worker pulled one.
  const auto min_job_count = DEPENDENT_JOB_TASKS_LENGTH * CPU_COUNT + 1;
  const auto job_count = std::max(min_job_count, CPU_COUNT * 4 * (HYRISE_WITH_TSAN ? 1 : DEFAULT_LOAD_FACTOR));

  for (const auto& fake_numa_topology : FAKE_SINGLE_NODE_NUMA_TOPOLOGIES) {
    Hyrise::get().topology.use_fake_numa_topology(fake_numa_topology);
    auto node_queue_scheduler = std::make_shared<NodeQueueScheduler>();
    Hyrise::get().set_scheduler(node_queue_scheduler);

    auto counter = std::atomic<uint32_t>{0};
    auto active_task_count = std::atomic<uint32_t>{0};
    auto wait_flag = std::atomic<bool>{true};

    auto waiting_jobs = std::vector<std::shared_ptr<AbstractTask>>{};
    waiting_jobs.reserve(job_count);
    for (auto job_id = uint32_t{0}; job_id < job_count; ++job_id) {
      waiting_jobs.emplace_back(std::make_shared<JobTask>([&] {
        ++active_task_count;
        while (wait_flag) {
          std::this_thread::sleep_for(SLEEP_TIME);
        }
        ++counter;
      }));
      // We create runs of dependent jobs and set the current job as a predecessor of the previous job.
      if (job_id % DEPENDENT_JOB_TASKS_LENGTH != 0) {
        waiting_jobs.back()->set_as_predecessor_of(waiting_jobs[job_id - 1]);
      }
    }

    for (const auto& queue : node_queue_scheduler->queues()) {
      if (!queue) {
        continue;
      }
      EXPECT_EQ(queue->semaphore.availableApprox(), 0);
    }

    for (const auto& waiting_job : waiting_jobs) {
      waiting_job->schedule();
    }

    // Wait a bit for workers to pull jobs and decrement semaphore.
    while (active_task_count < CPU_COUNT) {
      std::this_thread::sleep_for(SLEEP_TIME);
    }

    // The number of scheduled jobs depends on DEPENDENT_JOB_TASKS_LENGTH (see job definition above; due to the jobs
    // dependencies, jobs are only scheduled when they have no predecessors).
    const auto executable_jobs = static_cast<float>(job_count) / static_cast<float>(DEPENDENT_JOB_TASKS_LENGTH);
    for (const auto& queue : node_queue_scheduler->queues()) {
      if (!queue) {
        continue;
      }

      // We started scheduled jobs, which block all workers due to `wait_flag` being true. Thus, the semaphore should be
      // reduced by the number of workers (i.e., CPU_COUNT).
      EXPECT_EQ(queue->semaphore.availableApprox(), static_cast<size_t>(std::ceil(executable_jobs)) - CPU_COUNT);
    }

    wait_flag = false;
    Hyrise::get().scheduler()->wait_for_tasks(waiting_jobs);

    for (const auto& queue : node_queue_scheduler->queues()) {
      if (!queue) {
        continue;
      }
      EXPECT_EQ(queue->semaphore.availableApprox(), 0);
    }
    EXPECT_EQ(job_count, counter);
  }
}

// The issue with active NUMA nodes (see #2548) did not occur in our non-NUMA-bound CI setup but only when executing
// NUMA-bound benchmarks on a server. To catch such issues, this test executes a comparatively complex TPC-H query for
// different fake NUMA topologies.
TEST_F(StressTest, NodeQueueSchedulerMultiNumaNodeTPCHQ13) {
  const auto benchmark_config = std::make_shared<BenchmarkConfig>();

  TPCHTableGenerator(0.1f, ClusteringConfiguration::None, benchmark_config).generate_and_store();

  auto topologies = FAKE_SINGLE_NODE_NUMA_TOPOLOGIES;
  topologies.insert(topologies.end(), FAKE_SINGLE_NODE_NUMA_TOPOLOGIES.begin(), FAKE_SINGLE_NODE_NUMA_TOPOLOGIES.end());

  const auto tpch_q13 = std::string{
      "SELECT c_count, count(*) as custdist FROM (SELECT c_custkey, count(o_orderkey) "
      "as c_count FROM customer left outer join orders on c_custkey = o_custkey AND "
      "o_comment not like '%special%request%' GROUP BY c_custkey) as c_orders "
      "GROUP BY c_count ORDER BY custdist DESC, c_count DESC;"};

  for (const auto& fake_numa_topology : FAKE_SINGLE_NODE_NUMA_TOPOLOGIES) {
    Hyrise::get().topology.use_fake_numa_topology(fake_numa_topology);
    auto node_queue_scheduler = std::make_shared<NodeQueueScheduler>();
    Hyrise::get().set_scheduler(node_queue_scheduler);

    auto sql_pipeline = SQLPipelineBuilder{tpch_q13}.create_pipeline();
    const auto& [pipeline_status, _] = sql_pipeline.get_result_tables();
    EXPECT_EQ(pipeline_status, SQLPipelineStatus::Success);
  }
}

TEST_F(StressTest, NodeQueueSchedulerTaskGrouping) {
  const auto node_queue_scheduler = std::make_shared<NodeQueueScheduler>();
  Hyrise::get().set_scheduler(node_queue_scheduler);

  const auto worker_count = node_queue_scheduler->workers().size();
  // if (worker_count < NodeQueueScheduler::NUM_GROUPS) {
  if (worker_count < 10) {
    // We would not see any impact of task grouping with too few workers.
    GTEST_SKIP();
  }

  auto NodeQueueScheduler_NUM_GROUPS = size_t{10};

  const auto multiplier = 1'000;
  const auto task_count = multiplier * worker_count;

  for (auto run = uint8_t{0}; run < 10; ++run) {
    auto previous_task_id_per_group = std::vector<size_t>(NodeQueueScheduler_NUM_GROUPS, 0);
    auto output_counter = std::atomic<size_t>{0};
    auto concurrently_processed_tasks = std::atomic<int64_t>{0};

    auto tasks = std::vector<std::shared_ptr<AbstractTask>>{};

    for (auto task_id = size_t{0}; task_id < task_count; ++task_id) {
      tasks.emplace_back(std::make_shared<JobTask>([&, task_id] {
        ++output_counter;
        const auto active_groups = ++concurrently_processed_tasks;
        ASSERT_LE(active_groups, NodeQueueScheduler_NUM_GROUPS);

        const auto group_id = task_id % NodeQueueScheduler_NUM_GROUPS;
        const auto prev_task_id = previous_task_id_per_group[group_id];
        if (prev_task_id > 0) {
          // Once the first task of each group has been executed, we check the previous TaskID of the group to verify
          // that grouping execution happens in the expected round-robin order (see
          // `void NodeQueueScheduler::_group_tasks()`).
          EXPECT_EQ(prev_task_id + NodeQueueScheduler_NUM_GROUPS, task_id);
        }
        previous_task_id_per_group[group_id] = task_id;

        --concurrently_processed_tasks;
      }));
    }

    Hyrise::get().scheduler()->schedule_and_wait_for_tasks(tasks);
    EXPECT_EQ(output_counter, task_count);
  }

  Hyrise::get().scheduler()->finish();
}

TEST_F(StressTest, AtomicMaxConcurrentUpdate) {
  auto counter = std::atomic_uint32_t{0};
  const auto thread_count = 100;
  const auto repetitions = 1'000;

  auto threads = std::vector<std::thread>{};
  threads.reserve(thread_count);

  for (auto thread_id = uint32_t{1}; thread_id <= thread_count; ++thread_id) {
    threads.emplace_back([thread_id, &counter]() {
      for (auto i = uint32_t{1}; i <= repetitions; ++i) {
        set_atomic_max(counter, thread_id + i);
      }
    });
  }

  for (auto& thread : threads) {
    thread.join();
  }

  // Highest thread ID is 100, 1'000 repetitions. 100 + 1'000 = 1'100.
  EXPECT_EQ(counter.load(), 1'100);
}

// Insert operators automatically mark chunks as immutable when they are full and the operator (i) appends a new chunk
// and all other Inserts finished or (ii) they finish (commit/roll back) and are the last pending Insert operator for
// this chunk. To test all of these cases in a stress test, we let threads concurrently insert and commit/roll back.
TEST_F(StressTest, ConcurrentInsertsSetChunksImmutable) {
  const auto table = std::make_shared<Table>(TableColumnDefinitions{{"a", DataType::Int, false}}, TableType::Data,
                                             ChunkOffset{3}, UseMvcc::Yes);
  Hyrise::get().storage_manager.add_table("table_a", table);

  const auto values_to_insert =
      std::make_shared<Table>(TableColumnDefinitions{{"a", DataType::Int, false}}, TableType::Data);
  values_to_insert->append({int32_t{1}});
  values_to_insert->append({int32_t{1}});

  // We observed long runtimes in Debug builds, especially with UBSan enabled. Thus, we reduce the load a bit in this
  // case.
  const auto insert_count = 19 * (HYRISE_DEBUG && HYRISE_WITH_ADDR_UB_LEAK_SAN ? 1 : DEFAULT_LOAD_FACTOR) + 1;
  const auto thread_count = uint32_t{100};
  auto threads = std::vector<std::thread>{};
  threads.reserve(thread_count);

  for (auto thread_id = uint32_t{0}; thread_id < thread_count; ++thread_id) {
    threads.emplace_back([&]() {
      for (auto iteration = uint32_t{0}; iteration < insert_count; ++iteration) {
        const auto table_wrapper = std::make_shared<TableWrapper>(values_to_insert);
        const auto insert = std::make_shared<Insert>("table_a", table_wrapper);

        // Commit only 50% of transactions. Thus, there should be committed and rolled back operators that both mark
        // chunks as immutable.
        const auto do_commit = iteration % 2 == 0;
        const auto transaction_context = Hyrise::get().transaction_manager.new_transaction_context(AutoCommit::No);
        insert->set_transaction_context(transaction_context);
        table_wrapper->execute();
        insert->execute();
        EXPECT_FALSE(insert->execute_failed());
        if (do_commit) {
          transaction_context->commit();
        } else {
          transaction_context->rollback(RollbackReason::User);
        }
      }
    });
  }

  for (auto& thread : threads) {
    thread.join();
  }

  // Each iteration of a thread inserts two rows, which are stored in chunks with a target size of 3.
  const auto inserted_rows = insert_count * thread_count * 2;
  const auto expected_chunks = static_cast<ChunkID::base_type>(std::ceil(static_cast<double>(inserted_rows) / 3.0));
  EXPECT_EQ(table->row_count(), inserted_rows);
  EXPECT_EQ(table->chunk_count(), expected_chunks);

  // Only the final chunk is not full and not immutable.
  const auto immutable_chunk_count = table->chunk_count() - 1;
  for (auto chunk_id = ChunkID{0}; chunk_id < immutable_chunk_count; ++chunk_id) {
    const auto& chunk = table->get_chunk(chunk_id);
    ASSERT_TRUE(chunk);
    EXPECT_EQ(chunk->size(), 3);
    EXPECT_FALSE(chunk->is_mutable());
  }

  EXPECT_EQ(table->last_chunk()->size(), 1);
  EXPECT_TRUE(table->last_chunk()->is_mutable());
}

// Consuming operators register at their inputs and deregister when they are executed. Thus, operators can clear
// intermediate results. Consumer deregistration must work properly in concurrent scenarios.
TEST_F(StressTest, OperatorRegistration) {
  const auto repetition_count = uint32_t{100};
  const auto consumer_count = uint32_t{50};
  const auto sleep_time = std::chrono::milliseconds{5};

  const auto dummy_table = Table::create_dummy_table({{"a", DataType::Int, false}});

  for (auto repetition = uint32_t{0}; repetition < repetition_count; ++repetition) {
    const auto table_wrapper = std::make_shared<TableWrapper>(dummy_table);
    table_wrapper->execute();
    auto threads = std::vector<std::thread>{};
    threads.reserve(consumer_count);
    auto waiting_consumer_count = std::atomic_uint32_t{0};
    auto start_execution = std::atomic_flag{};

    // Generate consumers that try to deregister concurrently once they are executed.
    for (auto consumer_id = uint32_t{0}; consumer_id < consumer_count; ++consumer_id) {
      threads.emplace_back([&]() {
        const auto union_all = std::make_shared<UnionAll>(table_wrapper, table_wrapper);

        // Mark that the consumer is set up.
        ++waiting_consumer_count;

        // Wait for the signal to execute the operator.
        start_execution.wait(false);

        union_all->execute();
      });
    }

    // Wait until the consumers are constructed.
    while (waiting_consumer_count < consumer_count) {
      std::this_thread::sleep_for(sleep_time);
    }

    // The UnionAll operators have the input on both sides.
    EXPECT_EQ(table_wrapper->consumer_count(), consumer_count * 2);

    start_execution.test_and_set();
    start_execution.notify_all();
    for (auto& thread : threads) {
      thread.join();
    }

    EXPECT_EQ(table_wrapper->consumer_count(), 0);
    EXPECT_EQ(table_wrapper->state(), OperatorState::ExecutedAndCleared);

    // One additional deregistration (without prior registration) is not allowed.
    EXPECT_THROW(table_wrapper->deregister_consumer(), std::logic_error);
  }
}

/**
 * Test to verify that rolling back Insert operations does not lead to multiple (outdated) rows being visible. This
 * issue has occurred when using link-time optimization or when inlining MVCC functions (see #2649).
 * We execute and immediately roll back insert operations in multiple threads. In parallel, threads are checking that no
 * new rows are visible.
 */
TEST_F(StressTest, VisibilityOfInsertsBeingRolledBack) {
  // StressTestMultipleRuns runs 10x. Limit max runtime for *SAN builds.
  constexpr auto RUNTIME = std::chrono::seconds(5);
  constexpr auto MAX_VALUE_AND_ROW_COUNT = uint32_t{17};
  constexpr auto MAX_LOOP_COUNT = uint32_t{10'000};  // Experimentally determined, see #2651.

  const auto table_name = std::string{"table_a"};

  // The issues triggered in this test usually arise early (later, the scan is getting slower and slower on increasing
  // table sizes). For that, we execute multiple short runs.
  for (auto test_run = size_t{0}; test_run < 10; ++test_run) {
    if (Hyrise::get().storage_manager.has_table(table_name)) {
      Hyrise::get().storage_manager.drop_table(table_name);
    }

    Hyrise::get().storage_manager.add_table(
        table_name, std::make_shared<Table>(TableColumnDefinitions{{"a", DataType::Int, false}}, TableType::Data,
                                            Chunk::DEFAULT_SIZE, UseMvcc::Yes));

    const auto values_to_insert =
        std::make_shared<Table>(TableColumnDefinitions{{"a", DataType::Int, false}}, TableType::Data);
    values_to_insert->append({int32_t{123}});
    values_to_insert->append({int32_t{456}});

    for (auto init_insert_id = uint32_t{1}; init_insert_id <= MAX_VALUE_AND_ROW_COUNT; ++init_insert_id) {
      SQLPipelineBuilder{"INSERT INTO " + table_name + " VALUES( " + std::to_string(init_insert_id) + ");"}
          .create_pipeline()
          .get_result_table();
    }

    const auto insert_thread_count = std::max(uint32_t{10}, std::thread::hardware_concurrency() / 2);
    const auto watch_thread_count = std::max(uint32_t{10}, std::thread::hardware_concurrency() / 2);

    auto insert_threads = std::vector<std::thread>{};
    insert_threads.reserve(insert_thread_count);

    const auto start = std::chrono::system_clock::now();
    auto start_flag = std::atomic_flag{};
    auto stop_flag = std::atomic_flag{};

    for (auto thread_id = uint32_t{0}; thread_id < insert_thread_count; ++thread_id) {
      insert_threads.emplace_back([&]() {
        start_flag.wait(false);
        for (auto loop_id = uint32_t{0}; loop_id < MAX_LOOP_COUNT && std::chrono::system_clock::now() < start + RUNTIME;
             ++loop_id) {
          const auto table_wrapper = std::make_shared<TableWrapper>(values_to_insert);
          const auto insert = std::make_shared<Insert>(table_name, table_wrapper);

          const auto transaction_context = Hyrise::get().transaction_manager.new_transaction_context(AutoCommit::No);
          insert->set_transaction_context(transaction_context);
          table_wrapper->execute();
          insert->execute();
          ASSERT_FALSE(insert->execute_failed());
          transaction_context->rollback(RollbackReason::User);
        }
      });
    }

    auto watch_threads = std::vector<std::thread>{};
    watch_threads.reserve(watch_thread_count);

    for (auto thread_id = uint32_t{0}; thread_id < watch_thread_count; ++thread_id) {
      watch_threads.emplace_back([&]() {
        while (!stop_flag.test()) {
          {
            const auto [status, result_table] =
                SQLPipelineBuilder{"SELECT count(*) from " + table_name + ";"}.create_pipeline().get_result_table();
            ASSERT_EQ(status, SQLPipelineStatus::Success);
            const auto visible_row_count = result_table->get_value<int64_t>(ColumnID{0}, 0);
            ASSERT_TRUE(visible_row_count);
            ASSERT_EQ(*visible_row_count, MAX_VALUE_AND_ROW_COUNT);
          }

          {
            const auto [status, result_table] =
                SQLPipelineBuilder{"SELECT max(a) from " + table_name + ";"}.create_pipeline().get_result_table();
            ASSERT_EQ(status, SQLPipelineStatus::Success);
            const auto max_value = result_table->get_value<int32_t>(ColumnID{0}, 0);
            ASSERT_TRUE(max_value);
            ASSERT_EQ(*max_value, MAX_VALUE_AND_ROW_COUNT);
          }
        }
      });
    }

    // Start inserting threads.
    start_flag.test_and_set();
    start_flag.notify_all();

    for (auto& thread : insert_threads) {
      thread.join();
    }

    // Notifying watch threads that insert rollbacks are done so we can stop watching.
    stop_flag.test_and_set();

    for (auto& thread : watch_threads) {
      thread.join();
    }
  }
}

/**
 * Test that adding and accessing the TableKeyConstraints of a table concurrently does not lead to 
 * deadlocks or inconsistencies (e.g., duplicate constraints).
 */
TEST_F(StressTest, AddModifyTableKeyConstraintsConcurrently) {
  // Create a table with multiple TableKeyConstraints.
  auto table = std::make_shared<Table>(
      TableColumnDefinitions{{"a", DataType::Int, false}, {"b", DataType::Int, false}, {"c", DataType::Int, false}},
      TableType::Data, std::nullopt, UseMvcc::Yes);
  primary_key_constraint(table, {"a"});

  table->append({1, 1, 1});
  table->append({2, 2, 2});
  table->append({3, 3, 1});

  Hyrise::get().storage_manager.add_table("dummy_table", table);
  /** 
   * This test runs insertions and reads concurrently. Specifically, it tests the following functions:
   * - `UccDiscoveryPlugin::_validate_ucc_candidates`
   * - `StoredTableNode::unique_column_combinations`
   * In order to simulate insertions parallel to the reads, we have to clear the constraints in the table. As this is
   * only needed for the test, we can use a mutex to ensure that this does not happen in parallel.
   */

  Hyrise::get().default_pqp_cache = std::make_shared<SQLPhysicalPlanCache>();
  Hyrise::get().default_lqp_cache = std::make_shared<SQLLogicalPlanCache>();
  Hyrise::get().plugin_manager.load_plugin(build_dylib_path("libhyriseUccDiscoveryPlugin"));

  auto deletion_mutex = std::shared_mutex{};

  auto start_flag = std::atomic_flag{};
  auto stop_flag = std::atomic_flag{};

  // We need this flag to prevent the 'stored_table_node_constraint_access' threads from continuously reacquiring
  // shared locks on the `deletion_mutex`, starving the `validate_constraint` thread. For more details, see
  // https://stackoverflow.com/questions/32243245/can-thread-trying-to-stdlock-unique-an-stdshared-mutex-be-starved
  auto writer_waiting_flag = std::atomic_flag{};

  const auto VALIDATION_COUNT = uint32_t{100};
  const auto SLEEP_TIME = std::chrono::milliseconds{1};

  const auto validate_constraint = [&] {
    start_flag.wait(false);
    for (auto i = uint32_t{0}; i < VALIDATION_COUNT; ++i) {
      // Populate the plan cache.
      const auto sql = std::string{"SELECT b,c FROM dummy_table GROUP BY b,c;"};
      auto pipeline = SQLPipelineBuilder{sql}.create_pipeline();
      pipeline.get_result_table();

      Hyrise::get().plugin_manager.exec_user_function("hyriseUccDiscoveryPlugin", "DiscoverUCCs");

      std::this_thread::sleep_for(SLEEP_TIME);
      // Notify the reading threads that the writer is waiting.
      writer_waiting_flag.test_and_set();
      const auto lock = std::unique_lock{deletion_mutex};
      // We need to clear the constraints to simulate concurrent insertions. Normally, a deletion of a constraint would
      // not happen at all. Instead, we store that this constraint was invalidated for the specific commit ID. This
      // prevents unnecessary revalidation of constraints that are known to be invalid.
      clear_soft_key_constraints(table);
      writer_waiting_flag.clear();
      writer_waiting_flag.notify_all();
    }
  };

  const auto stored_table_node_constraint_access = [&] {
    start_flag.wait(false);
    while (!stop_flag.test()) {
      // Prevent this thread from starving the `validate_constraint` thread by continously acquiring `shared_locks`.
      writer_waiting_flag.wait(true);
      const auto stored_table_node = std::make_shared<StoredTableNode>("dummy_table");
      // Access the unique column combinations. We need to lock here because `unique_column_combinations` uses a
      // reference to iterate over the constraints. This reference is invalidated when the constraints are cleared.
      const auto lock = std::shared_lock{deletion_mutex};
      // Check that the set of TableKeyConstraints does not contain any duplicates.
      ASSERT_LE(stored_table_node->unique_column_combinations().size(), 3);
    }
  };

  // Start running the different modifications in parallel.
  const auto thread_count = 100;
  auto threads = std::vector<std::thread>{};
  threads.reserve(thread_count);

  for (auto thread_id = uint32_t{0}; thread_id < thread_count; ++thread_id) {
    threads.emplace_back(stored_table_node_constraint_access);
  }

  // The constraint validation is run in a single thread as it is not needed to be run in parallel.
  auto validation_thread = std::thread(validate_constraint);

  start_flag.test_and_set();
  start_flag.notify_all();

  validation_thread.join();

  stop_flag.test_and_set();
  stop_flag.notify_all();

  for (auto& thread : threads) {
    thread.join();
  }

  // The constraints were cleared, so we expect no constraints to be present anymore.
  ASSERT_LE(table->soft_key_constraints().size(), 0);
}

}  // namespace hyrise
