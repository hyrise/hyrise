#include "base_test.hpp"

#include <atomic>
#include <chrono>
#include <future>
#include <numeric>
#include <thread>

#include "benchmark_config.hpp"
#include "hyrise.hpp"
#include "operators/insert.hpp"
#include "operators/projection.hpp"
#include "operators/table_wrapper.hpp"
#include "operators/union_all.hpp"
#include "scheduler/node_queue_scheduler.hpp"
#include "scheduler/task_queue.hpp"
#include "sql/sql_pipeline_builder.hpp"
#include "storage/table.hpp"
#include "storage/table_column_definition.hpp"
#include "tpch/tpch_constants.hpp"
#include "tpch/tpch_table_generator.hpp"
#include "utils/atomic_max.hpp"

namespace hyrise {

class StressTest : public BaseTest {
 protected:
  void SetUp() override {
    // Set scheduler so that we can execute multiple SQL statements on separate threads.
    Hyrise::get().set_scheduler(std::make_shared<NodeQueueScheduler>());
  }

  const uint32_t CORES_PER_NODE = std::thread::hardware_concurrency();
  const std::vector<std::vector<uint32_t>> FAKE_SINGLE_NODE_NUMA_TOPOLOGIES = {
      {CORES_PER_NODE}, {CORES_PER_NODE, 0, 0}, {0, CORES_PER_NODE, 0}, {0, 0, CORES_PER_NODE}};

  const std::vector<std::vector<uint32_t>> FAKE_MULTI_NODE_NUMA_TOPOLOGIES = {{CORES_PER_NODE, CORES_PER_NODE, 0, 0},
                                                                              {0, CORES_PER_NODE, CORES_PER_NODE, 0},
                                                                              {0, 0, CORES_PER_NODE, CORES_PER_NODE}};
};

TEST_F(StressTest, TestTransactionConflicts) {
  // Update a table with two entries and a chunk size of 2. This will lead to a high number of transaction conflicts
  // and many chunks being created
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

  // Define the work package
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

  // Create the async objects and spawn them asynchronously (i.e., as their own threads).
  // Note that async has a bunch of issues:
  //  - https://stackoverflow.com/questions/12508653/what-is-the-issue-with-stdasync
  //  - Mastering the C++17 STL, pages 205f
  // TODO(anyone): Change this to proper threads+futures, or at least do not reuse this code.
  const auto num_threads = uint32_t{100};
  auto thread_futures = std::vector<std::future<void>>{};
  thread_futures.reserve(num_threads);

  for (auto thread_num = uint32_t{0}; thread_num < num_threads; ++thread_num) {
    // We want a future to the thread running, so we can kill it after a future.wait(timeout) or the test would freeze.
    thread_futures.emplace_back(std::async(std::launch::async, run));
  }

  // Wait for completion or timeout (should not occur).
  for (auto& thread_future : thread_futures) {
    // We give this a lot of time, not because we usually need that long for 100 threads to finish, but because
    // sanitizers and other tools like valgrind sometimes bring a high overhead.
    if (thread_future.wait_for(std::chrono::seconds(180)) == std::future_status::timeout) {
      ASSERT_TRUE(false) << "At least one thread got stuck and did not commit.";
    }
    // Retrieve the future so that exceptions stored in its state are thrown
    thread_future.get();
  }

  // Verify results
  auto final_sum = int64_t{0};
  {
    auto pipeline = SQLPipelineBuilder{std::string{"SELECT SUM(a) FROM table_a"}}.create_pipeline();
    const auto [_, verification_table] = pipeline.get_result_table();
    final_sum = *verification_table->get_value<int64_t>(ColumnID{0}, 0);
  }

  // Really pessimistic, but at least 2 statements should have made it
  EXPECT_GT(successful_increments, 2);

  EXPECT_EQ(successful_increments + conflicted_increments, num_threads * iterations_per_thread);
  EXPECT_EQ(final_sum - initial_sum, successful_increments);
}

TEST_F(StressTest, TestTransactionInsertsSmallChunks) {
  // An update-heavy load on a table with a ridiculously low target chunk size, creating many new chunks. This is
  // different from TestTransactionConflicts, in that each thread has its own logical row and no transaction
  // conflicts occur. In the other test, a failed "mark for deletion" (i.e., swap of the row's tid) would lead to
  // no row being appended.
  auto column_definitions = TableColumnDefinitions{};
  column_definitions.emplace_back("a", DataType::Int, false);
  column_definitions.emplace_back("b", DataType::Int, false);
  const auto table = std::make_shared<Table>(column_definitions, TableType::Data, ChunkOffset{3}, UseMvcc::Yes);
  Hyrise::get().storage_manager.add_table("table_b", table);

  const auto iterations_per_thread = uint32_t{20};

  // Define the work package - the job id is used so that each thread has its own logical row to work on
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

  // Create the async objects and spawn them asynchronously (i.e., as their own threads)
  const auto num_threads = uint32_t{100};
  auto thread_futures = std::vector<std::future<void>>{};
  thread_futures.reserve(num_threads);

  for (auto thread_num = uint32_t{0}; thread_num < num_threads; ++thread_num) {
    // We want a future to the thread running, so we can kill it after a future.wait(timeout) or the test would freeze
    thread_futures.emplace_back(std::async(std::launch::async, run));
  }

  // Wait for completion or timeout (should not occur)
  for (auto& thread_future : thread_futures) {
    // We give this a lot of time, not because we usually need that long for 100 threads to finish, but because
    // sanitizers and other tools like valgrind sometimes bring a high overhead.
    if (thread_future.wait_for(std::chrono::seconds(600)) == std::future_status::timeout) {
      FAIL() << "At least one thread got stuck and did not commit.";
    }
    // Retrieve the future so that exceptions stored in its state are thrown
    thread_future.get();
  }

  // Verify that the values in column b are correctly incremented
  {
    auto pipeline = SQLPipelineBuilder{std::string{"SELECT MIN(b) FROM table_b"}}.create_pipeline();
    const auto [_, verification_table] = pipeline.get_result_table();
    EXPECT_EQ(*verification_table->get_value<int32_t>(ColumnID{0}, 0), iterations_per_thread);
  }
}

TEST_F(StressTest, TestTransactionInsertsPackedNullValues) {
  // As ValueSegments store their null flags in a vector<bool>, which is not safe to be modified concurrently,
  // conflicts may (and have) occurred when that vector was written without any type of protection.

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

  // Create the async objects and spawn them asynchronously (i.e., as their own threads).
  const auto num_threads = uint32_t{20};
  auto thread_futures = std::vector<std::future<void>>{};
  thread_futures.reserve(num_threads);

  for (auto thread_num = uint32_t{0}; thread_num < num_threads; ++thread_num) {
    // We want a future to the thread running, so we can kill it after a future.wait(timeout) or the test would freeze
    thread_futures.emplace_back(std::async(std::launch::async, run));
  }

  // Wait for completion or timeout (should not occur)
  for (auto& thread_future : thread_futures) {
    // We give this a lot of time, not because we usually need that long for 100 threads to finish, but because
    // sanitizers and other tools like valgrind sometimes bring a high overhead.
    if (thread_future.wait_for(std::chrono::seconds(600)) == std::future_status::timeout) {
      FAIL() << "At least one thread got stuck and did not commit.";
    }
    // Retrieve the future so that exceptions stored in its state are thrown
    thread_future.get();
  }

  // Check that NULL values in column b are correctly set
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

TEST_F(StressTest, NodeSchedulerStressTest) {
  if (std::thread::hardware_concurrency() < 2) {
    GTEST_SKIP();
  }

  // Create a large number of nodes in a fake topology (many workers will share the same thread).
  const auto node_count = std::thread::hardware_concurrency() * 8;

  Hyrise::get().topology.use_fake_numa_topology(node_count, 1);
  const auto node_queue_scheduler = std::make_shared<NodeQueueScheduler>();
  Hyrise::get().set_scheduler(node_queue_scheduler);

  // Just a sufficiently large number to trigger a non-empty queue.
  const auto job_counts = std::vector<size_t>{node_count << 3u, node_count << 4u, node_count << 3u};

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
  EXPECT_TRUE(std::any_of(second_worker, node_queue_scheduler->workers().cend(),
                          [](const auto& worker) { return worker->queue()->estimate_load() > 0; }));

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
  constexpr auto SLEEP_TIME = std::chrono::milliseconds{10};
  const auto job_count = CORES_PER_NODE * 32;

  for (const auto& fake_numa_topology : FAKE_SINGLE_NODE_NUMA_TOPOLOGIES) {
    Hyrise::get().topology.use_fake_numa_topology(fake_numa_topology);

    auto node_queue_scheduler = std::make_shared<NodeQueueScheduler>();
    Hyrise::get().set_scheduler(node_queue_scheduler);

    auto counter = std::atomic<uint32_t>{0};
    auto wait_flag = std::atomic<bool>{true};

    auto waiting_jobs = std::vector<std::shared_ptr<AbstractTask>>{};
    waiting_jobs.reserve(job_count);
    for (auto job_id = size_t{0}; job_id < job_count; ++job_id) {
      waiting_jobs.emplace_back(std::make_shared<JobTask>([&] {
        while (wait_flag) {
          std::this_thread::sleep_for(SLEEP_TIME);
        }
        ++counter;
      }));
    }

    for (const auto& queue : node_queue_scheduler->queues()) {
      if (!queue) {
        continue;
      }
      EXPECT_EQ(queue->semaphore.availableApprox(), 0);
    }

    Hyrise::get().scheduler()->schedule_tasks(waiting_jobs);
    // Wait a bit for workers to pull jobs and decrement semaphore.
    std::this_thread::sleep_for(CORES_PER_NODE * SLEEP_TIME);

    for (const auto& queue : node_queue_scheduler->queues()) {
      if (!queue) {
        continue;
      }
      EXPECT_EQ(queue->semaphore.availableApprox(), job_count - CORES_PER_NODE);
    }

    wait_flag = false;
    std::this_thread::sleep_for(SLEEP_TIME);
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
  constexpr auto DEPENDENT_JOB_TASKS_LENGTH = size_t{10};
  constexpr auto SLEEP_TIME = std::chrono::milliseconds{1};
  const auto job_count = CORES_PER_NODE * 32;

  for (const auto& fake_numa_topology : FAKE_SINGLE_NODE_NUMA_TOPOLOGIES) {
    Hyrise::get().topology.use_fake_numa_topology(fake_numa_topology);
    auto node_queue_scheduler = std::make_shared<NodeQueueScheduler>();
    Hyrise::get().set_scheduler(node_queue_scheduler);

    auto counter = std::atomic<uint32_t>{0};
    auto wait_flag = std::atomic<bool>{true};

    auto waiting_jobs = std::vector<std::shared_ptr<AbstractTask>>{};
    waiting_jobs.reserve(job_count);
    for (auto job_id = size_t{0}; job_id < job_count; ++job_id) {
      waiting_jobs.emplace_back(std::make_shared<JobTask>([&] {
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

    Hyrise::get().scheduler()->schedule_tasks(waiting_jobs);
    // Wait a bit for workers to pull jobs and decrement semaphore.
    std::this_thread::sleep_for(5 * CORES_PER_NODE * SLEEP_TIME);

    // The number of scheduled jobs depends on DEPENDENT_JOB_TASKS_LENGTH (see job definition above; due to the jobs
    // dependencies, jobs are only scheduled when they have no predecessors).
    const auto executable_jobs = static_cast<float>(job_count) / static_cast<float>(DEPENDENT_JOB_TASKS_LENGTH);
    for (const auto& queue : node_queue_scheduler->queues()) {
      if (!queue) {
        continue;
      }

      // We started scheduled jobs, which block all workers due to `wait_flag` being true. Thus, the semaphore should be
      // reduced by the number of workers (i.e., CORES_PER_NODE).
      EXPECT_EQ(queue->semaphore.availableApprox(), static_cast<size_t>(std::ceil(executable_jobs)) - CORES_PER_NODE);
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
  const auto benchmark_config = BenchmarkConfig::get_default_config();

  TPCHTableGenerator(0.1f, ClusteringConfiguration::None, std::make_shared<BenchmarkConfig>(benchmark_config))
      .generate_and_store();

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

  const auto thread_count = uint32_t{100};
  const auto insert_count = uint32_t{301};
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

  // 100 threads * 301 insertions * 2 values = 60'200 tuples in 20'067 chunks with chunk size 3.
  EXPECT_EQ(table->row_count(), 60'200);
  EXPECT_EQ(table->chunk_count(), 20'067);

  // Only the final chunk is not full and not immutable.
  const auto immutable_chunk_count = table->chunk_count() - 1;
  for (auto chunk_id = ChunkID{0}; chunk_id < immutable_chunk_count; ++chunk_id) {
    const auto& chunk = table->get_chunk(chunk_id);
    ASSERT_TRUE(chunk);
    EXPECT_EQ(chunk->size(), 3);
    EXPECT_FALSE(chunk->is_mutable());
  }

  EXPECT_EQ(table->last_chunk()->size(), 2);
  EXPECT_TRUE(table->last_chunk()->is_mutable());
}

// Consuming operators register at their inputs and deregister when they are executed. Thus, operators can clear
// intermediate results. Consumer deregistration must work properly in concurrent scenarios.
TEST_F(StressTest, OperatorRegistration) {
  const auto repetition_count = uint32_t{100};
  const auto consumer_count = uint32_t{50};
  const auto sleep_time = std::chrono::milliseconds{10};

  const auto dummy_table = Table::create_dummy_table({{"a", DataType::Int, false}});

  for (auto repetition = uint32_t{0}; repetition < repetition_count; ++repetition) {
    const auto table_wrapper = std::make_shared<TableWrapper>(dummy_table);
    table_wrapper->execute();
    auto threads = std::vector<std::thread>{};
    threads.reserve(consumer_count);
    auto start_execution = std::atomic_bool{false};

    // Generate some consumers that try to deregister concurrently once they are allowed.
    for (auto consumer_id = uint32_t{0}; consumer_id < consumer_count; ++consumer_id) {
      threads.emplace_back([&]() {
        const auto union_all = std::make_shared<UnionAll>(table_wrapper, table_wrapper);

        // Wait for the signal to execute the operator.
        while (!start_execution) {
          std::this_thread::sleep_for(sleep_time);
        }

        union_all->execute();
      });
    }

    // Give the consumers some time for construction.
    std::this_thread::sleep_for(sleep_time);

    // 100 consumers because UnionAll has the input on both sides.
    EXPECT_EQ(table_wrapper->consumer_count(), 100);

    start_execution = true;
    for (auto& thread : threads) {
      thread.join();
    }

    EXPECT_EQ(table_wrapper->consumer_count(), 0);

    // One additional deregistration (without prior registration) is not allowed.
    EXPECT_THROW(table_wrapper->deregister_consumer(), std::logic_error);
  }
}

}  // namespace hyrise
