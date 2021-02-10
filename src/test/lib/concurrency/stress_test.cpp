#include <future>
#include <thread>

#include "base_test.hpp"

#include "hyrise.hpp"
#include "scheduler/node_queue_scheduler.hpp"

namespace opossum {

class StressTest : public BaseTest {
 protected:
  void SetUp() override {
    Hyrise::reset();

    // Set scheduler so that we can execute multiple SQL statements on separate threads.
    Hyrise::get().set_scheduler(std::make_shared<NodeQueueScheduler>());
  }
};

TEST_F(StressTest, TestTransactionConflicts) {
  // Update a table with two entries and a chunk size of 2. This will lead to a high number of transaction conflicts
  // and many chunks being created
  auto table_a = load_table("resources/test_data/tbl/int_float.tbl", 2);
  Hyrise::get().storage_manager.add_table("table_a", table_a);
  auto initial_sum = int64_t{};

  {
    auto pipeline = SQLPipelineBuilder{std::string{"SELECT SUM(a) FROM table_a"}}.create_pipeline();
    const auto [_, verification_table] = pipeline.get_result_table();
    initial_sum = *verification_table->get_value<int64_t>(ColumnID{0}, 0);
  }

  std::atomic_int successful_increments{0};
  std::atomic_int conflicted_increments{0};
  const auto iterations_per_thread = 20;

  // Define the work package
  const auto run = [&]() {
    int my_successful_increments{0};
    int my_conflicted_increments{0};
    for (auto iteration = 0; iteration < iterations_per_thread; ++iteration) {
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

  // Create the async objects and spawn them asynchronously (i.e., as their own threads)
  // Note that async has a bunch of issues:
  //  - https://stackoverflow.com/questions/12508653/what-is-the-issue-with-stdasync
  //  - Mastering the C++17 STL, pages 205f
  // TODO(anyone): Change this to proper threads+futures, or at least do not reuse this code.
  const auto num_threads = 100u;
  std::vector<std::future<void>> thread_futures;
  thread_futures.reserve(num_threads);

  for (auto thread_num = 0u; thread_num < num_threads; ++thread_num) {
    // We want a future to the thread running, so we can kill it after a future.wait(timeout) or the test would freeze
    thread_futures.emplace_back(std::async(std::launch::async, run));
  }

  // Wait for completion or timeout (should not occur)
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
  auto final_sum = int64_t{};
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
  TableColumnDefinitions column_definitions;
  column_definitions.emplace_back("a", DataType::Int, false);
  column_definitions.emplace_back("b", DataType::Int, false);
  const auto table = std::make_shared<Table>(column_definitions, TableType::Data, 3, UseMvcc::Yes);
  Hyrise::get().storage_manager.add_table("table_b", table);

  const auto iterations_per_thread = 20;

  // Define the work package - the job id is used so that each thread has its own logical row to work on
  std::atomic_int job_id{0};
  const auto run = [&]() {
    const auto my_job_id = job_id++;
    for (auto iteration = 0; iteration < iterations_per_thread; ++iteration) {
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
  const auto num_threads = 100u;
  std::vector<std::future<void>> thread_futures;
  thread_futures.reserve(num_threads);

  for (auto thread_num = 0u; thread_num < num_threads; ++thread_num) {
    // We want a future to the thread running, so we can kill it after a future.wait(timeout) or the test would freeze
    thread_futures.emplace_back(std::async(std::launch::async, run));
  }

  // Wait for completion or timeout (should not occur)
  for (auto& thread_future : thread_futures) {
    // We give this a lot of time, not because we usually need that long for 100 threads to finish, but because
    // sanitizers and other tools like valgrind sometimes bring a high overhead.
    if (thread_future.wait_for(std::chrono::seconds(600)) == std::future_status::timeout) {
      ASSERT_TRUE(false) << "At least one thread got stuck and did not commit.";
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

  TableColumnDefinitions column_definitions;
  column_definitions.emplace_back("a", DataType::Int, false);
  column_definitions.emplace_back("b", DataType::Int, true);
  const auto table = std::make_shared<Table>(column_definitions, TableType::Data, Chunk::DEFAULT_SIZE, UseMvcc::Yes);
  Hyrise::get().storage_manager.add_table("table_c", table);

  const auto iterations_per_thread = 200;

  // Define the work package - each job writes a=job_id, b=(NULL or 1, depending on job_id)
  std::atomic_int job_id{0};
  const auto run = [&]() {
    const auto my_job_id = job_id++;
    for (auto iteration = 0; iteration < iterations_per_thread; ++iteration) {
      // b is set to NULL by half of the jobs.
      auto pipeline = SQLPipelineBuilder{std::string{"INSERT INTO table_c (a, b) VALUES ("} +
                                         std::to_string(my_job_id) + ", " + (my_job_id % 2 ? "NULL" : "1") + ")"}
                          .create_pipeline();
      const auto [status, _] = pipeline.get_result_table();
      EXPECT_EQ(status, SQLPipelineStatus::Success);
    }
  };

  // Create the async objects and spawn them asynchronously (i.e., as their own threads)
  const auto num_threads = 20u;
  std::vector<std::future<void>> thread_futures;
  thread_futures.reserve(num_threads);

  for (auto thread_num = 0u; thread_num < num_threads; ++thread_num) {
    // We want a future to the thread running, so we can kill it after a future.wait(timeout) or the test would freeze
    thread_futures.emplace_back(std::async(std::launch::async, run));
  }

  // Wait for completion or timeout (should not occur)
  for (auto& thread_future : thread_futures) {
    // We give this a lot of time, not because we usually need that long for 100 threads to finish, but because
    // sanitizers and other tools like valgrind sometimes bring a high overhead.
    if (thread_future.wait_for(std::chrono::seconds(600)) == std::future_status::timeout) {
      ASSERT_TRUE(false) << "At least one thread got stuck and did not commit.";
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

}  // namespace opossum
