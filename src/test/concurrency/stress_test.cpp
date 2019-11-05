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

    auto table_a = load_table("resources/test_data/tbl/int_float.tbl", 2);
    Hyrise::get().storage_manager.add_table("table_a", table_a);

    // Set scheduler so that we can execute multiple SQL statements on separate threads.
    Hyrise::get().set_scheduler(std::make_shared<NodeQueueScheduler>());
  }
};

TEST_F(StressTest, TestTransactionConflicts) {
  long initial_sum = 0.0f;
  {
    auto pipeline = SQLPipelineBuilder{std::string{"SELECT SUM(a) FROM table_a"}}.create_pipeline();
    const auto [_, table] = pipeline.get_result_table();
    initial_sum = table->get_value<long>(ColumnID{0}, 0);
  }

  // Similar to TestParallelConnections, but this time we modify the table
  const std::string sql = "UPDATE table_a SET a = a + 1 WHERE a = (SELECT MIN(a) FROM table_a);";

  std::atomic_int successful_increments{0};
  std::atomic_int conflicted_increments{0};
  const auto run = [&]() {
    auto pipeline =
        SQLPipelineBuilder{std::string{"UPDATE table_a SET a = a + 1 WHERE a = (SELECT MIN(a) FROM table_a);"}}
            .create_pipeline();
    const auto [status, _] = pipeline.get_result_table();
    if (status == SQLPipelineStatus::Success) {
      ++successful_increments;
    } else {
      ++conflicted_increments;
    }
  };

  const auto num_threads = 1000u;
  std::vector<std::future<void>> thread_futures;
  thread_futures.reserve(num_threads);

  for (auto thread_num = 0u; thread_num < num_threads; ++thread_num) {
    // We want a future to the thread running, so we can kill it after a future.wait(timeout) or the test would freeze
    thread_futures.emplace_back(std::async(std::launch::async, run));
  }

  for (auto& thread_future : thread_futures) {
    // We give this a lot of time, not because we need that long for 100 threads to finish, but because sanitizers and
    // other tools like valgrind sometimes bring a high overhead that exceeds 10 seconds.
    if (thread_future.wait_for(std::chrono::seconds(150)) == std::future_status::timeout) {
      ASSERT_TRUE(false) << "At least one thread got stuck and did not commit.";
    }
  }

  auto final_sum = 0.0f;
  {
    auto pipeline = SQLPipelineBuilder{std::string{"SELECT SUM(a) FROM table_a"}}.create_pipeline();
    const auto [_, table] = pipeline.get_result_table();
    final_sum = table->get_value<long>(ColumnID{0}, 0);
  }

  // Really pessimistic, but at least 2 statements should have made it
  EXPECT_GT(successful_increments, 2);

  EXPECT_EQ(successful_increments + conflicted_increments, num_threads);
  EXPECT_FLOAT_EQ(final_sum - initial_sum, successful_increments);
}

}  // namespace opossum
