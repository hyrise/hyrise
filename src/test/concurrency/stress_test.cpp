#include <future>
#include <thread>

#include "base_test.hpp"

#include "hyrise.hpp"
#include "scheduler/node_queue_scheduler.hpp"

#include "server/server.hpp"

namespace opossum {

class StressTest : public BaseTest {
 protected:
  void SetUp() override {
    Hyrise::reset();

    auto table_a = load_table("resources/test_data/tbl/int_float.tbl", 2);
    Hyrise::get().storage_manager.add_table("table_a", table_a);

    // Set scheduler so that the server can execute the tasks on separate threads.
    Hyrise::get().set_scheduler(std::make_shared<NodeQueueScheduler>());
  }
};

TEST_F(StressTest, TestTransactionConflicts) {
  // Similar to TestParallelConnections, but this time we modify the table
  const std::string sql = "UPDATE table_a SET b = b + 1 WHERE b = (SELECT MIN(b) FROM table_a);";

  const auto run = [&]() {
    auto pipeline =
        SQLPipelineBuilder{std::string{"UPDATE table_a SET b = b + 1 WHERE b = (SELECT MIN(b) FROM table_a);"}}
            .create_pipeline();
    (void)pipeline.get_result_table();
  };

  const auto num_threads = 100u;
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

  auto pipeline = SQLPipelineBuilder{std::string{"SELECT MIN(b) FROM table_a"}}.create_pipeline();
  const auto [_, table] = pipeline.get_result_table();
  std::cout << table->get_value<float>(ColumnID{0}, 0) << std::endl;
}

}  // namespace opossum
