#include <limits>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <future>
#include <random>
#include <thread>
#include "base_test.hpp"

#include "concurrency/commit_context.hpp"
#include "types.hpp"

namespace hyrise {

class BufferManagerStressTest : public BaseTest {
 protected:
  void SetUp() override {}
};

/**
 * This stress test is used to test the concurrency of the buffer manager. It spawns a number of threads that read and write to a 
 * small amount of random pages.
*/
TEST_F(BufferManagerStressTest, TestPinAndUnpins) {
  auto config = BufferManagerConfig{};
  config.dram_buffer_pool_size = 1 << 20;
  auto bm = BufferManager{config};

  constexpr auto NUM_REQUESTS = 10000;
  constexpr auto SEED = 18731283;
  constexpr auto PAGE_IDX_MAX = 50;
  constexpr auto RW_RATIO = 0.3;

  struct PageRequest {
    PageID page_id;
    AccessIntent access_intent;
    char timestamp{0};
  };

  std::vector<PageRequest> requests{NUM_REQUESTS};
  std::mt19937_64 gen{SEED};
  std::uniform_int_distribution<size_t> page_idx_dist{0, PAGE_IDX_MAX};

  std::uniform_real_distribution<double> rw_dist{0, 1};
  std::atomic<size_t> request_idx{0};

  std::generate(requests.begin(), requests.end(), [&]() {
    auto page_id = PageID{magic_enum::enum_value<PageSizeType>(0), page_idx_dist(gen)};
    auto access_intent = rw_dist(gen) < RW_RATIO ? AccessIntent::Read : AccessIntent::Write;
    return PageRequest{page_id, access_intent};
  });

  // Warmup pages
  for (auto& request : requests) {
    bm.pin_exclusive(request.page_id);
    std::memset(bm._get_page_ptr(request.page_id), request.timestamp, request.page_id.num_bytes());
    bm.unpin_exclusive(request.page_id);
  }

  const auto run = [&]() {
    auto current = request_idx.fetch_add(1);
    if (current >= NUM_REQUESTS) {
      return;
    }

    auto& request = requests[current];
    if (request.access_intent == AccessIntent::Read) {
      bm.pin_shared(request.page_id);
      const auto timestamp = request.timestamp;
      const auto page = bm._get_page_ptr(request.page_id);
      for (auto i = 0; i < request.page_id.num_bytes(); ++i) {
        ASSERT_EQ(static_cast<char>(page[i]), timestamp);
      }
      bm.unpin_shared(request.page_id);
    } else {
      bm.pin_exclusive(request.page_id);
      const auto timestamp = request.timestamp++;
      std::memset(bm._get_page_ptr(request.page_id), timestamp, request.page_id.num_bytes());
      const auto page = bm._get_page_ptr(request.page_id);
      for (auto i = 0; i < request.page_id.num_bytes(); ++i) {
        ASSERT_EQ(static_cast<char>(page[i]), timestamp);
      }
      bm.unpin_exclusive(request.page_id);
    }
  };

  // Create the async objects and spawn them asynchronously (i.e., as their own threads)
  // Note that async has a bunch of issues:
  //  - https://stackoverflow.com/questions/12508653/what-is-the-issue-with-stdasync
  //  - Mastering the C++17 STL, pages 205f
  // TODO(anyone): Change this to proper threads+futures, or at least do not reuse this code.
  const auto num_threads = uint32_t{std::thread::hardware_concurrency()};
  std::vector<std::future<void>> thread_futures;
  thread_futures.reserve(num_threads);

  for (auto thread_num = uint32_t{0}; thread_num < num_threads; ++thread_num) {
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
}

}  // namespace hyrise