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

TEST_F(BufferManagerStressTest, TestPinAndUnpins) {
  auto bm = BufferManager{{.dram_buffer_pool_size = 1 << 20, .ssd_path = "./build"}};

  constexpr auto NUM_REQUESTS = 20000;
  constexpr auto SEED = 18731283;
  constexpr auto PAGE_IDX_MAX = 50;
  constexpr auto RW_RATIO = 0.3;

  struct PageRequest {
    PageID page_id;
    AccessIntent access_intent;
    std::chrono::microseconds access_time;
  };

  std::vector<PageRequest> requests{NUM_REQUESTS};
  std::mt19937_64 gen{SEED};
  std::uniform_int_distribution<size_t> page_idx_dist{0, 3};
  std::uniform_int_distribution<size_t> size_dist{2, 3};
  std::uniform_int_distribution<size_t> wait_dist{1, 3};

  std::uniform_real_distribution<double> rw_dist{0, 1};
  std::atomic<size_t> request_idx{0};

  std::generate(requests.begin(), requests.end(), [&]() {
    auto page_id = PageID{magic_enum::enum_value<PageSizeType>(3), 1};
    auto access_intent = rw_dist(gen) < RW_RATIO ? AccessIntent::Read : AccessIntent::Write;
    auto access_time = std::chrono::milliseconds{wait_dist(gen)};
    return PageRequest{page_id, access_intent, access_time};
  });

  // Warmup pages
  for (auto& request : requests) {
    bm.pin_for_write(request.page_id);
    bm.unpin_for_write(request.page_id);
  }

  const auto run = [&]() {
    auto current = request_idx.fetch_add(1);
    if (current >= NUM_REQUESTS) {
      return;
    }

    auto& request = requests[current];
    if (request.access_intent == AccessIntent::Read) {
      bm.pin_shared(request.page_id);
      std::cout << "pinned read" << std::endl;
      std::this_thread::sleep_for(request.access_time);
      bm.unpin_shared(request.page_id);
    } else {
      bm.pin_for_write(request.page_id);
      std::cout << "pinned write" << std::endl;
      std::this_thread::sleep_for(request.access_time);
      bm.unpin_for_write(request.page_id);
    }
  };

  // Create the async objects and spawn them asynchronously (i.e., as their own threads)
  // Note that async has a bunch of issues:
  //  - https://stackoverflow.com/questions/12508653/what-is-the-issue-with-stdasync
  //  - Mastering the C++17 STL, pages 205f
  // TODO(anyone): Change this to proper threads+futures, or at least do not reuse this code.
  const auto num_threads = uint32_t{30};
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