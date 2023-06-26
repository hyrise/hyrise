#include "base_test.hpp"

#include "utils/atomic_max.hpp"

namespace hyrise {

class AtomicMaxTest : public BaseTest {};

TEST_F(AtomicMaxTest, SingleUpdate) {
  auto counter = std::atomic_uint32_t{10};

  set_atomic_max(counter, uint32_t{5});
  EXPECT_EQ(counter, 10);

  set_atomic_max(counter, uint32_t{15});
  EXPECT_EQ(counter, 15);
}

TEST_F(AtomicMaxTest, ConcurrentUpdate) {
  auto counter = std::atomic_uint32_t{0};
  const auto thread_count = 50;
  const auto repetitions = 100;

  auto threads = std::vector<std::thread>{};
  threads.reserve(thread_count);

  for (auto thread_id = uint32_t{1}; thread_id <= thread_count; ++thread_id) {
    threads.emplace_back(std::thread{[thread_id, &counter]() {
      for (auto i = uint32_t{1}; i <= repetitions; ++i) {
        set_atomic_max(counter, thread_id + i);
      }
    }});
  }

  for (auto& thread : threads) {
    thread.join();
  }

  // Highest thread ID is 50. 50 + 100 = 150.
  EXPECT_EQ(counter, 150);
}

TEST_F(AtomicMaxTest, IgnoreMaxCommitID) {
  // The CommitID specialization should treat MAX_COMMIT_ID like an unset value.
  auto counter = std::atomic<CommitID>{MvccData::MAX_COMMIT_ID};

  EXPECT_EQ(counter, MvccData::MAX_COMMIT_ID);

  set_atomic_max(counter, CommitID{0});
  EXPECT_EQ(counter, CommitID{0});
}

}  // namespace hyrise
