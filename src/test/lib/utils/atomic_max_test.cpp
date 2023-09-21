#include "base_test.hpp"

#include "utils/atomic_max.hpp"

namespace hyrise {

class AtomicMaxTest : public BaseTest {};

TEST_F(AtomicMaxTest, SingleUpdate) {
  auto counter = std::atomic_uint32_t{10};

  set_atomic_max(counter, uint32_t{5});
  EXPECT_EQ(counter.load(), 10);

  set_atomic_max(counter, uint32_t{15});
  EXPECT_EQ(counter.load(), 15);
}

TEST_F(AtomicMaxTest, IgnoreMaxCommitID) {
  // The CommitID specialization should treat MAX_COMMIT_ID like an unset value.
  auto counter = std::atomic<CommitID>{MvccData::MAX_COMMIT_ID};

  EXPECT_EQ(counter.load(), MvccData::MAX_COMMIT_ID);

  set_atomic_max(counter, CommitID{0});
  EXPECT_EQ(counter.load(), CommitID{0});
}

// A concurrency stress test can be found at `stress_test.cpp` (AtomicMaxConcurrentUpdate).

}  // namespace hyrise
