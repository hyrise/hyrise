#include <memory>

#include <thread>
#include "base_test.hpp"
#include "storage/buffer/hybrid_latch.hpp"

namespace hyrise {

class HybridLatchTest : public BaseTest {};

TEST_F(HybridLatchTest, TestHybridLatchExclusive) {
  auto latch = HybridLatch{};
  EXPECT_FALSE(latch.is_locked_exclusive());
  latch.lock_exclusive();
  EXPECT_TRUE(latch.is_locked_exclusive());
  latch.unlock_exclusive();
  EXPECT_FALSE(latch.is_locked_exclusive());
}

TEST_F(HybridLatchTest, TestHybridLatchShared) {
  auto latch = HybridLatch{};
  EXPECT_FALSE(latch.is_locked_exclusive());
  latch.lock_shared();
  EXPECT_FALSE(latch.is_locked_exclusive());
  latch.unlock_shared();
  EXPECT_FALSE(latch.is_locked_exclusive());
}

TEST_F(HybridLatchTest, TestHybridLatchReadOptimisticIfPossible) {
  {
    auto latch = HybridLatch{};
    EXPECT_FALSE(latch.is_locked_exclusive());
    int read_called = 0;
    latch.read_optimistic_if_possible([&]() { read_called++; });
    EXPECT_EQ(read_called, 1);
    EXPECT_FALSE(latch.is_locked_exclusive());
  }
  {
    // TODO: Test optistimis lock restart
    // auto latch = HybridLatch{};
    // latch.lock_exclusive();
    // EXPECT_TRUE(latch.is_locked_exclusive());

    // std::thread thr{[] { latch.read_optimistic_if_possible([&]() { std::cout << "Read twice"; }); }};

    // EXPECT_FALSE(latch.is_locked_exclusive());
  }
}

}  // namespace hyrise