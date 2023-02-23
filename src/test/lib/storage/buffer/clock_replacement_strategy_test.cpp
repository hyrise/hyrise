#include <memory>

#include "base_test.hpp"

#include "storage/buffer/clock_replacement_strategy.hpp"
#include "types.hpp"

namespace hyrise {

class ClockReplacementStrategyTest : public BaseTest {};

TEST_F(ClockReplacementStrategyTest, TestReplacementSimple) {
  // TODO: Test if moving in frame access?
  auto strategy = ClockReplacementStrategy(5);
  strategy.record_frame_access(FrameID{1});
  strategy.record_frame_access(FrameID{0});

  EXPECT_EQ(strategy.find_victim(), FrameID{1});
}

TEST_F(ClockReplacementStrategyTest, TestReplacementSingle) {
  auto strategy = ClockReplacementStrategy(1);
  strategy.record_frame_access(FrameID{0});

  EXPECT_EQ(strategy.find_victim(), FrameID{0});
}

TEST_F(ClockReplacementStrategyTest, TestReplacementAllInUse) {}

TEST_F(ClockReplacementStrategyTest, TestReplacementAllReferenced) {
  auto strategy = ClockReplacementStrategy(3);

  strategy.record_frame_access(FrameID{1});
  strategy.record_frame_access(FrameID{0});
  strategy.record_frame_access(FrameID{2});

  EXPECT_EQ(strategy.find_victim(), FrameID{1});
}

TEST_F(ClockReplacementStrategyTest, TestMultipleRecordAndFindVictims) {
  auto strategy = ClockReplacementStrategy(4);

    strategy.record_frame_access(FrameID{1});

}

TEST_F(ClockReplacementStrategyTest, TestReplacementAllPinned) {
  auto strategy = ClockReplacementStrategy(3);

  strategy.record_frame_access(FrameID{0});
  strategy.record_frame_access(FrameID{1});
  strategy.record_frame_access(FrameID{2});

  strategy.pin(FrameID{0});
  strategy.pin(FrameID{1});
  strategy.pin(FrameID{2});

  EXPECT_ANY_THROW(strategy.find_victim());
}

// TODO: Include unpin

}  // namespace hyrise