#include <memory>

#include "base_test.hpp"

#include "storage/buffer/clock_replacement_strategy.hpp"
#include "types.hpp"

namespace hyrise {

class ClockReplacementStrategyTest : public BaseTest {};

TEST_F(ClockReplacementStrategyTest, TestReplacementSimple) {
  // TODO: Test if moving in frame access?
  auto strategy = ClockReplacementStrategy(3);
  strategy.record_frame_access(FrameID{0});
  strategy.record_frame_access(FrameID{1});
  strategy.record_frame_access(FrameID{2});

  // EXPECT_EQ(strategy.find_victim(), FrameID{1};
}

}  // namespace hyrise