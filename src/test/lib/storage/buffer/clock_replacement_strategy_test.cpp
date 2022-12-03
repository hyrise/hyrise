#include <memory>

#include "base_test.hpp"

#include "storage/buffer/clock_replacement_strategy.hpp"
#include "types.hpp"

namespace hyrise {

class ClockReplacementStrategyTest : public BaseTest {};

TEST_F(ClockReplacementStrategyTest, TestReplacement) {
  auto replacement_strategy = ClockReplacementStrategy(3);
  replacement_strategy.pin(FrameID{2});
  replacement_strategy.pin(FrameID{4});
  replacement_strategy.pin(FrameID{5});
  replacement_strategy.pin(FrameID{2});
  replacement_strategy.pin(FrameID{6});
  replacement_strategy.pin(FrameID{5});
  replacement_strategy.pin(FrameID{4});
  replacement_strategy.pin(FrameID{1});
  replacement_strategy.pin(FrameID{3});
  replacement_strategy.pin(FrameID{2});
  //https://www.youtube.com/watch?v=JQ4KRPuP-LY
}

}  // namespace hyrise