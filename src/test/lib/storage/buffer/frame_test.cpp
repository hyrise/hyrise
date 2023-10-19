#include "base_test.hpp"
#include "storage/buffer/frame.hpp"

namespace hyrise {

class FrameTest : public BaseTest {};

TEST_F(FrameTest, TestStateTransitions) {
  auto frame = Frame{};
  EXPECT_EQ(Frame::state(frame.state_and_version()), Frame::EVICTED);
  EXPECT_EQ(Frame::version(frame.state_and_version()), 0);

  auto old_state_and_version = frame.state_and_version();
  EXPECT_TRUE(frame.try_lock_exclusive(old_state_and_version));
  EXPECT_EQ(Frame::version(frame.state_and_version()), 0);
  EXPECT_EQ(Frame::state(frame.state_and_version()), Frame::LOCKED);
  EXPECT_FALSE(frame.try_lock_exclusive(old_state_and_version));
  EXPECT_FALSE(frame.try_lock_shared(old_state_and_version));

  frame.unlock_exclusive();
  EXPECT_EQ(Frame::state(frame.state_and_version()), Frame::UNLOCKED);
  EXPECT_EQ(Frame::version(frame.state_and_version()), 1);
  EXPECT_TRUE(frame.is_unlocked());

  // Lock 3 times in shared mode
  old_state_and_version = frame.state_and_version();
  EXPECT_TRUE(frame.try_lock_shared(old_state_and_version));
  EXPECT_EQ(Frame::version(frame.state_and_version()), 1);
  EXPECT_EQ(Frame::state(frame.state_and_version()), 1);

  old_state_and_version = frame.state_and_version();
  EXPECT_TRUE(frame.try_lock_shared(old_state_and_version));
  EXPECT_EQ(Frame::version(frame.state_and_version()), 1);
  EXPECT_EQ(Frame::state(frame.state_and_version()), 2);

  old_state_and_version = frame.state_and_version();
  EXPECT_TRUE(frame.try_lock_shared(old_state_and_version));
  EXPECT_EQ(Frame::version(frame.state_and_version()), 1);
  EXPECT_EQ(Frame::state(frame.state_and_version()), 3);

  // Unlock 3 times in shared mode
  EXPECT_FALSE(frame.unlock_shared());
  EXPECT_EQ(Frame::version(frame.state_and_version()), 1);
  EXPECT_EQ(Frame::state(frame.state_and_version()), 2);

  EXPECT_FALSE(frame.unlock_shared());
  EXPECT_EQ(Frame::version(frame.state_and_version()), 1);
  EXPECT_EQ(Frame::state(frame.state_and_version()), 1);

  EXPECT_TRUE(frame.unlock_shared());
  EXPECT_EQ(Frame::version(frame.state_and_version()), 1);
  EXPECT_EQ(Frame::state(frame.state_and_version()), Frame::UNLOCKED);

  // Try Mark
  old_state_and_version = frame.state_and_version();
  EXPECT_TRUE(frame.try_mark(old_state_and_version));
  EXPECT_EQ(Frame::version(frame.state_and_version()), 1);
  EXPECT_EQ(Frame::state(frame.state_and_version()), Frame::MARKED);

  // Lock shared again and unlock
  old_state_and_version = frame.state_and_version();
  EXPECT_TRUE(frame.try_lock_shared(old_state_and_version));
  EXPECT_EQ(Frame::version(frame.state_and_version()), 1);
  EXPECT_EQ(Frame::state(frame.state_and_version()), 1);
  frame.unlock_shared();

  // Evict again after locking
  frame.try_lock_exclusive(frame.state_and_version());
  EXPECT_EQ(Frame::state(frame.state_and_version()), Frame::LOCKED);
  frame.unlock_exclusive_and_set_evicted();
  EXPECT_EQ(Frame::state(frame.state_and_version()), Frame::EVICTED);
}

TEST_F(FrameTest, TestSetNodeID) {
  auto frame = Frame{};
  EXPECT_EQ(frame.node_id(), 0);
  EXPECT_TRUE(frame.try_lock_exclusive(frame.state_and_version()));
  frame.set_node_id(NodeID{13});
  frame.unlock_exclusive();
  EXPECT_EQ(frame.node_id(), NodeID{13});
}

TEST_F(FrameTest, TestSetDirty) {
  auto frame = Frame{};
  frame.try_lock_exclusive(frame.state_and_version());
  EXPECT_FALSE(frame.is_dirty());
  frame.mark_dirty();
  EXPECT_TRUE(frame.is_dirty());
  frame.mark_dirty();
  EXPECT_TRUE(frame.is_dirty());
  frame.reset_dirty();
  EXPECT_FALSE(frame.is_dirty());

  frame.unlock_exclusive();
}

TEST_F(FrameTest, TestStreamOperator) {
  auto frame = Frame{};
  {
    std::stringstream out;
    out << frame;
    EXPECT_EQ(out.str(), "Frame { state = EVICTED, node_id = 0, dirty = 0, version = 0}");
  }

  frame.try_lock_exclusive(frame.state_and_version());
  {
    std::stringstream out;
    out << frame;
    EXPECT_EQ(out.str(), "Frame { state = LOCKED, node_id = 0, dirty = 0, version = 0}");
  }

  frame.mark_dirty();
  {
    std::stringstream out;
    out << frame;
    EXPECT_EQ(out.str(), "Frame { state = LOCKED, node_id = 0, dirty = 1, version = 0}");
  }

  frame.set_node_id(NodeID{13});
  {
    std::stringstream out;
    out << frame;
    EXPECT_EQ(out.str(), "Frame { state = LOCKED, node_id = 13, dirty = 1, version = 0}");
  }

  frame.unlock_exclusive();
  {
    std::stringstream out;
    out << frame;
    EXPECT_EQ(out.str(), "Frame { state = UNLOCKED, node_id = 13, dirty = 1, version = 1}");
  }

  frame.try_lock_shared(frame.state_and_version());
  frame.try_lock_shared(frame.state_and_version());
  {
    std::stringstream out;
    out << frame;
    EXPECT_EQ(out.str(), "Frame { state = LOCKED_SHARED (2), node_id = 13, dirty = 1, version = 1}");
  }
  frame.unlock_shared();
  frame.unlock_shared();
  frame.try_mark(frame.state_and_version());
  {
    std::stringstream out;
    out << frame;
    EXPECT_EQ(out.str(), "Frame { state = MARKED, node_id = 13, dirty = 1, version = 1}");
  }
}

}  // namespace hyrise
