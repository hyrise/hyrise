#include "base_test.hpp"
#include "utils/pausable_loop_thread.hpp"

namespace hyrise {

class PausableLoopThreadTest : public BaseTest {
 protected:
  void SetUp() override {
    call_count = 0;
  }

  std::atomic<size_t> call_count{0};
};

TEST_F(PausableLoopThreadTest, BasicExecution) {
  // Just create a thread and destroy it after 15ms, the count should be at least 1.
  {
    PausableLoopThread loop(std::chrono::milliseconds(10), [&](size_t) {
      ++call_count;
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(15));
  }
  EXPECT_GE(call_count.load(), 1);
}

TEST_F(PausableLoopThreadTest, PauseAndResume) {
  PausableLoopThread loop(std::chrono::milliseconds(5), [&](size_t) {
    ++call_count;
  });

  std::this_thread::sleep_for(std::chrono::milliseconds(20));
  loop.pause();

  auto paused_count = call_count.load();
  std::this_thread::sleep_for(std::chrono::milliseconds(30));
  EXPECT_EQ(paused_count, call_count.load());

  loop.resume();
  std::this_thread::sleep_for(std::chrono::milliseconds(20));
  EXPECT_GT(call_count.load(), paused_count);
}

TEST_F(PausableLoopThreadTest, DestructorStopsThread) {
  std::atomic<size_t> temp{0};
  {
    PausableLoopThread loop(std::chrono::milliseconds(10), [&](size_t) {
      ++temp;
      ++call_count;
    });
    std::this_thread::sleep_for(std::chrono::milliseconds(30));
  }

  auto call_count_after_destruction = call_count.load();
  auto temp_count_after_destruction = temp.load();

  std::this_thread::sleep_for(std::chrono::milliseconds(std::chrono::milliseconds(50)));

  EXPECT_EQ(call_count.load(), call_count_after_destruction);
  EXPECT_EQ(temp.load(), temp_count_after_destruction);

  SUCCEED();
}

TEST_F(PausableLoopThreadTest, SetLoopSleepTimeChangesInterval) {
  std::vector<std::chrono::steady_clock::time_point> timestamps;

  PausableLoopThread loop(std::chrono::milliseconds(50), [&](size_t) {
    timestamps.push_back(std::chrono::steady_clock::now());
  });

  std::this_thread::sleep_for(std::chrono::milliseconds(120));
  // the internal timer of the Thread is 50ms, here we expect that we have at
  // least timestamps since the test waits for 120ms.
  auto initial_timestamp_count = timestamps.size();
  EXPECT_GE(initial_timestamp_count, 2);
  loop.set_loop_sleep_time(std::chrono::milliseconds(10));
  // even after 10ms, we dont expect the timestamp count to change
  // because one cycle is 50ms
  EXPECT_EQ(initial_timestamp_count, timestamps.size());
  std::this_thread::sleep_for(std::chrono::milliseconds(50));

  // Should now be firing much more rapidly
  EXPECT_GT(timestamps.size(), 3);
}

}  // namespace hyrise
