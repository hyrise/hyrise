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
  PausableLoopThread loop(std::chrono::milliseconds(10), [&](size_t) {
    ++call_count;
  });

  std::this_thread::sleep_for(std::chrono::milliseconds(30));

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
  auto timestamps = std::vector<std::chrono::steady_clock::time_point>();
  std::mutex timestamps_mutex;

  PausableLoopThread loop(std::chrono::milliseconds(50), [&](size_t) {
    std::lock_guard lock(timestamps_mutex);
    timestamps.push_back(std::chrono::steady_clock::now());
  });

  std::this_thread::sleep_for(std::chrono::milliseconds(125));

  loop.pause();

  size_t initial_timestamp_count;
  {
    std::lock_guard lock(timestamps_mutex);
    initial_timestamp_count = timestamps.size();
  }

  EXPECT_GE(initial_timestamp_count, 2);

  loop.set_loop_sleep_time(std::chrono::milliseconds(10));
  {
    std::lock_guard lock(timestamps_mutex);
    EXPECT_EQ(initial_timestamp_count, timestamps.size());
  }

  loop.resume();

  std::this_thread::sleep_for(std::chrono::milliseconds(55));
  {
    std::lock_guard lock(timestamps_mutex);
    // We expect at least 4-5 new timestamps in this period.
    // Check for more than initial + 3 to be safe.
    EXPECT_GT(timestamps.size(), initial_timestamp_count + 3);
  }
}

}  // namespace hyrise
