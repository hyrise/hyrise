#include <atomic>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <mutex>
#include <thread>

namespace opossum {

// This class spawns a thread that executes a procedure in a loop.
// Between each iteration there is a user-definable sleep period.
// The loop can be paused, resumed and finished. The loop starts in
// paused state.
struct PausableLoopThread {
 public:
  explicit PausableLoopThread(std::chrono::milliseconds loop_sleep_time, const std::function<void(size_t)>& loop_func);

  ~PausableLoopThread();
  void pause();
  void resume();
  void set_loop_sleep_time(std::chrono::milliseconds loop_sleep_time);

 private:
  std::atomic_bool _pause_requested{false};
  std::atomic_bool _is_paused{false};
  std::atomic_bool _shutdown_flag{false};
  std::mutex _mutex;
  std::condition_variable _cv;
  std::thread _loop_thread;
  std::chrono::milliseconds _loop_sleep_time;
};
}  // namespace opossum
