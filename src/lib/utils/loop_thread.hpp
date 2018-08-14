#include <atomic>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <mutex>
#include <thread>

namespace opossum {

// This class spawns a thread that executes a procedure in a loop.
// Between each iteration there is a user-definable sleep period.
// The loop is started on instantiation.
struct LoopThread {
 public:
  explicit LoopThread(std::chrono::milliseconds loop_sleep_time, std::function<void()> loop_func);

  ~LoopThread();
  void set_loop_sleep_time(std::chrono::milliseconds loop_sleep_time);

 private:
  std::atomic_bool _shutdown_flag{false};
  std::mutex _mutex;
  std::condition_variable _cv;
  std::thread _loop_thread;
  std::chrono::milliseconds _loop_sleep_time;
};
}  // namespace opossum
