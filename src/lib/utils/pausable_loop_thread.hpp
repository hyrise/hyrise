#include <atomic>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <mutex>
#include <thread>

namespace opossum {

// This class spawns a thread that executes a procedure in a loop.
// Between each iteration there is a user-definable sleep period.
// The loop can be paused, resumed and finished.
struct PausableLoopThread {
 public:
  explicit PausableLoopThread(std::chrono::milliseconds loop_sleep, std::function<void(size_t)> loop_func);

  ~PausableLoopThread() { finish(); }
  void pause();
  void resume();
  void finish();

 private:
  std::atomic_bool _is_paused{true};
  std::atomic_bool _shutdown_flag{false};
  std::mutex _mutex;
  std::condition_variable _cv;
  std::thread _loop_thread;
};
}  // namespace opossum
