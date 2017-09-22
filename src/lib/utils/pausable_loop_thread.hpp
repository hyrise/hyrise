#include <chrono>
#include <condition_variable>
#include <functional>
#include <mutex>
#include <thread>

namespace opossum {

struct PausableLoopThread {
 public:
  explicit PausableLoopThread(std::chrono::milliseconds loop_sleep, std::function<void(size_t)> loop_func);

  ~PausableLoopThread() { finish(); }
  void pause();
  void resume();
  void finish();

 private:
  bool isPaused = true;
  bool shutdownFlag = false;
  std::mutex m;
  std::condition_variable cv;
  std::thread loop_thread;
};
}  // namespace opossum
