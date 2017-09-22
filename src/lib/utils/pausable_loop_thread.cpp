#include <chrono>
#include <condition_variable>
#include <functional>
#include <mutex>
#include <thread>

#include "pausable_loop_thread.hpp"

namespace opossum {

PausableLoopThread::PausableLoopThread(std::chrono::milliseconds loop_sleep, std::function<void(size_t)> loop_func) {
  loop_thread = std::thread([&, loop_sleep, loop_func] {
    size_t counter = 0;
    while (!shutdownFlag) {
      if (loop_sleep > std::chrono::milliseconds(0)) {
        std::this_thread::sleep_for(loop_sleep);
      }
      if (shutdownFlag) return;
      while (isPaused) {
        std::unique_lock<std::mutex> lk(m);
        cv.wait(lk, [&] { return !isPaused || shutdownFlag; });
        if (shutdownFlag) return;
        lk.unlock();
      }
      loop_func(counter++);
    }
  });
}

void PausableLoopThread::pause() {
  std::lock_guard<std::mutex> lk(m);
  isPaused = true;
}

void PausableLoopThread::resume() {
  {
    std::lock_guard<std::mutex> lk(m);
    isPaused = false;
  }
  cv.notify_one();
}

void PausableLoopThread::finish() {
  {
    std::lock_guard<std::mutex> lk(m);
    isPaused = true;
    shutdownFlag = true;
  }
  cv.notify_one();
  loop_thread.join();
}
}  // namespace opossum
