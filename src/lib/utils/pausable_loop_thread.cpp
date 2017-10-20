#include "pausable_loop_thread.hpp"

#include <chrono>
#include <condition_variable>
#include <functional>
#include <mutex>
#include <thread>

namespace opossum {

PausableLoopThread::PausableLoopThread(std::chrono::milliseconds loop_sleep, std::function<void(size_t)> loop_func) {
  _loop_thread = std::thread([&, loop_sleep, loop_func] {
    size_t counter = 0;
    while (!_shutdown_flag) {
      if (loop_sleep > std::chrono::milliseconds(0)) {
        std::this_thread::sleep_for(loop_sleep);
      }
      if (_shutdown_flag) return;
      while (_is_paused) {
        std::unique_lock<std::mutex> lk(_mutex);
        _cv.wait(lk, [&] { return !_is_paused || _shutdown_flag; });
        if (_shutdown_flag) return;
        lk.unlock();
      }
      loop_func(counter++);
    }
  });
}

void PausableLoopThread::pause() { _is_paused = true; }

void PausableLoopThread::resume() {
  _is_paused = false;
  _cv.notify_one();
}

void PausableLoopThread::finish() {
  _is_paused = true;
  _shutdown_flag = true;
  _cv.notify_one();
  _loop_thread.join();
}

}  // namespace opossum
