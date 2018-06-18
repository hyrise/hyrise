#include "loop_thread.hpp"

#include <chrono>
#include <condition_variable>
#include <functional>
#include <mutex>
#include <thread>

namespace opossum {

LoopThread::LoopThread(std::chrono::milliseconds loop_sleep_time, std::function<void()> loop_func)
    : _loop_sleep_time(loop_sleep_time) {
  _loop_thread = std::thread([&, loop_func] {
    while (!_shutdown_flag) {
      std::unique_lock<std::mutex> lk(_mutex);
      if (_loop_sleep_time > std::chrono::milliseconds(0)) {
        _cv.wait_for(lk, _loop_sleep_time, [&] { return static_cast<bool>(_shutdown_flag); });
      }
      if (_shutdown_flag) return;
      loop_func();
    }
  });
}

LoopThread::~LoopThread() {
  _shutdown_flag = true;
  _cv.notify_one();
  if (_loop_thread.joinable()) _loop_thread.join();
}

void LoopThread::set_loop_sleep_time(std::chrono::milliseconds loop_sleep_time) {
  _loop_sleep_time = loop_sleep_time;
}

}  // namespace opossum
