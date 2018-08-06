#include "pausable_loop_thread.hpp"

#include <chrono>
#include <condition_variable>
#include <functional>
#include <mutex>
#include <thread>

namespace opossum {

PausableLoopThread::PausableLoopThread(std::chrono::milliseconds loop_sleep_time,
                                       const std::function<void(size_t)>& loop_func)
    : _loop_sleep_time(loop_sleep_time) {
  _loop_thread = std::thread([&, loop_func] {
    size_t counter = 0;
    while (!_shutdown_flag) {
      std::unique_lock<std::mutex> lk(_mutex);
      if (_loop_sleep_time > std::chrono::milliseconds(0)) {
        _cv.wait_for(lk, _loop_sleep_time, [&] { return static_cast<bool>(_shutdown_flag); });
      }
      if (_shutdown_flag) return;
      while (_pause_requested) {
        _is_paused = true;
        _cv.wait(lk, [&] { return !_pause_requested || _shutdown_flag; });
        if (_shutdown_flag) return;
        lk.unlock();
      }
      _is_paused = false;
      loop_func(counter++);
    }
  });
}

PausableLoopThread::~PausableLoopThread() {
  _pause_requested = true;
  _shutdown_flag = true;
  _cv.notify_one();
  if (_loop_thread.joinable()) _loop_thread.join();
}

void PausableLoopThread::pause() {
  if (!_loop_thread.joinable()) return;
  _pause_requested = true;
  while (!_is_paused) {
  }
}

void PausableLoopThread::resume() {
  _pause_requested = false;
  _cv.notify_one();
}

void PausableLoopThread::set_loop_sleep_time(std::chrono::milliseconds loop_sleep_time) {
  _loop_sleep_time = loop_sleep_time;
}

}  // namespace opossum
