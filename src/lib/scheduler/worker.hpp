#pragma once

#include <atomic>
#include <memory>
#include <thread>
#include <vector>

#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

class TaskQueue;

/**
 * To be executed on a separate Thread, fetches and executes tasks until the queue is empty AND the shutdown flag is set
 * Ideally there should be one Worker actively doing work per CPU, but multiple might be active occasionally
 */
class Worker : public std::enable_shared_from_this<Worker>, private Noncopyable {
  friend class AbstractScheduler;

 public:
  static std::shared_ptr<Worker> get_this_thread_worker();

  Worker(const std::shared_ptr<TaskQueue>& queue, WorkerID id, CpuID cpu_id);

  /**
   * Unique ID of a worker. Currently not in use, but really helpful for debugging.
   */
  WorkerID id() const;
  std::shared_ptr<TaskQueue> queue() const;
  CpuID cpu_id() const;

  void start();
  void join();

  uint64_t num_finished_tasks() const;

  void operator=(const Worker&) = delete;
  void operator=(Worker&&) = delete;

 protected:
  void operator()();
  void _work();

  template <typename TaskType>
  void _wait_for_tasks(const std::vector<std::shared_ptr<TaskType>>& tasks) {
    auto tasks_completed = [&tasks]() {
      // Reversely iterate through the list of tasks, because unfinished tasks are likely at the end of the list.
      for (auto it = tasks.rbegin(); it != tasks.rend(); ++it) {
        if (!(*it)->is_done()) {
          return false;
        }
      }
      return true;
    };

    while (!tasks_completed()) {
      _work();
    }
  }

 private:
  /**
   * Pin a worker to a particular core.
   * This does not work on non-NUMA systems, and might be addressed in the future.
   */
  void _set_affinity();

  std::shared_ptr<TaskQueue> _queue;
  WorkerID _id;
  CpuID _cpu_id;
  std::thread _thread;
  std::atomic<uint64_t> _num_finished_tasks{0};
};

}  // namespace opossum
