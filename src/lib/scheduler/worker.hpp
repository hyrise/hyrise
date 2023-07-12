#pragma once

#include <atomic>
#include <memory>
#include <thread>
#include <vector>

#include "scheduler/abstract_task.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace hyrise {

class TaskQueue;

/**
 * To be executed on a separate thread, fetches and executes tasks until the queue is empty.
 */
class Worker : public std::enable_shared_from_this<Worker>, private Noncopyable {
  friend class AbstractScheduler;

 public:
  static std::shared_ptr<Worker> get_this_thread_worker();

  Worker(const std::shared_ptr<TaskQueue>& queue, WorkerID worker_id, CpuID cpu_id);

  /**
   * Unique ID of a worker. Currently not in use, but really helpful for debugging.
   */
  WorkerID id() const;
  std::shared_ptr<TaskQueue> queue() const;
  CpuID cpu_id() const;

  void start();
  void join();

  // Try to execute task immediately after this worker finishes the execution of the current task. The goal is to
  // execute the task while the caches are still fresh instead of having to wait for it to be scheduled again. A task
  // can have multiple successors and all of them could become executable at the same time. In that case, the current
  // worker can only execute one of them immediately. The others are placed into a high priority queue on the same node
  // so that they are worked on as soon as possible by either this or another worker.
  void execute_next(const std::shared_ptr<AbstractTask>& task);

  // Returns the number of tasks the worker has processed. This method is used as part of the scheduler shutdown. Be
  // cautious when using this method in any other context (see comments in #2526).
  uint64_t num_finished_tasks() const;

  void operator=(const Worker&) = delete;
  void operator=(Worker&&) = delete;

 protected:
  enum class AllowSleep : bool { Yes = true, No = false };

  void operator()();

  void _work(const AllowSleep allow_sleep);

  void _wait_for_tasks(const std::vector<std::shared_ptr<AbstractTask>>& tasks);

 private:
  /**
   * Pin a worker to a particular core.
   * This does not work on non-NUMA systems, and might be addressed in the future.
   */
  void _set_affinity();

  std::shared_ptr<AbstractTask> _next_task{};
  std::shared_ptr<TaskQueue> _queue{};
  WorkerID _id{0};
  CpuID _cpu_id{0};
  std::thread _thread;
  std::atomic_uint64_t _num_finished_tasks{0};

  bool _active{true};

  std::vector<int> _random{};
  size_t _next_random{0};
};

}  // namespace hyrise
