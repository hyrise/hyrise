#pragma once

#include <atomic>
#include <memory>
#include <thread>
#include <vector>

#include "scheduler/abstract_task.hpp"
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

  // Try to execute task immediately after this worker finishes the execution of the current task. Useful for making
  // sure that a task's successor is executed without having to go through the queue again (at which point all data
  // would have been removed from the caches). Adds task to the queue if called multiple times without having had the
  // chance to execute the tasks in between.
  void execute_next(const std::shared_ptr<AbstractTask>& task);

  uint64_t num_finished_tasks() const;

  void operator=(const Worker&) = delete;
  void operator=(Worker&&) = delete;

 protected:
  void operator()();
  void _work();

  void _wait_for_tasks(const std::vector<std::shared_ptr<AbstractTask>>& tasks);

 private:
  /**
   * Pin a worker to a particular core.
   * This does not work on non-NUMA systems, and might be addressed in the future.
   */
  void _set_affinity();

  std::shared_ptr<AbstractTask> _next_task{};
  std::shared_ptr<TaskQueue> _queue;
  WorkerID _id;
  CpuID _cpu_id;
  std::thread _thread;
  std::atomic<uint64_t> _num_finished_tasks{0};
};

}  // namespace opossum
