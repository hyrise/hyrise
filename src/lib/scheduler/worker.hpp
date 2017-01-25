#pragma once

#include <atomic>
#include <memory>

#include "task_queue.hpp"
#include "types.hpp"

namespace opossum {

class AbstractScheduler;

/**
 * To be executed on a separate Thread, fetches and executes tasks until the queue is empty AND the shutdown flag is set
 * Usually there should be one active Worker per CPU.
 */
class Worker {
 public:
  Worker(AbstractScheduler& scheduler, std::shared_ptr<TaskQueue> queue, WorkerID id, CpuID cpu_id);
  Worker(const Worker& rhs) = delete;

  /**
   * Unique ID of a worker. Currently not in use, but really helpful for debugging.
   */
  WorkerID id() const;
  CpuID cpu_id() const;

  void set_shutdown_flag(bool flag);

  void operator=(const Worker& rhs) = delete;
  void operator()();

 private:
  /**
   * Pin a worker to a particular core.
   * This does not work on non-NUMA systems, and might be addressed in the future.
   */
  void _set_affinity();

  AbstractScheduler& _scheduler;
  std::shared_ptr<TaskQueue> _queue;
  WorkerID _id;
  CpuID _cpu_id;
  std::atomic_bool _shutdown_flag;
};

}  // namespace opossum
