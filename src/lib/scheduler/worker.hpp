#pragma once

#include <memory>
#include <vector>

#include "task_queue.hpp"
#include "types.hpp"

namespace opossum {

class ProcessingUnit;

/**
 * To be executed on a separate Thread, fetches and executes tasks until the queue is empty AND the shutdown flag is set
 * Ideally there should be one Worker actively doing work per CPU, but multiple might be active occasionally
 */
class Worker : public std::enable_shared_from_this<Worker> {
  friend class AbstractTask;
  friend class CurrentScheduler;
  friend class NodeQueueScheduler;

 public:
  static std::shared_ptr<Worker> get_this_thread_worker();

  Worker(std::weak_ptr<ProcessingUnit> processing_unit, std::shared_ptr<TaskQueue> queue, WorkerID id, CpuID cpu_id);
  Worker(const Worker& rhs) = delete;
  Worker(Worker&& rhs) = delete;

  /**
   * Unique ID of a worker. Currently not in use, but really helpful for debugging.
   */
  WorkerID id() const;
  std::shared_ptr<TaskQueue> queue() const;
  std::weak_ptr<ProcessingUnit> processing_unit() const;
  CpuID cpu_id() const;

  void operator()();

  void operator=(const Worker& rhs) = delete;
  void operator=(Worker&& rhs) = delete;

 protected:
  void _wait_for_tasks(const alloc_vector<std::shared_ptr<AbstractTask>>& task);

 private:
  /**
   * Pin a worker to a particular core.
   * This does not work on non-NUMA systems, and might be addressed in the future.
   */
  void _set_affinity();

  std::weak_ptr<ProcessingUnit> _processing_unit;
  std::shared_ptr<TaskQueue> _queue;
  WorkerID _id;
  CpuID _cpu_id;
};

}  // namespace opossum
