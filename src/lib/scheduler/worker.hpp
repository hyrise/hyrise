#pragma once

#include <memory>
#include <vector>

#include "processing_unit.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

class TaskQueue;

/**
 * To be executed on a separate Thread, fetches and executes tasks until the queue is empty AND the shutdown flag is set
 * Ideally there should be one Worker actively doing work per CPU, but multiple might be active occasionally
 */
class Worker : public std::enable_shared_from_this<Worker>, private Noncopyable {
  friend class AbstractTask;
  friend class CurrentScheduler;
  friend class NodeQueueScheduler;

 public:
  static std::shared_ptr<Worker> get_this_thread_worker();

  Worker(const std::weak_ptr<ProcessingUnit>& processing_unit, const std::shared_ptr<TaskQueue>& queue, WorkerID id,
         CpuID cpu_id, SchedulePriority min_priority = SchedulePriority::Lowest);

  /**
   * Unique ID of a worker. Currently not in use, but really helpful for debugging.
   */
  WorkerID id() const;
  std::shared_ptr<TaskQueue> queue() const;
  std::weak_ptr<ProcessingUnit> processing_unit() const;
  CpuID cpu_id() const;

  void operator()();

  void operator=(const Worker&) = delete;
  void operator=(Worker&&) = delete;

 protected:
  template <typename TaskType>
  void _wait_for_tasks(const std::vector<std::shared_ptr<TaskType>>& tasks) {
    /**
     * This method blocks the calling thread (worker) until all tasks have been completed.
     * It hands off the active worker token so that another worker can execute tasks while the calling worker is blocked.
     */
    auto processing_unit = _processing_unit.lock();
    DebugAssert(static_cast<bool>(processing_unit), "Bug: Locking the processing unit failed");

    processing_unit->yield_active_worker_token(_id);
    processing_unit->wake_or_create_worker();

    for (auto& task : tasks) {
      task->_join_without_replacement_worker();
    }
  }

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
  SchedulePriority _min_priority;
};

}  // namespace opossum
