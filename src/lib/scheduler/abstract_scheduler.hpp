#pragma once

#include <memory>
#include <vector>

#include "scheduler/abstract_task.hpp"
#include "types.hpp"
#include "utils/assert.hpp"
#include "worker.hpp"

namespace hyrise {

class TaskQueue;

/**
 *
 * GENERAL TASK PROCESSING AND SCHEDULING CONCEPT
 *
 * Everything that needs to be processed is encapsulated in tasks. A task will be pushed into a TaskQueue by a
 * scheduler and pulled by a worker to be processed.
 *
 * There are currently two alternative scheduler implementations: the ImmediateExecutionScheduler (single-threaded,
 * primarily for benchmarking and debugging) and the NodeQueueScheduler (multi-threaded).
 *
 *
 * TASK DEPENDENCIES
 *
 * Tasks can be dependent on each other. For example, a TableScan operation can depend on a GetTable operation, and so
 * do the tasks that encapsulate these operations. Tasks with predecessors are not scheduled (i.e., not added to the
 * TaskQueues) to avoid workers from pulling tasks that cannot (yet) be processed. Instead, succeeding tasks are
 * executed as soon as the preceding task(s) have been processed (workers try to process all successors before pulling
 * new tasks from the TaskQueues).
 * Considering the exemplary query TPC-H 6. Here, only a single GetTable operator and none of the other operators would
 * be scheduled. After an operator has been processed, the operators consuming its output are directly executed. For
 * more complex queries with multiple GetTable operators, we would schedule multiple operators concurrently.
 *
 *
 * TASKS
 *
 * The two main task types in Hyrise are OperatorTasks and JobTasks. OperatorTasks encapsulate database operators
 * (here, they only encapsulate the execute() function). JobTasks can be used by any component to parallelize arbitrary
 * parts of its work by taking a void-returning lambda. If a task itself spawns tasks to be executed, the worker
 * executing the main task executes these tasks directly when possible or waits for their completion in case other
 * workers already process these tasks (during this wait time, the worker pulls tasks from the TaskQueues to avoid
 * idling).
 *
 */

class AbstractScheduler : public Noncopyable {
 public:
  virtual ~AbstractScheduler() = default;

  /**
   * Begin the schedulers lifecycle as the global Scheduler instance. In this method do work that can't be done before
   * the Scheduler isn't registered as the global instance
   */
  virtual void begin() = 0;

  virtual void wait_for_all_tasks() = 0;

  /**
   * Ends the schedulers lifecycle as the global Scheduler instance. This waits for all scheduled tasks to be finished,
   * and sets the scheduler to inactive.
   *
   * The caller of this method has to make sure that no other tasks can be scheduled from outside while this method
   * is being executed, otherwise those tasks might get lost.
   */
  virtual void finish() = 0;

  virtual bool active() const = 0;

  virtual const std::vector<std::shared_ptr<TaskQueue>>& queues() const = 0;

  // Returns a vector containing indices for all tasks queues, prioritized by their actual node's NUMA distance to
  // the given node_id.
  virtual const std::vector<NodeID>& prioritized_queue_ids(NodeID node_id) const = 0;

  virtual void schedule(std::shared_ptr<AbstractTask> task, SchedulePriority priority = SchedulePriority::Default) = 0;

  // Schedules the given tasks for execution and returns immediately.
  // If no asynchronicity is needed, prefer schedule_and_wait_for_tasks.
  static void schedule_tasks(const std::vector<std::shared_ptr<AbstractTask>>& tasks);

  // Blocks until all specified tasks are completed.
  // If no asynchronicity is needed, prefer schedule_and_wait_for_tasks.
  static void wait_for_tasks(const std::vector<std::shared_ptr<AbstractTask>>& tasks);

  // Schedules the given tasks for execution and waits for them to complete before returning. Tasks may be reorganized
  // internally, e.g., to reduce the number of tasks being executed in parallel. See the implementation of
  // NodeQueueScheduler::_group_tasks for an example.
  void schedule_and_wait_for_tasks(const std::vector<std::shared_ptr<AbstractTask>>& tasks);

 protected:
  // Internal helper method that adds predecessor/successor relationships between tasks to limit the degree of
  // parallelism and reduce scheduling overhead.
  virtual void _group_tasks(const std::vector<std::shared_ptr<AbstractTask>>& tasks) const;
};

}  // namespace hyrise
