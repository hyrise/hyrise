#pragma once

#include <atomic>
#include <memory>
#include <thread>
#include <vector>

#include "abstract_scheduler.hpp"

namespace hyrise {

/*
 * GENERAL SCHEDULING CONCEPT
 *
 * Everything that needs to be processed is encapsulated in tasks. For example, in the context of the database the
 * OperatorTasks encapsulates database operators (here, it only encapsulates the execute function). A task will be
 * pushed by a Scheduler into a TaskQueue and pulled out by a Worker to be processed.
 *
 *
 * TASK DEPENDENCIES
 *
 * Tasks can be dependent of each other. For example, in the context of the database, a table scan operation can be
 * dependent on a GetTable operation and so do the tasks that encapsulates these operations.
 * Therefore, not every task that is on top of the TaskQueue is ready to be processed. Pulling this unready task and
 * pushing it back to the queue could result in high latency for this task and in the context of databases in a high
 * latency for queries. For that reason, Workers working on tasks with dependencies try to process these first.
 *
 *
 * JOBTASKS
 *
 * JobTasks can be used from anywhere to parallelize parts of their work. If a task spawns jobs to be executed, the
 * worker executing the main task executes these jobs when possible or waits for their completion in case other workers
 * process these tasks (during this wait time, the worker pulls tasks from the queue to avoid idling).
 *
 *
 * SCHEDULER AND TOPOLOGY
 *
 * The Scheduler is the main entry point and (currently) there are only the ImmediateExecutionScheduler
 * (single-threaded) and the NodeQueueScheduler (multi-threaded). For setting up the NodeQueueScheduler a topology is
 * used. A topology encapsulates the machine's architecture, e.g. number of CPUs and the number of nodes, where a node
 * is typically a socket or CPU (usually having multiple threads/cores).
 * In general, each node owns a TaskQueue. Furthermore, one Worker is assigned to one CPU thread. The Worker running on
 * one CPU thread of a node is primarily pulling from the local TaskQueue of this node.
 *
 * A topology can also be created with Hyrise::get().topology.use_fake_numa_topology() to simulate a NUMA system with
 * multiple nodes (thus, queues) and worker and should mainly be used for testing NUMA-concepts on non-NUMA development
 * machines.
 *
 *
 * WORK STEALING
 *
 * Currently, a simple work stealing is implemented. Work stealing is useful to avoid idle workers (and therefore idle
 * CPU threads) while there are still tasks in the system that need to be processed. A worker gets idle if it can not
 * pull a ready task. This occurs in two cases:
 *  1) all tasks in the queue are not ready
 *  2) the queue is empty
 * In both cases, the current worker is checking non-local queues of other NUMA nodes for ready tasks. The Worker pulls
 * a task from a remote queue and checks if this task is stealable. If not, the task is pushed to the task queue again.
 *
 * Note: currently, task queues are not explicitly allocated on a NUMA node. This means most workers will frequently
 * access distant task queues, which is ~1.6 times slower than accessing a local node [1]. 
 * [1] http://frankdenneman.nl/2016/07/13/numa-deep-dive-4-local-memory-optimization/
 */

class Worker;
class TaskQueue;
class UidAllocator;

/**
 * Schedules Tasks
 */
class NodeQueueScheduler : public AbstractScheduler {
 public:
  NodeQueueScheduler();
  ~NodeQueueScheduler() override;

  /**
   * Create a queue on every node and a processing unit for every core.
   * Start a single worker for each processing unit.
   */
  void begin() override;

  void finish() override;

  bool active() const override;

  const std::vector<std::shared_ptr<TaskQueue>>& queues() const override;

  const std::vector<std::shared_ptr<Worker>>& workers() const;

  /**
   * @param task
   * @param preferred_node_id determines to which queue tasks are added. Note, the task might still be stolen by other nodes due
   *                          to task stealing in NUMA environments.
   * @param priority
   */
  void schedule(std::shared_ptr<AbstractTask> task, NodeID preferred_node_id = CURRENT_NODE_ID,
                SchedulePriority priority = SchedulePriority::Default) override;

  /**
   * @param preferred_node_id
   * @return `preferred_node_id` if a non-default preferred node ID is passed. When the node is the default of
   *         CURRENT_NODE_ID but no current node (where the task is executed) can be obtained, the node ID of the node
   *         with the lowest queue pressure is returned.
   */
  NodeID determine_queue_id(const NodeID preferred_node_id) const;

  void wait_for_all_tasks() override;

  const std::atomic_int64_t& active_worker_count() const;

  // Number of groups for _group_tasks
  static constexpr auto NUM_GROUPS = 10;

 protected:
  void _group_tasks(const std::vector<std::shared_ptr<AbstractTask>>& tasks) const override;

 private:
  std::atomic<TaskID::base_type> _task_counter{0};
  std::shared_ptr<UidAllocator> _worker_id_allocator;
  std::vector<std::shared_ptr<TaskQueue>> _queues;
  std::vector<std::shared_ptr<Worker>> _workers;

  std::atomic_bool _active{false};
  std::atomic_int64_t _active_worker_count{0};

  size_t _node_count{1};
  std::vector<size_t> _workers_per_node;

  std::mutex _finish_mutex{};
};

}  // namespace hyrise
