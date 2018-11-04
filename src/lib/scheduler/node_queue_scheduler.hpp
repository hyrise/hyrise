#pragma once

#include <atomic>
#include <memory>
#include <thread>
#include <vector>

#include "abstract_scheduler.hpp"

namespace opossum {

/*
 * GENERAL SCHEDULING CONCEPT
 *
 * Everything that needs to be processed is encapsulated in tasks. For example, in the context of the database
 * the OperatorTasks encapsulates database operators (here, it only encapsulates the execute function).
 * A task will then be pushed by a Scheduler into a TaskQueue and pulled out by a Worker to be processed.
 *
 *
 * TASK DEPENDENCIES
 *
 * Tasks can be dependent of each other. For example, in the context of the database, a table scan operation can be
 * dependent on a GetTable operation and so do the tasks that encapsulates these operations.
 * Therefore, not every task that is on top of the TaskQueue is ready to be processed. Pulling this unready task and
 * pushing it back to the queue could result in high latency for this task and in the context of databases in a
 * high latency for queries. Not pulling this unready task and waiting until it is ready would block the queue.
 * To avoid these situations, a TaskQueue is implemented as vector and will be iterated until a ready task is found.
 * This preserves the desired order and will not block the queue.
 * To determine if a task is ready, it checks its parent tasks if they are done. A task will be set done after it
 * was processed successfully.
 *
 *
 * JOBTASKS
 *
 * JobTasks can be used from anywhere to parallelize parts of their work.
 * If a task spawns jobs to be executed, the worker executing the main task waits for the jobs to complete.
 * Since the CPU to which the worker is pinned shall not be blocked, a new worker is (re-)activated temporarily.
 * The worker executing the main task is hibernated and will be re-activated once the jobs have completed.
 * There is only one active worker per CPU, which is allowed to pull new tasks from the queue.
 * Thus, while it is possible that two workers are executing a task at the same time, this is only until the non-active
 * worker finished its task. It will then be hibernated.
 *
 *
 * SCHEDULER AND TOPOLOGY
 *
 * The Scheduler is the main entry point and (currently) there is only one Scheduler.
 * For setting up a Scheduler a topology is used. A topology encapsulates the machine's architecture, e.g. number
 * of CPUs and the number of nodes, where a node is a cluster of CPUs.
 * In general, each node owns a TaskQueue. Furthermore, one Worker is assigned to one CPU. Therefore, the Worker
 * running on CPUs of one node are just pulling from the single TaskQueue of this node.
 *
 * A topology can also be created with Topology::use_fake_numa_topology() to simulate a NUMA system
 * with multiple nodes (queues) and worker and should mainly be used for testing NUMA-concepts
 * on non-NUMA development machines.
 *
 *
 * WORK STEALING
 *
 * Currently, a simple work stealing is implemented. Work stealing is useful to avoid idle workers (and therefore
 * idle CPUs) while there are still tasks in the system that need to be processed. A worker gets idle if it can not
 * pull a ready task. This occurs in two cases:
 *  1) all tasks in the queue are not ready
 *  2) the queue is empty
 * In both cases the current worker is checking another queue for a ready task. Checking another queue means accessing
 * another node (remote node). As of the physical distance of nodes, accessing a remote nodes is ~1.6 times slower than
 * accessing a local node. [1]
 * Therefore, the current worker fetches a task from a remote queue (without removing it). Then, the current worker
 * is sleeping some milliseconds to give any local worker of the remote node the chance to pull that task. If no local
 * worker of the remote node pulled the task, the current worker is pulling the task and therefore steals it.
 * Afterwards, the current worker is checking its local queue gain.
 *
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
  ~NodeQueueScheduler();

  /**
   * Create a queue on every node and a processing unit for every core.
   * Start a single worker for each processing unit.
   */
  void begin() override;

  void finish() override;

  bool active() const override;

  const std::vector<std::shared_ptr<TaskQueue>>& queues() const override;

  /**
   * @param task
   * @param preferred_node_id The Task will be initially added to this node, but might get stolen by other Nodes later
   * @param priority Determines whether tasks are inserted at the beginning or end of the queue.
   */
  void schedule(std::shared_ptr<AbstractTask> task, NodeID preferred_node_id = CURRENT_NODE_ID,
                SchedulePriority priority = SchedulePriority::Default) override;

 private:
  std::atomic<TaskID> _task_counter{TaskID{0}};
  std::shared_ptr<UidAllocator> _worker_id_allocator;
  std::vector<std::shared_ptr<TaskQueue>> _queues;
  std::vector<std::shared_ptr<Worker>> _workers;
  std::atomic_bool _active{false};
};

}  // namespace opossum
