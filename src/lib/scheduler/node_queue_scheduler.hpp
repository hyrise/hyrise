#pragma once

#include <atomic>
#include <memory>
#include <thread>
#include <vector>

#include "abstract_scheduler.hpp"

namespace hyrise {

/**
 *
 * SCHEDULER AND TOPOLOGY
 *
 * For setting up the NodeQueueScheduler, the server's topology is used. A topology encapsulates the
 * machine's architecture, e.g., the number of CPU threads and NUMA nodes, where a node is typically a socket or CPU
 * (usually having multiple threads/cores).
 * Each node owns a TaskQueue. Furthermore, one worker is assigned to one CPU thread. A worker running on one CPU
 * thread of a node is primarily pulling from the local TaskQueue of this node.
 *
 * A topology can also be created with Hyrise::get().topology.use_fake_numa_topology() to simulate a NUMA system with
 * multiple nodes (thus, TaskQueues) and workers and should mainly be used for testing NUMA concepts on non-NUMA
 * development machines.
 *
 *
 * WORK STEALING
 *
 * Currently, a simple work stealing is implemented. Work stealing is useful to avoid idle workers (and therefore idle
 * CPU threads) while there are still tasks in the system that need to be processed. A worker gets idle when its local
 * TaskQueue is empty. In this case, the worker is checking non-local TaskQueues of other NUMA nodes for tasks. The
 * worker pulls a task from a remote TaskQueue and checks if this task is stealable. If not, the task is pushed to the
 * TaskQueue again.
 * In case no tasks can be processed, the worker thread is put to sleep and waits on the semaphore of its node-local
 * TaskQueue.
 *
 * Note: currently, TaskQueues are not explicitly allocated on a NUMA node. This means most workers will frequently
 * access distant TaskQueues, which is ~1.6 times slower than accessing a local node [1]. 
 *
 *  [1] http://frankdenneman.nl/2016/07/13/numa-deep-dive-4-local-memory-optimization/
 *
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
   * Create a TaskQueue on every node and a worker for every core.
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
  std::vector<NodeID> _active_nodes;

  std::atomic_bool _active{false};
  std::atomic_int64_t _active_worker_count{0};

  size_t _node_count{1};
  std::vector<size_t> _workers_per_node;

  std::mutex _finish_mutex{};
};

}  // namespace hyrise
