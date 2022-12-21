#include "node_queue_scheduler.hpp"
#include <cstdlib>
#include <iostream>
#include <memory>
#include <utility>
#include <vector>

#include "abstract_task.hpp"
#include "hyrise.hpp"
#include "task_queue.hpp"
#include "worker.hpp"

#include "uid_allocator.hpp"
#include "utils/assert.hpp"


namespace {
  /**
   * For the grouping of tasks (see _num_groups()), we scale the number of groups according to the current load of the
   * system. In cases of a single user, we can use a high group count (i.e., allowing parallelism) as the main queue is
   * usually not congested. In case of multiple clients, we lower the number of groups to limit concurrently processed
   * tasks (see discussion in #2243).
   *
   * We scale number of groups linearly between (NUM_GROUPS_MIN_FACTOR * _workers_per_node) and (NUM_GROUPS_MAX_FACTOR *
   * _workers_per_node).
   */
  constexpr auto NUM_GROUPS_MIN_FACTOR = 0.1f;
  constexpr auto NUM_GROUPS_MAX_FACTOR = 4.0f;
  constexpr auto NUM_GROUPS_RANGE = NUM_GROUPS_MAX_FACTOR - NUM_GROUPS_MIN_FACTOR;

  // This factor is used to determine at which queue load we use the maximum number of groups. Arbitrarily high queue
  // loads should not lead to an arbitrary number of groups, which hampers scheduler progress.
  constexpr auto UPPER_LIMIT_QUEUE_SIZE_FACTOR = size_t{6};
}

namespace hyrise {

NodeQueueScheduler::NodeQueueScheduler() {
  _worker_id_allocator = std::make_shared<UidAllocator>();
}

NodeQueueScheduler::~NodeQueueScheduler() {
  if (HYRISE_DEBUG && _active) {
    // We cannot throw an exception because destructors are noexcept by default.
    std::cerr << "NodeQueueScheduler::finish() wasn't called prior to destroying it" << std::endl;
    std::exit(EXIT_FAILURE);  // NOLINT(concurrency-mt-unsafe)
  }
}

void NodeQueueScheduler::begin() {
  DebugAssert(!_active, "Scheduler is already active");

  _queue_count = Hyrise::get().topology.nodes().size();
  _queues.reserve(_queue_count);

  _worker_count = Hyrise::get().topology.num_cpus();
  _workers.reserve(_workers_count);
  _workers_per_node = _worker_count / _queue_count;

  // For tasks lists with less tasks, we do not dynically determine the number of groups to use.
  _min_tasks_count_for_regrouping = std::max(size_t{16}, _worker_count);

  // Everything above this limit yields the max value for grouping.
  _regrouping_upper_limit = _worker_count * UPPER_LIMIT_QUEUE_SIZE_FACTOR;

  for (auto node_id = NodeID{0}; node_id < Hyrise::get().topology.nodes().size(); node_id++) {
    auto queue = std::make_shared<TaskQueue>(node_id);

    _queues.emplace_back(queue);

    const auto& topology_node = Hyrise::get().topology.nodes()[node_id];

    for (const auto& topology_cpu : topology_node.cpus) {
      _workers.emplace_back(
          std::make_shared<Worker>(queue, WorkerID{_worker_id_allocator->allocate()}, topology_cpu.cpu_id));
    }
  }

  _workers_per_node = _workers.size() / _queue_count;
  _active = true;

  for (auto& worker : _workers) {
    worker->start();
  }
}

void NodeQueueScheduler::wait_for_all_tasks() {
  while (true) {
    uint64_t num_finished_tasks = 0;
    for (auto& worker : _workers) {
      num_finished_tasks += worker->num_finished_tasks();
    }

    if (num_finished_tasks == _task_counter) {
      break;
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
}

void NodeQueueScheduler::finish() {
  wait_for_all_tasks();

  // All queues SHOULD be empty by now
  if (HYRISE_DEBUG) {
    for (auto& queue : _queues) {
      Assert(queue->empty(), "NodeQueueScheduler bug: Queue wasn't empty even though all tasks finished");
    }
  }

  _active = false;

  for (auto& worker : _workers) {
    worker->join();
  }

  _workers = {};
  _queues = {};
  _task_counter = 0;
}

bool NodeQueueScheduler::active() const {
  return _active;
}

const std::vector<std::shared_ptr<TaskQueue>>& NodeQueueScheduler::queues() const {
  return _queues;
}

const std::vector<std::shared_ptr<Worker>>& NodeQueueScheduler::workers() const {
  return _workers;
}

void NodeQueueScheduler::schedule(std::shared_ptr<AbstractTask> task, NodeID preferred_node_id,
                                  SchedulePriority priority) {
  /**
   * Add task to the queue of the preferred node if it is ready for execution.
   */
  DebugAssert(_active, "Can't schedule more tasks after the NodeQueueScheduler was shut down");
  DebugAssert(task->is_scheduled(), "Don't call NodeQueueScheduler::schedule(), call schedule() on the task");

  const auto task_counter = _task_counter++;  // Atomically take snapshot of counter
  task->set_id(TaskID{task_counter});

  if (!task->is_ready()) {
    return;
  }

  const auto node_id_for_queue = determine_queue_id_for_task(task, preferred_node_id);
  DebugAssert((static_cast<size_t>(node_id_for_queue) < _queues.size()),
              "Node ID is not within range of available nodes.");
  _queues[node_id_for_queue]->push(task, priority);
}

NodeID NodeQueueScheduler::determine_queue_id_for_task(const std::shared_ptr<AbstractTask>& task,
                                                       const NodeID preferred_node_id) const {
  // Early out: no need to check for preferred node or other queues, if there is only a single node queue.
  if (_queue_count == 1) {
    return NodeID{0};
  }

  if (preferred_node_id != CURRENT_NODE_ID) {
    return preferred_node_id;
  }

  // If the current node is requested, try to obtain node from current worker.
  const auto& worker = Worker::get_this_thread_worker();
  if (worker) {
    return worker->queue()->node_id();
  }

  // Initial min values with Node 0.
  auto min_load_queue_id = NodeID{0};
  auto min_load = _queues[0]->estimate_load();

  // When the current load of node 0 is small, do not check other queues.
  if (min_load < _workers_per_node) {
    return NodeID{0};
  }

  for (auto queue_id = NodeID{1}; queue_id < _queue_count; ++queue_id) {
    const auto queue_load = _queues[queue_id]->estimate_load();
    if (queue_load < min_load) {
      min_load_queue_id = queue_id;
      min_load = queue_load;
    }
  }

  return min_load_queue_id;
}

size_t NodeQueueScheduler::determine_group_count(const std::vector<std::shared_ptr<AbstractTask>>& tasks) const {
  if (tasks.size() < _min_tasks_count_for_regrouping) {
    // For small task sets we use a group count equal to the number of workers.
    return _worker_count;
  }

  // We check the first task for a node assignment. We assume that the passed tasks are not assigned to different
  // nodes. If this assumption becomes invalid (e.g., due to work on NUMA optimizations), the current node should be
  // revisited.
  const auto node_id_for_queue_check = (tasks[0]->node_id() == CURRENT_NODE_ID || tasks[0]->node_id() == INVALID_NODE_ID) ? NodeID{0} : tasks[0]->node_id();
  const auto queue_load = _queues[node_id_for_queue_check]->estimate_load();

  const auto fill_level = std::min(queue_load, _regrouping_upper_limit);
  const auto fill_degree = 1.0f - (static_cast<float>(fill_level) / static_cast<float>(_regrouping_upper_limit));
  const auto fill_factor = NUM_GROUPS_MIN_FACTOR + (NUM_GROUPS_RANGE * fill_degree);

  return static_cast<size_t>(static_cast<float>(_worker_count) * fill_factor);
}


void NodeQueueScheduler::_group_tasks(const std::vector<std::shared_ptr<AbstractTask>>& tasks) const {
  // Adds predecessor/successor relationships between tasks so that only NUM_GROUPS tasks can be executed in parallel.
  // Approach: Skip all tasks that already have predecessors or successors, as adding relationships to these could
  // introduce cyclic dependencies.

  auto round_robin_counter = 0;
  auto common_node_id = std::optional<NodeID>{};

  const auto num_groups = determine_group_count(tasks);

  std::vector<std::shared_ptr<AbstractTask>> grouped_tasks(num_groups);
  for (const auto& task : tasks) {
    if (!task->predecessors().empty() || !task->successors().empty()) {
      return;
    }

    if (common_node_id) {
      // This is not really a hard assertion. As the chain will likely be executed on the same Worker (see
      // Worker::execute_next), we would ignore all but the first node_id. At the time of writing, we did not do any
      // smart node assignment. This assertion is only here so that this behavior is understood if we ever assign NUMA
      // node ids.
      DebugAssert(task->node_id() == *common_node_id, "Expected all grouped tasks to have the same node_id");
    } else {
      common_node_id = task->node_id();
    }

    const auto group_id = round_robin_counter % num_groups;
    const auto& first_task_in_group = grouped_tasks[group_id];
    if (first_task_in_group) {
      task->set_as_predecessor_of(first_task_in_group);
    }
    grouped_tasks[group_id] = task;
    ++round_robin_counter;
  }
}

}  // namespace hyrise
