#include "node_queue_scheduler.hpp"

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <mutex>
#include <optional>
#include <thread>
#include <utility>
#include <vector>

#include "abstract_task.hpp"
#include "hyrise.hpp"
#include "job_task.hpp"
#include "shutdown_task.hpp"
#include "task_queue.hpp"
#include "types.hpp"
#include "uid_allocator.hpp"
#include "utils/assert.hpp"
#include "worker.hpp"


namespace {
  /**
   * For the grouping of tasks (see _num_groups()), we scale the number of groups according to the current load of the
   * system. In cases of a single user, we can use a high group count (i.e., allowing parallelism) as the task queue is
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
    std::cerr << "NodeQueueScheduler::finish() wasn't called prior to destroying it." << std::endl;
    std::exit(EXIT_FAILURE);  // NOLINT(concurrency-mt-unsafe)
  }
}

void NodeQueueScheduler::begin() {
  DebugAssert(!_active, "Scheduler is already active.");

  _worker_count = Hyrise::get().topology.num_cpus();
  _workers.reserve(_worker_count);
  _node_count = Hyrise::get().topology.nodes().size();
  _queues.resize(_node_count);
  _workers_per_node.reserve(_node_count);

  // For task lists with less tasks, we do not dynically determine the number of groups to use.
  _min_task_count_for_regrouping = std::max(size_t{16}, _worker_count);

  // Everything above this limit yields the max value for grouping.
  _regrouping_upper_limit = _worker_count * UPPER_LIMIT_QUEUE_SIZE_FACTOR;

  for (auto node_id = NodeID{0}; node_id < _node_count; ++node_id) {
    const auto& topology_node = Hyrise::get().topology.nodes()[node_id];

    // Tracked per node as core restrictions can lead to unbalanced core counts.
    _workers_per_node.emplace_back(topology_node.cpus.size());

    // Only create queues for nodes with CPUs assigned. Otherwise, no workers are active on these nodes and we might
    // add tasks to these queues that can never be directly pulled and must be stolen by other nodes' workers. As
    // ShutdownTasks are not stealable, placing tasks on nodes without workers can lead to failing shutdowns.
    if (!topology_node.cpus.empty()) {
      _active_nodes.push_back(node_id);
      auto queue = std::make_shared<TaskQueue>(node_id);
      _queues[node_id] = queue;

      for (const auto& topology_cpu : topology_node.cpus) {
        // TODO(anybody): Place queues on the actual NUMA node once we have NUMA-aware allocators.
        _workers.emplace_back(
            std::make_shared<Worker>(queue, WorkerID{_worker_id_allocator->allocate()}, topology_cpu.cpu_id));
      }
    }
  }

  Assert(!_active_nodes.empty(), "None of the system nodes has active workers.");
  _active = true;

  for (auto& worker : _workers) {
    worker->start();
    ++_active_worker_count;
  }
}

void NodeQueueScheduler::wait_for_all_tasks() {
  // To check if the system is still processing incoming jobs, we store the previous task count and loop-wait until no
  // new jobs are created anymore.
  auto previous_task_count = TaskID::base_type{_task_counter.load()};

  auto progressless_loop_count = size_t{0};
  auto previous_finished_task_count = TaskID::base_type{0};

  while (true) {
    const auto current_task_count = _task_counter.load();
    if (current_task_count > previous_task_count) {
      // System is still processing new tasks (can happen when, e.g., currently running tasks schedule new tasks):
      // loop-wait until task counter is stable (this check can still fail in edge cases, but we make the simple
      // assumption that nobody calls wait_for_all_tasks() if there is still significant processing ongoing).
      previous_task_count = current_task_count;
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
      continue;
    }

    auto num_finished_tasks = uint64_t{0};
    for (const auto& worker : _workers) {
      num_finished_tasks += worker->num_finished_tasks();
    }

    if (num_finished_tasks == _task_counter) {
      break;
    }

    // Ensure we do not wait forever for tasks that cannot be processed or are stuck. We currently wait 1 hour (3600
    // seconds). This wait time allows us to run TPC-H with scale factor 1000 and two cores without issues, which we
    // consider acceptable right now. If large scale factors or slower data access paths (e.g., data on secondary
    // storage) become relevant, the current mechanism and general query processing probably need to be re-evaluated
    // (e.g., ensure operators split their work into smaller tasks).
    if (progressless_loop_count >= 360'000) {
      const auto remaining_task_count = _task_counter - num_finished_tasks;
      auto message = std::stringstream{};
      // We waited for 1 h (360'000 * 10 ms).
      message << "Timeout: no progress while waiting for all scheduled tasks to be processed. " << remaining_task_count
              << " task(s) still remaining without progress for 1 h now, quitting.";
      Fail(message.str());
    }

    if (previous_finished_task_count == num_finished_tasks) {
      ++progressless_loop_count;
    } else {
      previous_finished_task_count = num_finished_tasks;
      progressless_loop_count = 0;
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }

  for (const auto& queue : _queues) {
    if (!queue) {
      continue;
    }

    auto queue_check_runs = size_t{0};
    while (!queue->empty()) {
      // The following assert checks that we are not looping forever. The empty() check can be inaccurate for
      // concurrent queues when many tiny tasks have been scheduled (see MergeSort scheduler test). When this assert is
      // triggered in other situations, there have probably been new tasks added after wait_for_all_tasks() was called.
      Assert(queue_check_runs < 6'000, "Queue is not empty but all registered tasks have already been processed.");

      std::this_thread::sleep_for(std::chrono::milliseconds(10));
      ++queue_check_runs;
    }
  }
}

void NodeQueueScheduler::finish() {
  // Lock finish() to ensure that the shutdown tasks are not sent twice.
  const auto lock = std::lock_guard<std::mutex>{_finish_mutex};

  if (!_active) {
    return;
  }

  wait_for_all_tasks();

  Assert(static_cast<size_t>(_active_worker_count.load()) == _workers.size(), "Expected all workers to be active.");
  for (auto node_id = NodeID{0}; node_id < _node_count; ++node_id) {
    const auto node_worker_count = _workers_per_node[node_id];
    for (auto worker_id = size_t{0}; worker_id < node_worker_count; ++worker_id) {
      // Create a shutdown task for every worker.
      auto shut_down_task = std::make_shared<ShutdownTask>(_active_worker_count);
      shut_down_task->schedule(node_id);
    }
  }

  auto check_runs = size_t{0};
  while (_active_worker_count.load() > 0) {
    Assert(check_runs < 3'000, "Timeout: not all shut down tasks have been processed.");
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    ++check_runs;
  }

  _active = false;

  for (auto& worker : _workers) {
    worker->join();
  }

  _task_counter = 0;
  _workers = {};
  _queues = {};
  _active_nodes = {};
  _workers_per_node = {};
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
  DebugAssert(_active, "Cannot schedule more tasks after the NodeQueueScheduler was shut down.");
  DebugAssert(task->is_scheduled(), "Do not call NodeQueueScheduler::schedule(), call schedule() on the task.");

  const auto task_counter = _task_counter++;  // Atomically take snapshot of counter
  task->set_id(TaskID{task_counter});

  if (!task->is_ready()) {
    return;
  }

  const auto node_id_for_queue = determine_queue_id(preferred_node_id);
  DebugAssert((static_cast<size_t>(node_id_for_queue) < _queues.size()),
              "Node ID is not within range of available nodes.");
  _queues[node_id_for_queue]->push(task, priority);
}

NodeID NodeQueueScheduler::determine_queue_id(const NodeID preferred_node_id) const {
  const auto active_node_count = _active_nodes.size();

  // Early out: no need to check for preferred node or other queues, if there is only a single node queue.
  if (active_node_count == 1) {
    return _active_nodes[0];
  }

  if (preferred_node_id != CURRENT_NODE_ID) {
    return preferred_node_id;
  }

  // If the current node is requested, try to obtain node from current worker.
  const auto& worker = Worker::get_this_thread_worker();
  if (worker) {
    return worker->queue()->node_id();
  }

  // Initialize minimal values with first active node.
  auto min_load_node_id = _active_nodes[0];
  auto min_load = _queues[min_load_node_id]->estimate_load();

  // When the load of the initial node is small (less tasks than threads on first node), do not check other queues.
  if (min_load < _workers_per_node[min_load_node_id]) {
    return min_load_node_id;
  }

  // Check remaining nodes.
  for (auto node_id_offset = size_t{1}; node_id_offset < active_node_count; ++node_id_offset) {
    const auto node_id = _active_nodes[node_id_offset];
    const auto queue_load = _queues[node_id]->estimate_load();
    if (queue_load < min_load) {
      min_load_node_id = _active_nodes[node_id];
      min_load = queue_load;
    }
  }

  return min_load_node_id;
}

std::optional<size_t> NodeQueueScheduler::determine_group_count(const std::vector<std::shared_ptr<AbstractTask>>& tasks) const {
  if (tasks.size() < _min_task_count_for_regrouping) {
    // For small task lists, we use a group count equal to the number of workers.
    return std::nullopt;
  }

  // We check the first task for a node assignment. We assume that the passed tasks are not assigned to different
  // nodes. If this assumption becomes invalid (e.g., due to work on NUMA optimizations), the current code should be
  // revisited (see check for common NodeID in _group_tasks()).
  const auto first_task_node_id = tasks[0]->node_id();
  const auto node_id_for_queue_check = (first_task_node_id == CURRENT_NODE_ID || first_task_node_id == INVALID_NODE_ID) ? _active_nodes[0] : first_task_node_id;
  std::cout << "Using " << node_id_for_queue_check << std::endl;
  const auto queue_load = _queues[node_id_for_queue_check]->estimate_load();

  const auto fill_level = std::min(queue_load, _regrouping_upper_limit);
  const auto fill_degree = 1.0f - (static_cast<float>(fill_level) / static_cast<float>(_regrouping_upper_limit));
  const auto fill_factor = NUM_GROUPS_MIN_FACTOR + (NUM_GROUPS_RANGE * fill_degree);

  return std::max(size_t{2}, static_cast<size_t>(static_cast<float>(_worker_count) * fill_factor));
}


void NodeQueueScheduler::_group_tasks(const std::vector<std::shared_ptr<AbstractTask>>& tasks) const {
  // Adds predecessor/successor relationships between tasks so that only N tasks (determiend by determine_group_count())
  // can be executed in parallel (tasks with predecessors/successors ares not scheduled).
  // Grouping thus reduces load on the task queues and allows workers to process up to N tasks without coordinating
  // with task queues. On the other hand, it can reduce potential parallelism if too few groups are formed.
  // We use a round robin assignment due to the assumption that chunk characteristics change for older data (e.g., old
  // and infrequently accessed data might be tiered or heavily compressed). A simpler grouping (e.g., forming the first
  // chain with the first N tasks) could cause chain processing to be inbalanced (e.g., chains processing frequently
  // accessed data might be less expensive than ones processing tiered data).
  // Approach: Skip all tasks that already have predecessors or successors, as adding relationships to these could
  // introduce cyclic dependencies.

  auto round_robin_counter = 0;
  auto common_node_id = std::optional<NodeID>{};

  const auto num_groups = determine_group_count(tasks);
  if (!num_groups) {
    return;
  }

  auto grouped_tasks = std::vector<std::shared_ptr<AbstractTask>>(*num_groups);
  // Tasks are iterated in reverse as we set tasks as predecessors.
  for (auto iter = tasks.rbegin(); iter != tasks.rend(); ++iter) {
    const auto& task = *iter;
    if (!task->predecessors().empty() || !task->successors().empty() || dynamic_cast<ShutdownTask*>(&*task)) {
      // Do not group tasks that either have precessors/successors or are ShutdownTasks.
      return;
    }

    if (HYRISE_DEBUG) {
      if (common_node_id) {
        // This is not really a hard assertion. As the chain will likely be executed on the same worker (see
        // Worker::execute_next), we would ignore all but the first node_id. At the time of writing, we did not do any
        // smart node assignment. This assertion is only here so that this behavior is understood if we ever assign NUMA
        // node ids.
        DebugAssert(task->node_id() == *common_node_id, "Expected all grouped tasks to have the same node_id.");
      } else {
        common_node_id = task->node_id();
      }
    }

    const auto group_id = round_robin_counter % *num_groups;
    const auto& previous_task_in_group = grouped_tasks[group_id];
    if (previous_task_in_group) {
      task->set_as_predecessor_of(previous_task_in_group);
    }
    grouped_tasks[group_id] = task;
    ++round_robin_counter;
  }
}

const std::atomic_int64_t& NodeQueueScheduler::active_worker_count() const {
  return _active_worker_count;
}

}  // namespace hyrise
