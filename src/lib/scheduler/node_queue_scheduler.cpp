#include "node_queue_scheduler.hpp"

#include <atomic>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <utility>
#include <vector>

#include "abstract_task.hpp"
#include "hyrise.hpp"
#include "job_task.hpp"
#include "shutdown_task.hpp"
#include "task_queue.hpp"
#include "uid_allocator.hpp"
#include "utils/assert.hpp"
#include "worker.hpp"

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

  _workers.reserve(Hyrise::get().topology.num_cpus());
  _node_count = Hyrise::get().topology.nodes().size();
  _queues.resize(_node_count);
  _workers_per_node.reserve(_node_count);

  for (auto node_id = NodeID{0}; node_id < Hyrise::get().topology.nodes().size(); ++node_id) {
    const auto& topology_node = Hyrise::get().topology.nodes()[node_id];

    // Tracked per node as core restrictions can lead to unbalanced core counts.
    _workers_per_node.emplace_back(topology_node.cpus.size());

    // Only create queues for nodes with CPUs assigned. Otherwise, we might add tasks to these queues that can never be
    // directly pulled and must be stolen by other nodes' CPUs. This can lead to problems when shutting down the
    // scheduler.
    if (!topology_node.cpus.empty()) {
      _active_nodes.push_back(node_id);
      auto queue = std::make_shared<TaskQueue>(node_id);
      _queues[node_id] = queue;

      for (const auto& topology_cpu : topology_node.cpus) {
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
  auto progressless_loop_count = size_t{0};
  auto previous_finished_task_count = size_t{0};
  while (true) {
    auto num_finished_tasks = uint64_t{0};
    for (const auto& worker : _workers) {
      num_finished_tasks += worker->num_finished_tasks();
    }

    if (num_finished_tasks == _task_counter) {
      break;
    }

    // Ensure we do not wait forever for tasks that cannot be processed or are stuck. 10s seemed too little for TSAN
    // builds in the CI.
    if (progressless_loop_count >= 1'500) {
      const auto remaining_task_count = _task_counter - num_finished_tasks;
      auto message = std::stringstream{};
      // We waited for 10 ms, 1'500 times = 15s.
      message << "Timeout: no progress while waiting for all scheduled tasks to be processed. " << remaining_task_count
              << " task(s) still remaining for 15s now, quitting.";
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
      Assert(queue_check_runs < 1'000, "Queue is not empty but all registered tasks have already been processed.");

      std::this_thread::sleep_for(std::chrono::milliseconds(1));
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
    Assert(check_runs < 1'000, "Timeout: not all shut down tasks have been processed.");
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
    ++check_runs;
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
  DebugAssert(_active, "Can't schedule more tasks after the NodeQueueScheduler was shut down.");
  DebugAssert(task->is_scheduled(), "Don't call NodeQueueScheduler::schedule(), call schedule() on the task.");

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
  // Early out: no need to check for preferred node or other queues, if there is only a single node queue.
  if (_active_nodes.size() == 1) {
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

  // Initial min values with first active node.
  auto min_load_node_id = _active_nodes[0];
  auto min_load = _queues[min_load_node_id]->estimate_load();

  // When the load of the initial node is small (less tasks than threads on first node), do not check other queues.
  if (min_load < _workers_per_node[min_load_node_id]) {
    return min_load_node_id;
  }

  // Check remaining nodes.
  const auto active_node_count = _active_nodes.size();
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

void NodeQueueScheduler::_group_tasks(const std::vector<std::shared_ptr<AbstractTask>>& tasks) const {
  // Adds predecessor/successor relationships between tasks so that only NUM_GROUPS tasks can be executed in parallel.
  // The optimal value of NUM_GROUPS depends on the number of cores and the number of queries being executed
  // concurrently. The current value has been found with a divining rod.
  //
  // Approach: Skip all tasks that already have predecessors or successors, as adding relationships to these could
  // introduce cyclic dependencies. Again, this is far from perfect, but better than not grouping the tasks.

  auto round_robin_counter = 0;
  auto common_node_id = std::optional<NodeID>{};

  std::vector<std::shared_ptr<AbstractTask>> grouped_tasks(NUM_GROUPS);
  for (const auto& task : tasks) {
    if (!task->predecessors().empty() || !task->successors().empty() || dynamic_cast<ShutdownTask*>(&*task)) {
      // Do not group tasks that either have precessors/successors or are ShutdownTasks.
      return;
    }

    if (common_node_id) {
      // This is not really a hard assertion. As the chain will likely be executed on the same worker (see
      // Worker::execute_next), we would ignore all but the first node_id. At the time of writing, we did not do any
      // smart node assignment. This assertion is only here so that this behavior is understood if we ever assign NUMA
      // node ids.
      DebugAssert(task->node_id() == *common_node_id, "Expected all grouped tasks to have the same node_id.");
    } else {
      common_node_id = task->node_id();
    }

    const auto group_id = round_robin_counter % NUM_GROUPS;
    const auto& first_task_in_group = grouped_tasks[group_id];
    if (first_task_in_group) {
      task->set_as_predecessor_of(first_task_in_group);
    }
    grouped_tasks[group_id] = task;
    ++round_robin_counter;
  }
}

const std::atomic_int64_t& NodeQueueScheduler::active_worker_count() const {
  return _active_worker_count;
}

}  // namespace hyrise
