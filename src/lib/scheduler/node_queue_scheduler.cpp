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
  constexpr auto NUM_GROUPS_MIN_FACTOR = 0.1f;
  constexpr auto NUM_GROUPS_MAX_FACTOR = 4.0f;

  constexpr auto NUM_GROUPS_RANGE = NUM_GROUPS_MAX_FACTOR - NUM_GROUPS_MIN_FACTOR;

  constexpr auto MAX_QUEUE_SIZE_FACTOR = size_t{6};
}

namespace hyrise {

NodeQueueScheduler::NodeQueueScheduler() {
  _worker_id_allocator = std::make_shared<UidAllocator>();

  // Good default value
  _num_workers =Hyrise::get().topology.num_cpus();
  NUM_GROUPS = _num_workers;
  std::printf("####\n####\n####\t\t %f\n###\n####\n####\n", static_cast<float>(0.4f));
  std::printf("####\n####\n####\t\t %f\n###\n####\n####\n", static_cast<float>(4.0f));
  std::printf("####\n####\n####\t\t %lu\n###\n####\n####\n", static_cast<size_t>(8));
  std::printf("####\n####\n####\t\t %f\n###\n####\n####\n", static_cast<float>(1.0f));
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

  _workers.reserve(Hyrise::get().topology.num_cpus());
  _queues.reserve(Hyrise::get().topology.nodes().size());

  for (auto node_id = NodeID{0}; node_id < Hyrise::get().topology.nodes().size(); node_id++) {
    auto queue = std::make_shared<TaskQueue>(node_id);

    _queues.emplace_back(queue);

    const auto& topology_node = Hyrise::get().topology.nodes()[node_id];

    for (const auto& topology_cpu : topology_node.cpus) {
      _workers.emplace_back(
          std::make_shared<Worker>(queue, WorkerID{_worker_id_allocator->allocate()}, topology_cpu.cpu_id));
    }
  }

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

  // Lookup node id for current worker.
  if (preferred_node_id == CURRENT_NODE_ID) {
    auto worker = Worker::get_this_thread_worker();
    if (worker) {
      preferred_node_id = worker->queue()->node_id();
    } else {
      // TODO(all): Actually, this should be ANY_NODE_ID, LIGHT_LOAD_NODE or something
      preferred_node_id = NodeID{0};
    }
  }

  DebugAssert(!(static_cast<size_t>(preferred_node_id) >= _queues.size()),
              "preferred_node_id is not within range of available nodes");

  auto queue = _queues[preferred_node_id];
  queue->push(task, static_cast<uint32_t>(priority));
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

  auto num_groups = NUM_GROUPS;

  if (!tasks.empty() && tasks.size() > static_cast<size_t>(static_cast<float>(NUM_GROUPS) * static_cast<float>(0.75f))) {
    //std::printf("1\n");

    //const auto node_id_for_queue_check = (tasks[0]->node_id() == CURRENT_NODE_ID) ? NodeID{0} : tasks[0]->node_id();
    const auto node_id_for_queue_check = (tasks[0]->node_id() == CURRENT_NODE_ID || tasks[0]->node_id() == INVALID_NODE_ID) ? NodeID{0} : tasks[0]->node_id();

    //std::printf("1a, %u\n", static_cast<NodeID::base_type>(node_id_for_queue_check));
    //std::printf("1b, %lu\n", static_cast<size_t>(tasks[0]->node_id()));
    //std::printf("1c, %lu\n", static_cast<size_t>(CURRENT_NODE_ID));
    const auto cumu_length_queues = 2*_queues[node_id_for_queue_check]->_queues[0].unsafe_size() + _queues[node_id_for_queue_check]->_queues[1].unsafe_size();
    //std::printf("2\n");

    const auto upper_limit = _num_workers * MAX_QUEUE_SIZE_FACTOR;  // Everything above this limit yields the max value for grouping.
    const auto fill_level = std::min(cumu_length_queues, upper_limit);
    const auto fill_degree = 1.0f - (static_cast<float>(fill_level) / static_cast<float>(upper_limit));
    const auto fill_factor = NUM_GROUPS_MIN_FACTOR + (NUM_GROUPS_RANGE * fill_degree);

    //std::printf("3\n");
    num_groups = static_cast<size_t>(static_cast<float>(_num_workers) * fill_factor);
    //std::printf("num groups: %lu for length: %lu (fill degree is %f)\n", num_groups, cumu_length_queues, fill_factor);
  }

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
