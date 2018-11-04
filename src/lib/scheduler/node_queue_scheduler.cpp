#include "node_queue_scheduler.hpp"

#include <cstdlib>
#include <iostream>
#include <memory>
#include <utility>
#include <vector>

#include "abstract_task.hpp"
#include "current_scheduler.hpp"
#include "task_queue.hpp"
#include "topology.hpp"
#include "worker.hpp"

#include "uid_allocator.hpp"
#include "utils/assert.hpp"

namespace opossum {

NodeQueueScheduler::NodeQueueScheduler() { _worker_id_allocator = std::make_shared<UidAllocator>(); }

NodeQueueScheduler::~NodeQueueScheduler() {
  if (IS_DEBUG && _active) {
    // We cannot throw an exception because destructors are noexcept by default.
    std::cerr << "NodeQueueScheduler::finish() wasn't called prior to destroying it" << std::endl;
    std::exit(EXIT_FAILURE);
  }
}

void NodeQueueScheduler::begin() {
  _workers.reserve(Topology::get().num_cpus());
  _queues.reserve(Topology::get().nodes().size());

  for (auto node_id = NodeID{0}; node_id < Topology::get().nodes().size(); node_id++) {
    auto queue = std::make_shared<TaskQueue>(node_id);

    _queues.emplace_back(queue);

    auto& topology_node = Topology::get().nodes()[node_id];

    for (auto& topology_cpu : topology_node.cpus) {
      _workers.emplace_back(std::make_shared<Worker>(queue, _worker_id_allocator->allocate(), topology_cpu.cpu_id));
    }
  }

  _active = true;

  for (auto& worker : _workers) {
    worker->start();
  }
}

void NodeQueueScheduler::finish() {
  /**
   * Periodically count all finished tasks and when this number matches the number of scheduled tasks, it is safe to
   * shut down
   */
  while (true) {
    uint64_t num_finished_tasks = 0;
    for (auto& worker : _workers) {
      num_finished_tasks += worker->num_finished_tasks();
    }

    if (num_finished_tasks == _task_counter) break;

    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }

  // All queues SHOULD be empty by now
  if (IS_DEBUG) {
    for ([[gnu::unused]] auto& queue : _queues) {
      DebugAssert(queue->empty(), "NodeQueueScheduler bug: Queue wasn't empty even though all tasks finished");
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

bool NodeQueueScheduler::active() const { return _active; }

const std::vector<std::shared_ptr<TaskQueue>>& NodeQueueScheduler::queues() const { return _queues; }

void NodeQueueScheduler::schedule(std::shared_ptr<AbstractTask> task, NodeID preferred_node_id,
                                  SchedulePriority priority) {
  /**
   * Add task to the queue of the preferred node.
   */
  DebugAssert(_active, "Can't schedule more tasks after the NodeQueueScheduler was shut down");
  DebugAssert(task->is_scheduled(), "Don't call NodeQueueScheduler::schedule(), call schedule() on the task");

  const auto task_counter = _task_counter++;  // Atomically take snapshot of counter
  task->set_id(task_counter);

  if (!task->is_ready()) return;

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
}  // namespace opossum
