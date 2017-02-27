#include "node_queue_scheduler.hpp"

#include <cstdlib>
#include <iostream>
#include <memory>
#include <utility>
#include <vector>

#include "current_scheduler.hpp"
#include "processing_unit.hpp"
#include "topology.hpp"

namespace opossum {

NodeQueueScheduler::NodeQueueScheduler(std::shared_ptr<Topology> topology) : AbstractScheduler(topology) {
  _worker_id_allocator = std::make_shared<UidAllocator>();
}

NodeQueueScheduler::~NodeQueueScheduler() {
  if (IS_DEBUG && !_shut_down) {
    // We cannot throw an exception because destructors are noexcept by default.
    std::cerr << "NodeQueueScheduler::finish() wasn't called prior to destroying it" << std::endl;
    std::exit(EXIT_FAILURE);
  }
}

void NodeQueueScheduler::begin() {
  _processing_units.reserve(_topology->numCpus());
  _queues.reserve(_topology->nodes().size());

  for (size_t q = 0; q < _topology->nodes().size(); q++) {
    auto queue = std::make_shared<TaskQueue>(q);

    _queues.emplace_back(queue);

    auto& topology_node = _topology->nodes()[q];

    for (auto& topology_cpu : topology_node.cpus) {
      _processing_units.emplace_back(std::make_shared<ProcessingUnit>(queue, _worker_id_allocator, topology_cpu.cpuID));
    }
  }

  for (auto& processing_unit : _processing_units) {
    processing_unit->wake_or_create_worker();
  }
}

void NodeQueueScheduler::finish() {
  /**
   * Periodically count all finished tasks and when this number matches the number of scheduled tasks, it is safe to
   * shut down
   */
  while (true) {
    uint64_t num_finished_tasks = 0;
    for (auto& processing_unit : _processing_units) {
      num_finished_tasks += processing_unit->num_finished_tasks();
    }

    if (num_finished_tasks == _task_counter) break;

    std::this_thread::sleep_for(std::chrono::milliseconds(200));
  }

  // All queues SHOULD be empty by now
  for (auto& queue : _queues) {
    if (IS_DEBUG && !queue->empty()) {
      throw std::logic_error("NodeQueueScheduler bug: Queue wasn't empty even though all tasks finished");
    }
  }

  for (auto& processing_unit : _processing_units) {
    processing_unit->shutdown();
  }

  for (auto& processing_unit : _processing_units) {
    processing_unit->join();
  }

  _shut_down = true;
}

const std::vector<std::shared_ptr<TaskQueue>>& NodeQueueScheduler::queues() const { return _queues; }

void NodeQueueScheduler::schedule(std::shared_ptr<AbstractTask> task, NodeID preferred_node_id,
                                  SchedulePriority priority) {
  /**
   * Add task to the queue of the preferred node.
   */
  if (IS_DEBUG) {
    if (_shut_down) {
      throw std::logic_error("Can't schedule more tasks after the NodeQueueScheduler was shut down");
    }

    if (!task->is_scheduled()) {
      throw std::logic_error("Don't call NodeQueueScheduler::schedule(), call schedule() on the Task");
    }
  }

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
      preferred_node_id = 0;
    }
  }

  if (IS_DEBUG && preferred_node_id >= _queues.size()) {
    throw std::logic_error("preferred_node_id is not within range of available nodes");
  }

  auto queue = _queues[preferred_node_id];
  queue->push(std::move(task), static_cast<uint32_t>(priority));
}
}  // namespace opossum
