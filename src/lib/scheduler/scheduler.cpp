#include <memory>
#include <utility>

#include <iostream>

#include "abstract_topology.hpp"
#include "scheduler.hpp"

namespace opossum {

Scheduler::Scheduler(std::shared_ptr<AbstractTopology> topology) : AbstractScheduler(topology) {
  _topology->setup(*this);

  _threads.reserve(_topology->workers().size());

  for (auto& worker : _topology->workers()) {
    auto fn = std::bind(&Worker::operator(), worker.get());
    _threads.emplace_back(fn);
  }
}

Scheduler::~Scheduler() { finish(); }

void Scheduler::finish() {
  // Wait for all queues to empty
  for (auto& queue : _topology->queues()) {
    while (!queue->empty()) {
      std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
  }

  for (auto& worker : _topology->workers()) {
    worker->set_shutdown_flag(true);
  }

  for (auto& thread : _threads) {
    thread.join();
  }
}

void Scheduler::schedule(std::shared_ptr<AbstractTask> task, uint32_t preferred_node_id) {
  task->set_id(_task_id_incrementor);
  _task_id_incrementor++;

  auto queue = _topology->node_queue(preferred_node_id);
  queue->push(std::move(task));
}

}  // namespace opossum
