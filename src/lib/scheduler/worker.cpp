#include "worker.hpp"

#include <pthread.h>
#include <sched.h>
#include <unistd.h>

#include <chrono>
#include <iostream>
#include <memory>
#include <thread>

#include "abstract_scheduler.hpp"
#include "abstract_topology.hpp"

namespace opossum {

Worker::Worker(AbstractScheduler& scheduler, std::shared_ptr<TaskQueue> queue, WorkerID id, CpuID cpu_id)
    : _scheduler(scheduler), _queue(queue), _id(id), _cpu_id(cpu_id), _shutdown_flag(false) {}

WorkerID Worker::id() const { return _id; }

CpuID Worker::cpu_id() const { return _cpu_id; }

void Worker::operator()() {
  _set_affinity();

  while (true) {
    auto task = _queue->pull();

    // TODO(all): this might shutdown the worker and leave non-ready tasks in the queue.
    // Figure out how we want to deal with that later.
    if (!task) {
      if (_shutdown_flag) {
        break;
      }

      // Simple work stealing without transferring data between nodes
      auto work_stealing_successful = false;
      for (auto& queue : _scheduler.topology()->queues()) {
        if (queue == _queue) {
          continue;
        }

        auto ready_task = queue->get_ready_task();
        if (ready_task) {
          // Give the owning node a chance to execute the task itself.
          std::this_thread::sleep_for(std::chrono::milliseconds(10));

          // Steal the task. If the owning node (or somebody else) already started it, steal_task will return a nullptr.
          task = queue->steal_task(ready_task);
          if (task) {
            task->set_node_id(_queue->node_id());
            work_stealing_successful = true;
            break;
          }
        }
      }

      // Sleep iff there is no ready task in our queue and work stealing was not successful.
      if (!work_stealing_successful) {
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
        continue;
      }
    }

    task->execute();
  }
}

void Worker::set_shutdown_flag(bool flag) { _shutdown_flag = flag; }

void Worker::_set_affinity() {
#if OPOSSUM_NUMA_SUPPORT
  cpu_set_t cpuset;
  CPU_ZERO(&cpuset);
  CPU_SET(_cpu_id, &cpuset);
  auto rc = pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
  if (rc != 0) {
    std::cerr << "Error calling pthread_setaffinity_np: " << rc << std::endl;
  }
#endif
}

}  // namespace opossum
