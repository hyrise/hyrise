#include "worker.hpp"

#include <pthread.h>
#include <sched.h>
#include <unistd.h>

#include <chrono>
#include <iostream>
#include <memory>
#include <thread>
#include <vector>

#include "current_scheduler.hpp"
#include "processing_unit.hpp"
#include "topology.hpp"

namespace {

thread_local std::weak_ptr<opossum::Worker> this_thread_worker;
}

namespace opossum {

std::shared_ptr<Worker> Worker::get_this_thread_worker() { return ::this_thread_worker.lock(); }

Worker::Worker(std::weak_ptr<ProcessingUnit> processing_unit, std::shared_ptr<TaskQueue> queue, WorkerID id,
               CpuID cpu_id)
    : _processing_unit(processing_unit), _queue(queue), _id(id), _cpu_id(cpu_id) {}

WorkerID Worker::id() const { return _id; }

std::shared_ptr<TaskQueue> Worker::queue() const { return _queue; }

CpuID Worker::cpu_id() const { return _cpu_id; }

std::weak_ptr<ProcessingUnit> Worker::processing_unit() const { return _processing_unit; }

void Worker::_wait_for_tasks(const std::vector<std::shared_ptr<AbstractTask>>& tasks) {
  /**
   * This method blocks the calling thread (worker) until all tasks have been completed.
   * It hands off the active worker token so that another worker can execute tasks while the calling worker is blocked.
   */
  auto processing_unit = _processing_unit.lock();
  if (IS_DEBUG && !processing_unit) {
    throw std::logic_error("Bug: Locking the processing unit failed");
  }

  processing_unit->yield_active_worker_token(_id);
  processing_unit->wake_or_create_worker();

  for (auto& task : tasks) {
    task->_join_without_replacement_worker();
  }
}

void Worker::operator()() {
  if (IS_DEBUG && !this_thread_worker.expired()) {
    throw std::logic_error("Thread already has a worker");
  }

  this_thread_worker = shared_from_this();

  _set_affinity();

  auto scheduler = CurrentScheduler::get();

  if (IS_DEBUG && !scheduler) {
    throw std::logic_error("No scheduler");
  }

  while (!_processing_unit.lock()->shutdown_flag()) {
    // Hibernate if this is not the active worker.
    {
      auto processing_unit = _processing_unit.lock();
      if (IS_DEBUG && !processing_unit) {
        throw std::logic_error("No processing unit");
      }

      auto this_worker_is_active = processing_unit->try_acquire_active_worker_token(_id);
      if (!this_worker_is_active) {
        processing_unit->hibernate_calling_worker();
        continue;  // Re-try to become the active worker
      }
    }

    auto task = _queue->pull();

    // TODO(all): this might shutdown the worker and leave non-ready tasks in the queue.
    // Figure out how we want to deal with that later.
    if (!task) {
      // Simple work stealing without explicitly transferring data between nodes.
      auto work_stealing_successful = false;
      for (auto& queue : scheduler->queues()) {
        if (queue == _queue) {
          continue;
        }

        task = queue->pull();
        if (task) {
          task->set_node_id(_queue->node_id());
          work_stealing_successful = true;
          break;
        }
      }

      // Sleep iff there is no ready task in our queue and work stealing was not successful.
      if (!work_stealing_successful) {
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
        continue;
      }
    }

    task->execute();

    // This is part of the Scheduler shutdown system. Count the number of tasks a ProcessingUnit executed to allow the
    // Scheduler to determine whether all tasks finished
    _processing_unit.lock()->on_worker_finished_task();
  }

  _processing_unit.lock()->yield_active_worker_token(_id);
}

void Worker::_set_affinity() {
#if OPOSSUM_NUMA_SUPPORT
  cpu_set_t cpuset;
  CPU_ZERO(&cpuset);
  CPU_SET(_cpu_id, &cpuset);
  auto rc = pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
  if (rc != 0) {
    // This is not an Assert(), though maybe it should be. Not being able to pin the threads doesn't make the DB
    // unfunctional, but probably slower
    std::cerr << "Error calling pthread_setaffinity_np: " << rc << std::endl;
  }
#endif
}
}  // namespace opossum
