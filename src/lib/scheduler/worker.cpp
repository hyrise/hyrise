#include "worker.hpp"

#include <pthread.h>
#include <sched.h>
#include <unistd.h>

#include <chrono>
#include <iostream>
#include <memory>
#include <thread>
#include <vector>

#include "abstract_scheduler.hpp"
#include "abstract_task.hpp"
#include "current_scheduler.hpp"
#include "task_queue.hpp"

namespace {

/**
 * On worker threads, this references the Worker running on this thread, on all other threads, this is empty.
 * Uses a weak_ptr, because otherwise the ref-count of it would not reach zero within the main() scope of the program.
 */
thread_local std::weak_ptr<opossum::Worker> this_thread_worker;
}  // namespace

namespace opossum {

std::shared_ptr<Worker> Worker::get_this_thread_worker() { return ::this_thread_worker.lock(); }

Worker::Worker(const std::weak_ptr<ProcessingUnit>& processing_unit, const std::shared_ptr<TaskQueue>& queue,
               WorkerID id, CpuID cpu_id, SchedulePriority min_priority)
    : _processing_unit(processing_unit), _queue(queue), _id(id), _cpu_id(cpu_id), _min_priority(min_priority) {}

WorkerID Worker::id() const { return _id; }

std::shared_ptr<TaskQueue> Worker::queue() const { return _queue; }

CpuID Worker::cpu_id() const { return _cpu_id; }

std::weak_ptr<ProcessingUnit> Worker::processing_unit() const { return _processing_unit; }

void Worker::operator()() {
  DebugAssert((this_thread_worker.expired()), "Thread already has a worker");

  this_thread_worker = shared_from_this();

  _set_affinity();

  auto scheduler = CurrentScheduler::get();

  DebugAssert(static_cast<bool>(scheduler), "No scheduler");

  auto processing_unit = _processing_unit.lock();

  DebugAssert(static_cast<bool>(processing_unit), "No processing unit");

  while (!processing_unit->shutdown_flag()) {
    // Hibernate if this is not the active worker.
    {
      auto this_worker_is_active = processing_unit->try_acquire_active_worker_token(_id);
      if (!this_worker_is_active) {
        processing_unit->hibernate_calling_worker();
        continue;  // Re-try to become the active worker
      }
    }

    auto task = _queue->pull(_min_priority);

    // TODO(all): this might shutdown the worker and leave non-ready tasks in the queue.
    // Figure out how we want to deal with that later.
    if (!task) {
      // If the worker is the dedicated JobTask worker, go back into hibernation to wake up
      // (or pass priority to) one of the all-priorities workers.
      if (_min_priority < SchedulePriority::Lowest) {
        processing_unit->yield_active_worker_token(_id);
        processing_unit->wake_or_create_worker();
        continue;  // Re-try to become the active worker
      }

      // Simple work stealing without explicitly transferring data between nodes.
      auto work_stealing_successful = false;
      for (auto& queue : scheduler->queues()) {
        if (queue == _queue) {
          continue;
        }

        task = queue->steal();
        if (task) {
          task->set_node_id(_queue->node_id());
          work_stealing_successful = true;
          break;
        }
      }

      // Sleep if there is no ready task in our queue and work stealing was not successful.
      if (!work_stealing_successful) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        continue;
      }
    }

    task->execute();

    // This is part of the Scheduler shutdown system. Count the number of tasks a ProcessingUnit executed to allow the
    // Scheduler to determine whether all tasks finished
    processing_unit->on_worker_finished_task();
  }

  processing_unit->yield_active_worker_token(_id);
}

void Worker::_set_affinity() {
#if HYRISE_NUMA_SUPPORT
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
