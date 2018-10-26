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

Worker::Worker(const std::shared_ptr<TaskQueue>& queue, WorkerID id, CpuID cpu_id)
    : _queue(queue), _id(id), _cpu_id(cpu_id) {}

WorkerID Worker::id() const { return _id; }

std::shared_ptr<TaskQueue> Worker::queue() const { return _queue; }

CpuID Worker::cpu_id() const { return _cpu_id; }

void Worker::operator()() {
  Assert(this_thread_worker.expired(), "Thread already has a worker");

  this_thread_worker = shared_from_this();

  _set_affinity();

  while (CurrentScheduler::get()->active()) {
    _work();
  }
}

void Worker::_work() {
  auto task = _queue->pull();

  if (!task) {
    // Simple work stealing without explicitly transferring data between nodes.
    auto work_stealing_successful = false;
    for (auto& queue : CurrentScheduler::get()->queues()) {
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
      return;
    }
  }

  task->execute();

  // This is part of the Scheduler shutdown system. Count the number of tasks a Worker executed to allow the
  // Scheduler to determine whether all tasks finished
  _num_finished_tasks++;
}

void Worker::start() { _thread = std::thread(&Worker::operator(), this); }

void Worker::join() {
  Assert(!CurrentScheduler::get()->active(), "Worker can't be join()-ed while the scheduler is still active");
  _thread.join();
}

uint64_t Worker::num_finished_tasks() const { return _num_finished_tasks; }

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
