#include "worker.hpp"

#include <pthread.h>
#include <sched.h>
#include <unistd.h>

#include <chrono>
#include <iostream>
#include <memory>
#include <mutex>
#include <random>
#include <thread>
#include <vector>

#include "abstract_scheduler.hpp"
#include "abstract_task.hpp"
#include "hyrise.hpp"
#include "task_queue.hpp"

namespace {

using namespace hyrise;  // NOLINT(build/namespaces)

/**
 * On worker threads, this references the Worker running on this thread, on all other threads, this is empty.
 * Uses a weak_ptr, because otherwise the ref-count of it would not reach zero within the main() scope of the program.
 */
thread_local std::weak_ptr<Worker> this_thread_worker;  // NOLINT (clang-tidy wants this const)

bool get_task(const bool check_only_local_queue, const std::atomic_bool& shutdown_flag,
              std::shared_ptr<AbstractTask>& task, std::shared_ptr<AbstractTask>& next_task,
              std::shared_ptr<TaskQueue>& local_queue, const std::vector<std::shared_ptr<TaskQueue>>& all_queues) {
  // If execute_next has been called, run that task first, otherwise try to retrieve a task from the queue.
  if (next_task) {
      task = std::move(next_task);
      next_task = nullptr;
      return true;
  }

  // When waiting for the semaphore, we always check if the worker is supposed to shut down. Otherwise, a worker might
  // "consume" muliple shutdown signals (no tasks in the queue) which hinders other workers for awaking.
  if (!shutdown_flag && local_queue->semaphore.tryWait()) {
    task = local_queue->pull();
    return true;
  }

  if (check_only_local_queue) {
    return false;
  }

  for (const auto& queue : all_queues) {
    if (queue == local_queue) {
      continue;
    }

    if (!shutdown_flag && queue->semaphore.tryWait()) {
      task = queue->steal();
      if (task) {
        task->set_node_id(local_queue->node_id());
        return true;
      }
    }
  }

  return false;
}

}  // namespace

namespace hyrise {

std::shared_ptr<Worker> Worker::get_this_thread_worker() {
  return ::this_thread_worker.lock();
}

Worker::Worker(const std::shared_ptr<TaskQueue>& queue, WorkerID worker_id, CpuID cpu_id,
               std::atomic_bool& shutdown_flag)
    : _queue(queue), _id(worker_id), _cpu_id(cpu_id), _shutdown_flag(shutdown_flag) {
  // Generate a random distribution from 0-99 for later use, see below
  _random.resize(100);
  std::iota(_random.begin(), _random.end(), 0);
  std::shuffle(_random.begin(), _random.end(), std::default_random_engine{std::random_device{}()});
}

WorkerID Worker::id() const {
  return _id;
}

std::shared_ptr<TaskQueue> Worker::queue() const {
  return _queue;
}

CpuID Worker::cpu_id() const {
  return _cpu_id;
}

void Worker::operator()() {
  Assert(this_thread_worker.expired(), "Thread already has a worker");

  this_thread_worker = shared_from_this();

  _set_affinity();

  while (Hyrise::get().scheduler()->active() && !_shutdown_flag) {
    // Worker is allowed to sleep (when queue is empty) as long as the scheduler is not shutting down.
    _work(true);
  }
}

void Worker::_work(const bool allow_sleep) {
  const auto& queues = Hyrise::get().scheduler()->queues();

  auto task = std::shared_ptr<AbstractTask>{};
  const auto got_local_task = get_task(true, _shutdown_flag, task, _next_task, _queue, queues);
  if (!got_local_task) {
    const auto got_distant_task = get_task(false, _shutdown_flag, task, _next_task, _queue, queues);
    if (!got_distant_task) {
      // If there is no ready task neither in the local queue nor in any other, the worker waits for a new task to be
      // pushed to the own queue. The waiting is skipped in case the scheduler is shutting down or sleep is not allowed
      //  (e.g., when _work() is called for a known number of unfinished jobs, see wait_for_tasks()).
      if (allow_sleep && !get_task(true, _shutdown_flag, task, _next_task, _queue, queues)) {
        _queue->semaphore.wait();
        task = _queue->pull();
      }
    }
  }

  if (!task) {
    return;
  }

  const auto successfully_assigned = task->try_mark_as_assigned_to_worker();
  if (!successfully_assigned) {
    // Some other worker has already started to work on this task - pick a different one.
    return;
  }

  task->execute();

  // This is part of the Scheduler shutdown system. Count the number of tasks a Worker executed to allow the
  // Scheduler to determine whether all tasks finished
  _num_finished_tasks++;
}

void Worker::execute_next(const std::shared_ptr<AbstractTask>& task) {
  DebugAssert(&*get_this_thread_worker() == this,
              "execute_next must be called from the same thread that the worker works in");
  if (!_next_task) {
    const auto successfully_enqueued = task->try_mark_as_enqueued();
    if (!successfully_enqueued) {
      // The task was already enqueued. This can happen if
      //   * two tasks ARE TO BE scheduled via AbstractScheduler::schedule where one task is the other one's successor
      //   * the first one is scheduled and executed very quickly before the second one reaches the schedule method
      //   * AbstractScheduler::schedule then looks at the second task and realizes that it is ready to be enqueued
      //   * ... and both the scheduler and this method try to enqueue it.
      // If successfully_enqueued is false, we lost, and the task is already in one of the task queues.
      return;
    }
    Assert(successfully_enqueued, "Task was already enqueued, expected to be solely responsible for execution");
    _next_task = task;
  } else {
    _queue->push(task, SchedulePriority::Default);
  }
}

void Worker::start() {
  _thread = std::thread(&Worker::operator(), this);
}

void Worker::join() {
  Assert(!Hyrise::get().scheduler()->active(), "Worker can't be join()-ed while the scheduler is still active");
  _thread.join();
}

uint64_t Worker::num_finished_tasks() const {
  return _num_finished_tasks;
}

void Worker::_wait_for_tasks(const std::vector<std::shared_ptr<AbstractTask>>& tasks) {
  // This lambda checks if all tasks from the vector (our "own" tasks) have been executed. If they are, it causes
  // _wait_for_tasks to return. If there are remaining tasks, it primarily tries to execute these. If they cannot be
  // executed, the worker performs work for others (i.e., executes tasks from the queue).
  auto all_own_tasks_done = [&]() {
    auto all_done = true;
    // Note: If we found a task that has not yet been executed, we reset this loop and start from the beginning.
    // As such, both all_done and it may be reset outside of the following line.
    auto it = tasks.begin();
    while (it != tasks.end()) {
      const auto& task = *it;
      if (task->is_done()) {
        ++it;
        continue;
      }

      if (!task->is_ready()) {
        // Task is not yet done - check if it is ready for execution
        all_done = false;

        // Task cannot be executed. We could stop here and simply return all_own_tasks_done == false. Instead, we
        // continue in the list of tasks and check if there are any other tasks that we could work on.
        ++it;
        continue;
      }

      // Give other tasks, i.e., tasks that are unrelated to what we are currently waiting on, a certain chance of being
      // executed, too. Anecdotal evidence says that this is a good idea. For some reason, this keeps the memory
      // consumption of TPC-H Q6 low even if the scheduler is overcommitted. Because generating random numbers is
      // somewhat expensive, we keep a list of random numbers and reuse them.
      // TODO(anyone): Look deeper into scheduling theory and make this theoretically sound.
      _next_random = (_next_random + 1) % _random.size();
      if (_random[_next_random] <= 20) {
        return false;
      }

      // Run one of our own tasks. First, let everyone know that we are about to execute it. This is necessary because
      // the task might already be in a queue and some other worker might pull it at the same time.
      const auto successfully_assigned = task->try_mark_as_assigned_to_worker();
      if (!successfully_assigned) {
        // Some other worker has already started to work on this task - pick a different one.
        all_done = false;
        ++it;
        continue;
      }

      // Actually execute it.
      task->execute();
      ++_num_finished_tasks;

      // Reset loop so that we re-visit tasks that may have finished in the meantime. We need to decrement `it` because
      // it will be incremented when the loop iteration finishes.
      all_done = true;
      it = tasks.begin();
    }
    return all_done;
  };

  while (!all_own_tasks_done()) {
    // Run any job. This could be any job that is currently enqueued. Note: This job may internally call wait_for_tasks
    // again, in which case we would first wait for the inner task before the outer task has a chance to proceed.
    // We do not allow the worker to sleep here as we know that the passed task list is not yet fully processed.
    _work(false);
  }
}

void Worker::_set_affinity() {
#if HYRISE_NUMA_SUPPORT
  cpu_set_t cpuset;
  CPU_ZERO(&cpuset);
  CPU_SET(_cpu_id, &cpuset);
  const auto return_code = pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
  if (return_code != 0) {
    // This is not an Assert(), though maybe it should be. Not being able to pin the threads doesn't make the database
    // unfunctional, but probably slower.
    std::cerr << "Error calling pthread_setaffinity_np (return code: " << return_code << ")." << std::endl;
  }
#endif
}

}  // namespace hyrise
