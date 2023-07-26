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

#include <boost/predef.h>
#include <boost/atomic/detail/pause.hpp>

#include "abstract_scheduler.hpp"
#include "abstract_task.hpp"
#include "hyrise.hpp"
#include "task_queue.hpp"

namespace {

using namespace hyrise;  // NOLINT(build/namespaces)

// Uses a pause instruction to signal the CPU that the process is spinning to wait, which can be more energy efficient
// and puts less pressure on the cache than a pause-less spinning loop (see
// https://www.intel.com/content/www/us/en/developer/articles/technical/a-common-construct-to-avoid-the-contention-of-threads-architecture-agnostic-spin-wait-loops.html).
// Instructions differ between ARM and x86. Tested on recent x86 CPUs (AMD and Intel) as well as ARM-based Apple M2 and
// Raspberry Pi 4.
// void spin_wait() {
// #if BOOST_ARCH_ARM
//     __asm__ __volatile__("yield");
// #elif BOOST_ARCH_X86_64
//     _mm_pause();
// #endif
// }

/**
 * On worker threads, this references the Worker running on this thread, on all other threads, this is empty.
 * Uses a weak_ptr, because otherwise the ref-count of it would not reach zero within the main() scope of the program.
 */
thread_local std::weak_ptr<Worker> this_thread_worker;  // NOLINT (clang-tidy wants this const)

}  // namespace

namespace hyrise {

std::shared_ptr<Worker> Worker::get_this_thread_worker() {
  return ::this_thread_worker.lock();
}

Worker::Worker(const std::shared_ptr<TaskQueue>& queue, WorkerID worker_id, CpuID cpu_id)
    : _queue(queue), _id(worker_id), _cpu_id(cpu_id) {
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

  while (Hyrise::get().scheduler()->active() && _active) {
    // Worker is allowed to sleep (when queue is empty) as long as the scheduler is not shutting down.
    _work(AllowSleep::Yes);
  }
}

void Worker::_work(const AllowSleep allow_sleep) {
  const auto& queues = Hyrise::get().scheduler()->queues();
  // Optional to only fetch current time when needed later.
  auto first_sleep_try = std::optional<std::chrono::steady_clock::time_point>{};

  auto task = std::shared_ptr<AbstractTask>{};
  auto spin_count = int64_t{-1};

  // auto break_at_next_task = false;
  // auto break_at_local_task = false;
  // auto break_at_distant_task = false;
  // auto break_at_no_sleep = false;
  // auto break_at_wake = false;

  while (true) {
    ++spin_count;

    if (_next_task) {
      task = std::move(_next_task);
      _next_task = nullptr;
      // break_at_next_task = true;
      break;
    } else {
      if (_queue->semaphore.tryWait()) {
        task = _queue->pull();
        // break_at_local_task = true;
        break;
      }
    }

    // if (spin_count < 4 && allow_sleep == AllowSleep::Yes) {
    //   // Do not immediately steal tasks but first spin a few times on the local queue. In case the worker is not
    //   // allowed to sleep, directly try to steal.
    //   continue;
    // }

    // No workable task on local queue: try to steal from other queues.
    for (const auto& queue : queues) {
      if (queue == _queue) {
        continue;
      }

      if (queue->semaphore.tryWait()) {
        task = queue->steal();
        if (task) {
          task->set_node_id(_queue->node_id());
          // break_at_distant_task = true;
          break;
        }
      }
    }

    if (task || allow_sleep == AllowSleep::No) {
      // Exit loop when task successfully pulled from distant queue or when worker is not allowed to sleep or spin (see
      // _work() call in _wait_for_tasks()).
      // break_at_no_sleep = true;
      break;
    }

    // Exponential back off. Do not spin more than 1024 times.
    const auto spin_loop_count = std::min(1024, 1 << std::min(int64_t{10}, spin_count));
    DebugAssert(spin_loop_count >= 0 && spin_loop_count < 1025, "Unexpected spin count (over-/underflow?).");
    // std::printf("Worker id %zu spins %zu times.\n", static_cast<size_t>(_id), static_cast<size_t>(spin_loop_count));
    for (auto loop_id = int32_t{0}; loop_id < spin_loop_count; ++loop_id) {
      // spin_wait();
      boost::atomics::detail::pause();
    }

    if (spin_count < 16) {
      // Continue spinning without checking for the overall spin time if spin count is small.
      continue;
    }

    if (!first_sleep_try) {
      // Set start time here to avoid obtaining the clock in the spinning loop unnecessarily.
      first_sleep_try = std::chrono::steady_clock::now();
    }

    const auto time_passed_spinning = std::chrono::nanoseconds{std::chrono::steady_clock::now() - *first_sleep_try};
    if (time_passed_spinning.count() > 10'000'000) {
      // We only yield the worker thread after we have spinned for 10 ms.
      // std::printf("Worker id %zu goes to sleep. Spin count is %zu and we waited %zu ns.\n", static_cast<size_t>(_id), static_cast<size_t>(spin_count), static_cast<size_t>(time_passed_spinning.count()));
      _queue->semaphore.wait();
      // std::printf("Worker id %zu awakes. Spin count is %zu and we waited %zu ns.\n", static_cast<size_t>(_id), static_cast<size_t>(spin_count), static_cast<size_t>(time_passed_spinning.count()));
      task = _queue->pull();
      // break_at_wake = true;
      break;
    }
    // std::printf("Worker id %zu spin count: %zu.\n", static_cast<size_t>(_id), static_cast<size_t>(spin_count));
  }

  if (!task) {
    // std::printf("Worker id %zu returns with spin count of: %zu (%d, %d, %d, %d, %d, sleep: %s).\n", static_cast<size_t>(_id), static_cast<size_t>(spin_count), break_at_next_task, break_at_local_task, break_at_distant_task, break_at_no_sleep, break_at_wake, std::string{magic_enum::enum_name(allow_sleep)}.c_str());
    return;
  }

  // std::printf("Worker id %zu processes task with a spin count of: %zu (%d, %d, %d, %d, %d, sleep: %s).\n", static_cast<size_t>(_id), static_cast<size_t>(spin_count), break_at_next_task, break_at_local_task, break_at_distant_task, break_at_no_sleep, break_at_wake, std::string{magic_enum::enum_name(allow_sleep)}.c_str());

  const auto successfully_assigned = task->try_mark_as_assigned_to_worker();
  if (!successfully_assigned) {
    // Some other worker has already started to work on this task - pick a different one.
    return;
  }

  task->execute();

  if (task->is_shutdown_task()) {
    _active = false;
  }

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
    _work(AllowSleep::No);
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
