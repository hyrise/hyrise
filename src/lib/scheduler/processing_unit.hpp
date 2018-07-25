#pragma once

#include <atomic>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

#include "types.hpp"

namespace opossum {

class UidAllocator;
class TaskQueue;
class Worker;

/**
 * Encapsulates the concept of a CPU. Mainly makes sure that there is always a Worker active per CPU, but only one of
 * these Workers is able to pull new tasks from the TaskQueue
 */
class ProcessingUnit final : public std::enable_shared_from_this<ProcessingUnit> {
 public:
  ProcessingUnit(const std::shared_ptr<TaskQueue>& queue, const std::shared_ptr<UidAllocator>& worker_id_allocator,
                 CpuID cpu_id);

  bool shutdown_flag() const;

  /**
   * In order to be allowed to pull new Tasks, a Worker must be the active worker, i.e. call this method with its id
   * and receive true from it.
   */
  bool try_acquire_active_worker_token(WorkerID worker_id);

  /**
   * If @worked_id owns the active worker token, it yields it, otherwise nothing happens.
   * It's okay for Workers to call this without actually owning the token, think of Tasks waiting for multiple
   * batches of jobs.
   */
  void yield_active_worker_token(WorkerID worker_id);

  /**
   * Put the Worker into hibernation state, which means it will only wake up when the Scheduler is shutting down or
   * when the ProcessingUnit needs a new worker to be active (i.e. when the currently active worker waits for jobs)
   */
  void hibernate_calling_worker();

  /**
   * When hibernated workers are available, wake one of them up. Otherwise create a new worker.
   */
  void wake_or_create_worker();

  /**
   * Increments the local counter of finished tasks to allow the Scheduler to determine whether all tasks finished.
   * Typically called by Workers
   */
  void on_worker_finished_task();

  /**
   * To be called by the Scheduler
   */
  void join();
  void shutdown();
  uint64_t num_finished_tasks() const;

 private:
  std::shared_ptr<TaskQueue> _queue;
  std::shared_ptr<UidAllocator> _worker_id_allocator;
  CpuID _cpu_id;
  std::mutex _mutex;  // Synchronizes access to _threads, _workers
  std::vector<std::thread> _threads;
  std::vector<std::shared_ptr<Worker>> _workers;
  std::atomic_bool _shutdown_flag{false};
  std::atomic<WorkerID> _active_worker_token{INVALID_WORKER_ID};
  std::mutex _hibernation_mutex;
  std::condition_variable _hibernation_cv;
  std::atomic_uint _num_hibernated_workers{0};
  std::atomic<uint64_t> _num_finished_tasks{0};
};
}  // namespace opossum
