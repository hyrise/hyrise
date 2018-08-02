#include "processing_unit.hpp"

#include <functional>
#include <memory>

#include "uid_allocator.hpp"
#include "worker.hpp"

// It is important to limit the number of workers per core to avoid
// resource depletion. This number is an arbitrary pick, but has been reached
// and exceeded in experiments.
static constexpr size_t MAX_WORKERS_PER_CORE = 5;
static constexpr size_t MAX_WORKERS_PER_CORE_JOBTASKS = 1;

namespace opossum {

ProcessingUnit::ProcessingUnit(const std::shared_ptr<TaskQueue>& queue,
                               const std::shared_ptr<UidAllocator>& worker_id_allocator, CpuID cpu_id)
    : _queue(queue), _worker_id_allocator(worker_id_allocator), _cpu_id(cpu_id) {
  // Do not start worker yet, the object is still under construction and no shared_ptr of it is held right now -
  // shared_from_this will fail!
}

bool ProcessingUnit::try_acquire_active_worker_token(WorkerID worker_id) {
  // Are we already the active worker?
  auto expected_worked_id = worker_id;
  auto already_active = _active_worker_token.compare_exchange_strong(expected_worked_id, worker_id);
  if (already_active) {
    // The worker kept the token
    return true;
  }

  // No one else active?
  expected_worked_id = INVALID_WORKER_ID;
  auto no_one_active = _active_worker_token.compare_exchange_strong(expected_worked_id, worker_id);

  // no_one_active == true means the worker successfully acquired the token

  return no_one_active;
}

void ProcessingUnit::yield_active_worker_token(WorkerID worker_id) {
  _active_worker_token.compare_exchange_strong(worker_id, INVALID_WORKER_ID);
}

void ProcessingUnit::hibernate_calling_worker() {
  std::unique_lock<std::mutex> lock(_hibernation_mutex);

  _num_hibernated_workers++;

  _hibernation_cv.wait(lock, [&]() { return _shutdown_flag || _active_worker_token == INVALID_WORKER_ID; });

  _num_hibernated_workers--;
}

void ProcessingUnit::wake_or_create_worker() {
  if (_num_hibernated_workers == 0) {
    std::lock_guard<std::mutex> lock(_mutex);

    if (_workers.size() < MAX_WORKERS_PER_CORE) {
      auto worker = std::make_shared<Worker>(shared_from_this(), _queue, _worker_id_allocator->allocate(), _cpu_id,
                                             SchedulePriority::Lowest);
      _workers.emplace_back(worker);

      auto fn = std::bind(&Worker::operator(), worker.get());
      _threads.emplace_back(fn);
    } else if (_workers.size() < MAX_WORKERS_PER_CORE + MAX_WORKERS_PER_CORE_JOBTASKS) {
      auto worker = std::make_shared<Worker>(shared_from_this(), _queue, _worker_id_allocator->allocate(), _cpu_id,
                                             SchedulePriority::JobTask);
      _workers.emplace_back(worker);

      auto fn = std::bind(&Worker::operator(), worker.get());
      _threads.emplace_back(fn);
    }
  } else {
    std::unique_lock<std::mutex> lock(_hibernation_mutex);
    _hibernation_cv.notify_one();
  }
}

void ProcessingUnit::join() {
  for (auto& thread : _threads) {
    thread.join();
  }
}

void ProcessingUnit::shutdown() {
  {
    std::unique_lock<std::mutex> lock(_hibernation_mutex);
    _shutdown_flag = true;
  }
  _hibernation_cv.notify_all();
}

bool ProcessingUnit::shutdown_flag() const { return _shutdown_flag; }

void ProcessingUnit::on_worker_finished_task() { _num_finished_tasks++; }

uint64_t ProcessingUnit::num_finished_tasks() const { return _num_finished_tasks; }

}  // namespace opossum
