#include "processing_unit.hpp"

#include <functional>
#include <memory>

#include "uid_allocator.hpp"
#include "worker.hpp"

namespace opossum {

ProcessingUnit::ProcessingUnit(const std::shared_ptr<TaskQueue>& queue,
                               const std::shared_ptr<UidAllocator>& worker_id_allocator, CpuID cpuID)
    : _queue(queue), _worker_id_allocator(worker_id_allocator), _cpuID(cpuID) {
  // Do not start worker yet, the object is still under construction and no shared_ptr of it is held right now -
  // shared_from_this will fail!
}

bool ProcessingUnit::try_acquire_active_worker_token(WorkerID workerID) {
  // Are we already the active worker?
  auto o_workerID = workerID;
  auto already_active = _active_worker_token.compare_exchange_strong(o_workerID, workerID);
  if (already_active) {
    // The worker kept the token
    return true;
  }

  // No one else active?
  o_workerID = INVALID_WORKER_ID;
  auto no_one_active = _active_worker_token.compare_exchange_strong(o_workerID, workerID);

  // no_one_active == true means the worker successfully acquired the token

  return no_one_active;
}

void ProcessingUnit::yield_active_worker_token(WorkerID workerID) {
  _active_worker_token.compare_exchange_strong(workerID, INVALID_WORKER_ID);
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

    auto worker = std::make_shared<Worker>(shared_from_this(), _queue, _worker_id_allocator->allocate(), _cpuID);
    _workers.emplace_back(worker);

    auto fn = std::bind(&Worker::operator(), worker.get());
    _threads.emplace_back(fn);
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
}  // namespace opossum
