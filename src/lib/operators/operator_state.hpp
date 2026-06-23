#include <cstddef>
#include <format>
#include <functional>
#include <memory>
#include <utility>
#include <vector>

#include "hyrise.hpp"
#include "scheduler/node_queue_scheduler.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace hyrise {

/**
 * A simple wapper around state that can be shared across tasks. Inside a task, you can obtain and use the current
 * worker's state without considering concurrency. When processing the tasks took place, the individual states can be
 * merged into a final result. Even for hundreds or thousands of tasks, the merge overhead is limited by the number of
 * workers.
 *
 * Usage example:
 *
 *   template<typename DataType>
 *   class MinValue<DataType> : public WorkerLocalState<MinValue<DataType>> {
 *    public:
 *     virtual void merge(MinValue<DataType>& other) final {
 *       value = std::min(value, other.value);
 *     }
 *
 *     DataType value{std::numeric_limits<DataType>::max()};
 *   };
 *
 *
 * Calling code:
 *
 *   auto jobs = std::vector<std::shared_ptr<AbstractTask>>{};
 *   auto operator_state = OperatorSharedState<MinValue<DataType>>{};
 *
 *   for (...) {
 *     jobs.emplace_back(std::make_shared<JobTask>([&](){
 *       auto& local_state = operator_state.current_worker_state();
 *       segment_iterate(..., [&](const auto& position){
 *         if (!position->is_null()) {
 *           local_state.value = std::min(local_state.value, position->value());
 *         }
 *       });
 *     });
 *   }
 *
 *   const auto min_value = operator_state.merge_worker_states().value;
 *
 */

template <typename Derived>
class WorkerLocalState : public Noncopyable {
 public:
  virtual void merge(Derived& other) = 0;
};

template <typename WorkerState>
class OperatorSharedState : public Noncopyable {
 public:
  OperatorSharedState() {
    const auto node_queue_scheduler = std::dynamic_pointer_cast<NodeQueueScheduler>(Hyrise::get().scheduler());
    const auto worker_count = node_queue_scheduler ? node_queue_scheduler->workers().size() : size_t{1};
    _worker_states.resize(worker_count);
  }

  WorkerState& current_worker_state() {
    const auto worker = Worker::get_this_thread_worker();
    const auto worker_id = worker ? worker->id() : WorkerID{0};
    Assert(worker_id < _worker_states.size(),
           std::format("Invalid worker ID #{}, has only {}.", WorkerID::base_type{worker_id}, _worker_states.size()));

    // Create local states only lazily because they could be large and we do not want to waste memory if we only have a
    // few jobs that are not distributed across all workers.
    auto& local_state = _worker_states[worker_id];
    if (!local_state) {
      local_state = std::make_unique<WorkerState>();
    }
    return *local_state;
  }

  WorkerState& merge_worker_states() {
    const auto merged_state_it = std::ranges::find_if(_worker_states, [](const auto& state) {
      return state != nullptr;
    });
    Assert(merged_state_it != _worker_states.end(), "Could not find a single initialized worker state.");

    // For now, just a simple linear merge. If merging is expensive, consider using a merge tree.
    auto& merged_state = *merged_state_it;
    for (auto it = std::next(merged_state_it); it != _worker_states.end(); ++it) {
      if (*it) {
        merged_state->merge(**it);
      }
    }

    _worker_states[0] = std::move(merged_state);
    _worker_states.resize(1);

    return *_worker_states[0];
  }

  std::vector<std::reference_wrapper<WorkerState>> worker_states() const {
    auto worker_states = std::vector<std::reference_wrapper<WorkerState>>{};
    worker_states.reserve(_worker_states.size());

    for (const auto& state : _worker_states) {
      if (state) {
        worker_states.emplace_back(*state);
      }
    }

    Assert(!worker_states.empty(), "Could not find a single initialized worker state.");

    return worker_states;
  }

 protected:
  std::vector<std::unique_ptr<WorkerState>> _worker_states;
};

}  // namespace hyrise
