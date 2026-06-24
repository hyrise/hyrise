#include <cstddef>
#include <format>
#include <functional>
#include <memory>
#include <utility>
#include <vector>
#include <concepts>

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
 *   class MinValue<DataType> : public AbstractWorkerState<MinValue<DataType>> {
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
 *   Hyrise::get().scheduler()->schedule_and_wait_for_tasks(jobs);
 *   const auto min_value = operator_state.merge_worker_states().value;
 */

template <typename Derived>
class AbstractWorkerState : public Noncopyable {
 public:
  virtual void merge(Derived& other) = 0;
};

template <typename WorkerState>
requires(std::derived_from<WorkerState, AbstractWorkerState<WorkerState>>)
class OperatorSharedState : public Noncopyable {
 public:
  // Initializes a wrapper that is able to hold state for each worker.
  OperatorSharedState() {
    const auto node_queue_scheduler = std::dynamic_pointer_cast<NodeQueueScheduler>(Hyrise::get().scheduler());

    // Reserve one slot for the main thread if the scheduler is used.
    _main_thread_worker_id =
        node_queue_scheduler ? static_cast<WorkerID>(node_queue_scheduler->workers().size()) : WorkerID{0};
    _worker_states.resize(_main_thread_worker_id + 1);
  }

  // Returns the state exclusive to the current worker. Initializes it if necessary.
  WorkerState& current_worker_state() {
    // There is no worker when ImmediateExecutionScheduler is used or when we run from the main thread. Make sure not to
    // interfere with the main thread by using the reseved slot in this case.
    const auto worker = Worker::get_this_thread_worker();
    const auto worker_id = worker ? worker->id() : _main_thread_worker_id;
    // The follwoing assertion can only be violated if the topology changed between initialization and now.
    Assert(worker_id < _worker_states.size(), std::format("Invalid worker ID #{}. There are only {} worker states.",
                                                          WorkerID::base_type{worker_id}, _worker_states.size()));

    // Create local states only lazily because they could be large and we do not want to waste memory if we only have a
    // few jobs, but many workers.
    if (!_worker_states[worker_id]) {
      _worker_states[worker_id] = std::make_unique<WorkerState>();
    }
    return *_worker_states[worker_id];
  }

  // Combines all states into a single object, discards the intermediate results, and returns the combined result.
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

  // Returns all individual state objects, e.g., for manual merge.
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
  WorkerID _main_thread_worker_id;
};

}  // namespace hyrise
