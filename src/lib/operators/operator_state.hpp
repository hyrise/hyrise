#include <concepts>
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
 *   class MinValue<DataType> {
 *    public:
 *     void merge(MinValue<DataType>& other) {
 *       value = std::min(value, other.value);
 *     }
 *
 *     DataType value{std::numeric_limits<DataType>::max()};
 *   };
 *
 *   ...
 *
 *   auto jobs = std::vector<std::shared_ptr<AbstractTask>>{};
 *   auto operator_state = OperatorSharedState<MinValue<DataType>>{};
 *
 *   for (...) {
 *     jobs.emplace_back(std::make_shared<JobTask>([&](){
 *       auto& worker_state = operator_state.current_worker_state();
 *       segment_iterate(..., [&](const auto& position){
 *         if (!position->is_null()) {
 *           worker_state.value = std::min(worker_state.value, position->value());
 *         }
 *       });
 *     });
 *   }
 *
 *   Hyrise::get().scheduler()->schedule_and_wait_for_tasks(jobs);
 *   const auto min_value = operator_state.merge_worker_states().value;
 */

template <typename WorkerState>
concept HasMergeMethod = requires(WorkerState state) {
  WorkerState();                                 // WorkerState has default constructor for use with `std::make_unique`.
  { state.merge(state) } -> std::same_as<void>;  // WorkerState has `merge` method that takes same type, returns void.
};

template <HasMergeMethod WorkerState>
class OperatorSharedState : public Noncopyable {
 public:
  // Initializes a wrapper that is able to hold state for each worker.
  OperatorSharedState() {
    // Reserve one slot for the main thread if the scheduler is used.
    const auto scheduler = std::dynamic_pointer_cast<NodeQueueScheduler>(Hyrise::get().scheduler());
    _main_thread_worker_id = scheduler ? static_cast<WorkerID>(scheduler->workers().size()) : WorkerID{0};
    _worker_states.resize(_main_thread_worker_id + 1);
  }

  // Returns the state exclusive to the current worker. Initializes it if necessary.
  WorkerState& current_worker_state() {
    // There is no worker when ImmediateExecutionScheduler is used or when we run from the main thread. Make sure not to
    // interfere with the main thread by using the reseved slot in this case.
    const auto worker = Worker::get_this_thread_worker();
    const auto worker_id = worker ? worker->id() : _main_thread_worker_id;
    // The following assertion can only be violated if the topology/scheduler changed between initialization and now.
    Assert(worker_id < _worker_states.size(), std::format("Invalid worker ID #{}. There are only {} worker states.",
                                                          WorkerID::base_type{worker_id}, _worker_states.size()));

    // Create worker states only lazily because they could be large and we do not want to waste memory if we only have a
    // few jobs, but many workers.
    if (!_worker_states[worker_id]) {
      _worker_states[worker_id] = std::make_unique<WorkerState>();
    }
    return *_worker_states[worker_id];
  }

  // Combines all states into a single object, discards the intermediate results, and returns the combined result.
  WorkerState& merge_worker_states() {
    const auto result_state_it = std::ranges::find_if(_worker_states, [](const auto& state) {
      return state != nullptr;
    });
    Assert(result_state_it != _worker_states.end(), "Could not find a single initialized worker state.");

    // For now, just a simple linear merge. If merging is expensive, consider using a merge tree.
    auto& result_state = *result_state_it;
    for (auto it = std::next(result_state_it); it != _worker_states.end(); ++it) {
      if (*it) {
        result_state->merge(**it);
      }
    }

    _worker_states[0] = std::move(result_state);
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
