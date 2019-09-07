#pragma once

#include <vector>

#include "scheduler/abstract_task.hpp"

namespace opossum::aggregate_hashsort::mt {

using namespace opossum::aggregate_hashsort;

template <typename Run>
struct AbstractRunSource {
  virtual const std::vector<Run>& fetch_runs() = 0;
};

template <typename Run>
class AggregateHashSortTask;

template <typename Run>
class AggregateHashSortTaskSet {
 public:
  AggregateHashSortTaskSet(AggregateHashSortSetup& setup,
                           const std::vector<std::shared_ptr<AggregateHashSortTask<Run>>>& tasks)
      : setup{setup}, tasks{tasks}, local_task_counter{tasks.size()} {
    setup.global_task_counter.fetch_add(tasks.size());
  }

  AggregateHashSortSetup& setup;
  std::vector<std::shared_ptr<AggregateHashSortTask<Run>>>& tasks;
  std::atomic_size_t local_task_counter;
};

template <typename Run>
class AggregateHashSortTask : public AbstractTask {
 protected:
  void _on_execute() override {
    const auto& runs = _run_source->fetch_runs();
    partitions = adaptive_hashing_and_partition(setup, runs, )

        if (_task_set->local_task_counter.fetch_sub(1) == 1) {
      const auto partition_count = _partitions.size();

      for (auto partition_idx = size_t{0}; partition_idx < partition_count; ++partition_idx) {
        auto runs = std::vector<Run>{};

        for (auto& task : _task_set->tasks) {
          auto& partition = task->partitions[partition_idx];
          runs.insert(runs.end(), std::move_iterator(partition.runs.begin()), std::move_iterator(partition.runs.end()))
        }

        if (!runs.empty()) {
          if (runs.size() == 1 && runs.front().is_aggregated) {
            _result_runs->emplace_back(std::move(runs.front()))
          } else {
            const auto run_source = ... const auto task_set = std::make_shared<AggregateTaskSet>(run_source);
            task_set->schedule();
          }
        }
      }
    }

    if (setup.global_task_counter.fetch_sub(1) == 1) {
      setup.done_condition.notify_all();
    }
  }
};

}  // namespace opossum::aggregate_hashsort::mt