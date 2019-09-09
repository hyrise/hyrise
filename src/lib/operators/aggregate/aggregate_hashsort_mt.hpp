#pragma once

#include <vector>

#include "scheduler/abstract_task.hpp"
#include "scheduler/current_scheduler.hpp"

namespace opossum::aggregate_hashsort::mt {

using namespace opossum::aggregate_hashsort;

template <typename Run>
struct AbstractRunSource {
  virtual Run fetch_run() = 0;
};

template <typename Run>
class AggregateHashSortTask;

template<typename Run>
class AggregateHashSortOutputTask : public AbstractTask {
 public:
  AggregateHashSortOutputTask(const std::shared_ptr<AggregateHashSortSetup>& setup, Run run):
    setup(setup), run(std::move(run)) {
    setup->global_task_counter.fetch_add(1);
  }

  std::shared_ptr<AggregateHashSortSetup> setup;
  Run run;

 protected:
  void _on_execute() override {
    auto output_segments = Segments{setup->aggregate_definitions.size() + setup->offsets.size()};

    // Materialize the group-by columns
    for (auto column_id = ColumnID{0}; column_id < setup->offsets.size(); ++column_id) {
      const auto& output_column_definition = setup->output_column_definitions[column_id];
      resolve_data_type(output_column_definition.data_type, [&](const auto data_type_t) {
        using ColumnDataType = typename decltype(data_type_t)::type;
        output_segments[column_id] = run.template materialize_group_column<ColumnDataType>(setup,
                                                                                           column_id, output_column_definition.nullable);
      });
    }

    // Materialize the aggregate columns
    for (auto aggregate_idx = ColumnID{0}; aggregate_idx < setup->aggregate_definitions.size(); ++aggregate_idx) {
      output_segments[setup->offsets.size() + aggregate_idx] =
          run.aggregates[aggregate_idx]->materialize_output(run.size);
    }

    // Publish completed Chunk under lock
    {
      const auto output_chunks_lock = std::unique_lock{setup->output_chunks_mutex};
      setup->output_chunks.emplace_back(std::make_shared<Chunk>(output_segments));
    }

    /**
     * Check if this was the last task in the entire Aggregation that completed. If that's the case, notify the main
     * thread
     */
    if (CurrentScheduler::is_set()) {
      if (setup->global_task_counter.fetch_sub(1) == 1) {
        setup->done_condition.notify_all();
      }
    }
  }
};

template <typename Run>
class AggregateHashSortTaskSet {
 public:
  AggregateHashSortTaskSet(const std::shared_ptr<AggregateHashSortSetup>& setup,
                           const size_t level)
      : setup{setup}, level(0) {
  }

  void add_task(const std::shared_ptr<AggregateHashSortTask<Run>>& task) {
    _tasks.emplace_back(task);
    setup->global_task_counter.fetch_add(1);
    local_task_counter.fetch_add(1);
  }

  void schedule() {
    CurrentScheduler::schedule_tasks(_tasks);
  }

  std::shared_ptr<AggregateHashSortSetup> setup;
  size_t level;
  std::atomic_size_t local_task_counter;

 private:
  template<typename> friend class AggregateHashSortTask;

  std::vector<std::shared_ptr<AggregateHashSortTask<Run>>> _tasks;
};

template <typename Run>
class AggregateHashSortTask : public AbstractTask {
 public:
  AggregateHashSortTask(const std::shared_ptr<AggregateHashSortSetup>& setup,
      const std::shared_ptr<AggregateHashSortTaskSet<Run>>& task_set,
      const std::shared_ptr<AbstractRunSource<Run>>& run_source):
    setup(setup), task_set(task_set), run_source(run_source) { }

  std::shared_ptr<AggregateHashSortSetup> setup;
  std::shared_ptr<AggregateHashSortTaskSet<Run>> task_set;
  std::shared_ptr<AbstractRunSource<Run>> run_source;

 protected:
  void _on_execute() override {
    const auto input_run = run_source->fetch_run();

    auto partitions = adaptive_hashing_and_partition(setup, input_run);

    if (task_set->local_task_counter.fetch_sub(1) == 1) {
      /**
       * This task is the last in the task set to finish: For each partition, either start a recursive task or perform
       * materialization
       */

      const auto partition_count = partitions.size();

      for (auto partition_idx = size_t{0}; partition_idx < partition_count; ++partition_idx) {
        auto runs = std::vector<Run>{};
        for (auto& task : task_set->tasks()) {
          auto& partition = task->partitions[partition_idx];
          runs.insert(runs.end(), std::move_iterator(partition.runs.begin()), std::move_iterator(partition.runs.end()));
        }

        if (!runs.empty()) {
          if (runs.size() == 1 && runs.front().is_aggregated) {
            const auto output_task = std::make_shared<AggregateHashSortOutputTask>(setup, std::move(runs.front()));
            output_task->schedule();
          } else {
            auto sub_task_set = std::make_shared<AggregateHashSortTaskSet<Run>>(setup, task_set->level + 1);
            for (auto& output_run : runs) {
              const auto run_source = std::make_shared<RunSource<Run>>(std::move(run));
              sub_task_set->add_task(std::make_shared<AggregateHashSortTask<Run>>(setup, sub_task_set, run_source));
            }
            sub_task_set->schedule();

            CurrentScheduler::schedule_tasks(task_set->tasks);
          }
        }
      }

      /**
       * Check if this was the last task in the entire Aggregation that completed. If that's the case, notify the main
       * thread
       */
      if (CurrentScheduler::is_set()) {
        if (setup->global_task_counter.fetch_sub(1) == 1) {
          setup->done_condition.notify_all();
        }
      }
    }
  }
};

}  // namespace opossum::aggregate_hashsort::mt