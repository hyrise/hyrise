#pragma once

#include <vector>

#include "scheduler/abstract_task.hpp"
#include "scheduler/current_scheduler.hpp"
#include "aggregate_hashsort_utils.hpp"

namespace opossum::aggregate_hashsort {

template <typename Run>
class AggregateHashSortTask;

/**
 * Task that turns a Run into a Chunk and adds it to the list of output Chunks
 * @tparam Run
 */
template<typename Run>
class AggregateHashSortOutputTask : public AbstractTask {
 public:
  AggregateHashSortOutputTask(const std::shared_ptr<AggregateHashSortEnvironment>& environment, Run run):
    environment(environment), run(std::move(run)) {
    environment->increment_global_task_counter();
  }

  std::shared_ptr<AggregateHashSortEnvironment> environment;
  Run run;

 protected:
  void _on_execute() override {
    auto output_segments = Segments{environment->aggregate_definitions.size() + environment->offsets.size()};

    // Materialize the group-by columns
    for (auto column_id = ColumnID{0}; column_id < environment->offsets.size(); ++column_id) {
      const auto& output_column_definition = environment->output_column_definitions[column_id];
      resolve_data_type(output_column_definition.data_type, [&](const auto data_type_t) {
        using ColumnDataType = typename decltype(data_type_t)::type;
        output_segments[column_id] = run.template materialize_group_column<ColumnDataType>(environment,
                                                                                           column_id, output_column_definition.nullable);
      });
    }

    // Materialize the aggregate columns
    for (auto aggregate_idx = ColumnID{0}; aggregate_idx < environment->aggregate_definitions.size(); ++aggregate_idx) {
      output_segments[environment->offsets.size() + aggregate_idx] =
          run.aggregates[aggregate_idx]->materialize_output(run.size);
    }

    // Publish completed Chunk under lock
    {
      const auto output_chunks_lock = std::unique_lock{environment->output_chunks_mutex};
      environment->output_chunks.emplace_back(std::make_shared<Chunk>(output_segments));
    }

    environment->decrement_global_task_counter();
  }
};

template <typename Run>
class AggregateHashSortTaskSet {
 public:
  AggregateHashSortTaskSet(const std::shared_ptr<AggregateHashSortEnvironment>& environment,
                           const size_t level)
      : environment{environment}, level(0) {
  }

  void add_task(const std::shared_ptr<AggregateHashSortTask<Run>>& task) {
    _tasks.emplace_back(task);
    local_task_counter.fetch_add(1);
    environment->increment_global_task_counter();
  }

  void schedule() {
    CurrentScheduler::schedule_tasks(_tasks);
  }

  std::shared_ptr<AggregateHashSortEnvironment> environment;

  // Number of tasks in this task set that are still active
  std::atomic_size_t local_task_counter;

  size_t level;

 private:
  template<typename> friend class AggregateHashSortTask;

  std::vector<std::weak_ptr<AggregateHashSortTask<Run>>> _tasks;
};

/**
 * Task that takes a number of input runs (in form of a AbstractRunSource) and hashes/partitions them
 */
template <typename Run>
class AggregateHashSortTask : public AbstractTask {
 public:
  AggregateHashSortTask(const std::shared_ptr<AggregateHashSortEnvironment>& environment,
      const std::shared_ptr<AggregateHashSortTaskSet<Run>>& task_set,
      const std::shared_ptr<AbstractRunSource<Run>>& run_source):
    environment(environment), task_set(task_set), run_source(run_source) { }

  std::shared_ptr<AggregateHashSortEnvironment> environment;

  // The tasks of a task set have shared ownership of the task set
  std::shared_ptr<AggregateHashSortTaskSet<Run>> task_set;

  std::shared_ptr<AbstractRunSource<Run>> run_source;

  std::vector<Partition<Run>> partitions;

 protected:
  void _on_execute() override {
    auto fan_out = RadixFanOut::for_level(task_set->level);

    partitions = adaptive_hashing_and_partition(environment, run_source, fan_out, task_set->level);

    if (task_set->local_task_counter.fetch_sub(1) == 1) {
      /**
       * This task is the last in the task set to finish: For each partition, either start a recursive task set or perform
       * materialization (if the run is fully aggregated)
       */

      const auto partition_count = fan_out.partition_count;
      for (auto partition_idx = size_t{0}; partition_idx < partition_count; ++partition_idx) {
        auto runs = std::vector<Run>{};

        for (auto& task : task_set->tasks()) {
          auto& partition = task->partitions[partition_idx];
          runs.insert(runs.end(), std::move_iterator(partition.runs.begin()), std::move_iterator(partition.runs.end()));
        }

        if (!runs.empty()) {
          if (runs.size() == 1 && runs.front().is_aggregated) {
            const auto output_task = std::make_shared<AggregateHashSortOutputTask>(environment, std::move(runs.front()));
            output_task->schedule();
          } else {
            auto sub_task_set = std::make_shared<AggregateHashSortTaskSet<Run>>(environment, task_set->level + 1);

            for (auto run_idx = size_t{0}; run_idx < runs.size();) {
              auto sub_task_runs = std::vector<Run>{};
              auto sub_task_group_count = size_t{0};

              for (; run_idx < runs.size(); ++run_idx) {
                sub_task_group_count += runs[run_idx].size;
                if (sub_task_group_count > environment->config.task_group_count_target) {
                  break;
                }
              }

              const auto sub_task_run_source = std::make_shared<RunSource<Run>>(std::move(runs));
              const auto sub_task = std::make_shared<AggregateHashSortTask<Run>>(environment, sub_task_set, sub_task_run_source);
              sub_task_set->add_task(sub_task);

              sub_task->schedule();
            }
          }
        }
      }
    }

    environment->decrement_global_task_counter();
  }
};


}  // namespace opossum::aggregate_hashsort