#pragma once

#include <vector>

#include "aggregate_hashsort_utils.hpp"
#include "scheduler/abstract_task.hpp"
#include "scheduler/current_scheduler.hpp"

#define VERBOSE_MT VERBOSE && 0

namespace opossum::aggregate_hashsort {

template <typename Run>
class AggregateHashSortTask;

/**
 * Task that turns a Run into a Chunk and adds it to the list of output Chunks
 * @tparam Run
 */
template <typename Run>
class AggregateHashSortOutputTask : public AbstractTask {
 public:
  AggregateHashSortOutputTask(const std::shared_ptr<AggregateHashSortEnvironment>& environment, Run run)
      : environment(environment), run(std::move(run)) {
    environment->increment_global_task_counter();
  }

  std::shared_ptr<AggregateHashSortEnvironment> environment;
  Run run;

 protected:
  void _on_execute() override {
#if VERBOSE_MT
    std::cout << "AggregateHashSortOutputTask: " << run.size << " rows" << std::endl;
#endif

    auto output_segments = Segments{environment->aggregate_definitions.size() + environment->offsets.size()};

    // Materialize the group-by columns
    for (auto column_id = ColumnID{0}; column_id < environment->offsets.size(); ++column_id) {
      const auto& output_column_definition = environment->output_column_definitions[column_id];
      resolve_data_type(output_column_definition.data_type, [&](const auto data_type_t) {
        using ColumnDataType = typename decltype(data_type_t)::type;
        output_segments[column_id] = run.template materialize_group_column<ColumnDataType>(
            *environment, column_id, output_column_definition.nullable);
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

#if VERBOSE_MT
    std::cout << "AggregateHashSortOutputTask: Done" << std::endl;
#endif
  }
};

template <typename Run>
class AggregateHashSortTaskSet {
 public:
  AggregateHashSortTaskSet(const std::shared_ptr<AggregateHashSortEnvironment>& environment, const size_t level)
      : environment{environment}, level(level) {}

  void add_task(const std::shared_ptr<AggregateHashSortTask<Run>>& task) {
    _tasks.emplace_back(task);
    _partitions.emplace_back();
    local_task_counter.fetch_add(1);
    environment->increment_global_task_counter();
  }

  std::shared_ptr<AggregateHashSortEnvironment> environment;

  // Number of tasks in this task set that are still active
  std::atomic_size_t local_task_counter{0};

  size_t level;

 private:
  template <typename>
  friend class AggregateHashSortTask;

  std::vector<std::weak_ptr<AggregateHashSortTask<Run>>> _tasks;

  // Tasks write their output here: _partitions[<task_idx>][<partition_idx>]
  std::vector<std::vector<Partition<Run>>> _partitions;
};

/**
 * Task that takes a number of input runs (in form of a AbstractRunSource) and hashes/partitions them
 */
template <typename Run>
class AggregateHashSortTask : public AbstractTask {
 public:
  AggregateHashSortTask(const std::shared_ptr<AggregateHashSortEnvironment>& environment, const size_t task_idx,
                        const std::shared_ptr<AggregateHashSortTaskSet<Run>>& task_set,
                        const std::shared_ptr<AbstractRunSource<Run>>& run_source)
      : environment(environment), task_idx(task_idx), task_set(task_set), run_source(run_source) {}

  std::shared_ptr<AggregateHashSortEnvironment> environment;

  // Index of the task within its TaskSet
  const size_t task_idx;

  // The tasks of a task set have shared ownership of the task set
  std::shared_ptr<AggregateHashSortTaskSet<Run>> task_set;

  std::shared_ptr<AbstractRunSource<Run>> run_source;

 protected:
  void _on_execute() override {
#if VERBOSE_MT
    std::cout << "AggregateHashSortTask: level=" << task_set->level << "; task_idx=" << task_idx << "; Started"
              << std::endl;
#endif

    auto fan_out = RadixFanOut::for_level(task_set->level);
    auto input_runs = run_source->fetch_runs();

    task_set->_partitions[task_idx] =
        adaptive_hashing_and_partition(*environment, std::move(input_runs), fan_out, task_set->level);

    if (task_set->local_task_counter.fetch_sub(1) == 1) {
#if VERBOSE_MT
      std::cout << "AggregateHashSortTask: level=" << task_set->level << "; task_idx=" << task_idx
                << "; Starting follow up tasks" << std::endl;
#endif
      /**
       * This task is the last in the task set to finish: For each partition, either start a recursive task set or perform
       * materialization (if the run is fully aggregated)
       */

      const auto partition_count = fan_out.partition_count;
      for (auto partition_idx = size_t{0}; partition_idx < partition_count; ++partition_idx) {
        auto partition_runs = std::vector<Run>{};

        for (auto& task_partitions : task_set->_partitions) {
          auto& partition = task_partitions[partition_idx];
          partition_runs.insert(partition_runs.end(), std::move_iterator(partition.runs.begin()), std::move_iterator(partition.runs.end()));
        }

        if (!partition_runs.empty()) {
          if (partition_runs.size() == 1 && partition_runs.front().is_aggregated) {
            const auto output_task =
                std::make_shared<AggregateHashSortOutputTask<Run>>(environment, std::move(partition_runs.front()));
            output_task->schedule();
          } else {
            auto sub_task_set = std::make_shared<AggregateHashSortTaskSet<Run>>(environment, task_set->level + 1);
            auto sub_tasks = std::vector<std::shared_ptr<AggregateHashSortTask<Run>>>{};

            for (size_t run_idx{0}, sub_task_idx{0}; run_idx < partition_runs.size(); ++sub_task_idx) {
              auto sub_task_runs = std::vector<Run>{};
              auto sub_task_group_count = size_t{0};

              while (run_idx < partition_runs.size()) {
                sub_task_group_count += partition_runs[run_idx].size;
                sub_task_runs.emplace_back(std::move(partition_runs[run_idx]));

                // Increased here, instead of in loop head, to make sure run_idx is incremented even if the break
                // below is triggered
                ++run_idx;

                // Not a loop condition to make sure at least one run, no matter how big, makes it into the sub task
                if (sub_task_group_count > environment->config.task_group_count_target) {
                  break;
                }
              }

              const auto sub_task_run_source = std::make_shared<ForwardingRunSource<Run>>(std::move(sub_task_runs));
              const auto sub_task = std::make_shared<AggregateHashSortTask<Run>>(environment, sub_task_idx,
                                                                                 sub_task_set, sub_task_run_source);
              sub_task_set->add_task(sub_task);
              sub_tasks.emplace_back(sub_task);
            }

            CurrentScheduler::schedule_tasks(sub_tasks);
          }
        }
      }
    }

    environment->decrement_global_task_counter();
  }
};

}  // namespace opossum::aggregate_hashsort