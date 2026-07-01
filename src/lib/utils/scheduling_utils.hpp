#pragma once

#include <algorithm>
#include <memory>
#include <utility>
#include <vector>

#include <boost/container/small_vector.hpp>

#include "hyrise.hpp"
#include "scheduler/abstract_task.hpp"
#include "scheduler/job_task.hpp"
#include "scheduler/node_queue_scheduler.hpp"
#include "storage/table.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace hyrise {

struct TaskBatchingResult {
  std::vector<std::shared_ptr<AbstractTask>> jobs;
  std::vector<ChunkID> chunk_ids;
};

/**
 * EXPLAIN THAT NORMALLY, SCAN-LIKE PARALLELIZATION IS FINE ...
 *
 * For parallelizable processes that do not require any coordination between tasks, we can simply issue one task per
 * chunk. But if some form of coordination is required, spawning thousands of small tasks for large tables can be
 * problematic.
 * One example is the merging of per-chunk histograms as done in the hash join's radix partitioning. Initially, we
 * created one histogram per chunk which later all needed to be merged. This coordination after all jobs are done can be
 * significantly reduced, when chunks are grouped and thus less histograms need to be merged.
 * Spawning just enough tasks to ensure parallelization but minimizing coordination can be advantageous.
 *
 * Please note, task grouping within the scheduler is an orthogonal topic and can still apply when the grouped jobs are
 * scheduled.
 */
template <typename Functor>
TaskBatchingResult batch_chunks_for_scheduling(const std::shared_ptr<const Table>& table,
                                                                       Functor&& functor) {
  const auto chunk_count = table->chunk_count();
  Assert(chunk_count > 0, "Tables without any chunks must be handled by the caller.");

  auto owned_functor = std::forward<Functor>(functor);

  // If Hyrise is running single-threaded, we do not gain anything by grouping chunks but increase the coordination
  // overhead afterwards. Thus, we use a single large group for all chunks. However, for testing we use one group per
  // chunk to make sure that the coordination afterwards is properly tested.
  auto group_count = HYRISE_DEBUG ? size_t{chunk_count} : size_t{1};
  if (const auto node_queue_scheduler = std::dynamic_pointer_cast<NodeQueueScheduler>(Hyrise::get().scheduler())) {
    // For the group count, we use the number of workers, which should allow sufficient parallelization not putting too
    // much pressure on the scheduler. Please note: If the load is high, the scheduler might further internally group
    // tasks and execute them sequentially.
    group_count = node_queue_scheduler->workers().size();
  }
  group_count = std::min(group_count, size_t{chunk_count});

  const auto tasks_per_group = (chunk_count + group_count - 1) / group_count;  // Ceil of divison.
  auto jobs = std::vector<std::shared_ptr<AbstractTask>>{};
  jobs.reserve(group_count);

  // auto group_row_counts = std::vector<size_t>{};
  // group_row_counts.reserve(group_count);

  auto chunk_ids = std::vector<ChunkID>{};
  chunk_ids.reserve(chunk_count);
  auto chunk_ids_start_offset = size_t{0};
  auto group_size = size_t{0};
  // auto group_row_count = size_t{0};  // Track row count of current group.
  auto group_id = size_t{0};

  // The concurrent vector of chunks in a table does not allow us to use something like `views::chunk()`.
  for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
    chunk_ids.push_back(chunk_id);
    ++group_size;
    // group_row_count += table->get_chunk(chunk_id)->size();

    if (group_size < tasks_per_group) {
      continue;
    }

    const auto first = chunk_ids.begin() + static_cast<std::ptrdiff_t>(chunk_ids_start_offset);
    const auto last = chunk_ids.end();
    jobs.emplace_back(std::make_shared<JobTask>([&, group_id, first, last, owned_functor]() {
      owned_functor(group_id, std::span(first, last));
    }));
    // group_row_counts.push_back()

    chunk_ids_start_offset = chunk_ids.size();
    group_size = 0;
    ++group_id;
  }

  if (chunk_ids_start_offset < chunk_ids.size()) {
    const auto first = chunk_ids.begin() + static_cast<std::ptrdiff_t>(chunk_ids_start_offset);
    const auto last = chunk_ids.end();
    jobs.emplace_back(std::make_shared<JobTask>([&, group_id, first, last, owned_functor]() {
      owned_functor(group_id, std::span(first, last));
    }));
    ++group_id;
  }

  DebugAssert(group_id == jobs.size(), "Group count unexpectedly deviates from number of created jobs.");

  // No jobs have been scheduled at this point. Scheduling is up to the caller. We further pass chunk_ids along which
  // needs to be kept alive as the jobs reference ChunkIDs in this vector.
  return {.jobs = std::move(jobs), .chunk_ids = std::move(chunk_ids)};
}

}  // namespace hyrise
