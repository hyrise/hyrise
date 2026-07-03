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

struct ChunkBatch {
  std::function<void()> function;
  size_t batch_row_count;
};

struct TaskBatchingResult {
  std::vector<ChunkBatch> chunk_batches;
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
TaskBatchingResult batch_chunks_for_scheduling(const std::shared_ptr<const Table>& table, const size_t minimal_group_row_count, Functor&& functor) {
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
  auto chunk_batches = std::vector<ChunkBatch>{};
  chunk_batches.reserve(group_count);

  auto chunk_ids = std::vector<ChunkID>{};
  chunk_ids.reserve(chunk_count);
  auto chunk_ids_start_offset = size_t{0};
  auto group_size = size_t{0};
  auto group_row_count = size_t{0};  // Track row count of current group.
  auto group_id = size_t{0};

  // The concurrent vector of chunks in a table does not allow us to use something like `views::chunk()`.
  for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
    chunk_ids.push_back(chunk_id);
    ++group_size;
    group_row_count += table->get_chunk(chunk_id)->size();

    // We emit when we have sufficent rows AND the group size is reached
    //
    // std::cout << group_row_count << " < " << minimal_group_row_count << std::endl;
    if ((group_row_count < minimal_group_row_count) || (group_size < tasks_per_group)) {
      continue;
    }
    // std::cout << "GRPUING\n";

    const auto first = chunk_ids.begin() + static_cast<std::ptrdiff_t>(chunk_ids_start_offset);
    const auto last = chunk_ids.end();
    chunk_batches.emplace_back([&, group_id, first, last, owned_functor]() {
      owned_functor(group_id, std::span(first, last));
    }, group_row_count);
    // group_row_counts.push_back()

    chunk_ids_start_offset = chunk_ids.size();
    group_size = 0;
    group_row_count = 0;
    ++group_id;
  }

  if (chunk_ids_start_offset < chunk_ids.size()) {
    const auto first = chunk_ids.begin() + static_cast<std::ptrdiff_t>(chunk_ids_start_offset);
    const auto last = chunk_ids.end();
    chunk_batches.emplace_back([&, group_id, first, last, owned_functor]() {
      owned_functor(group_id, std::span(first, last));
    }, group_row_count);
    ++group_id;
  }

  DebugAssert(group_id == chunk_batches.size(), "Group count unexpectedly deviates from number of created chunk_batches.");

  // No chunk_batches have been scheduled at this point. Scheduling is up to the caller. We further pass chunk_ids along
  // which needs to be kept alive as the chunk_batches reference ChunkIDs in this vector.
  return {.chunk_batches = std::move(chunk_batches), .chunk_ids = std::move(chunk_ids)};
}

}  // namespace hyrise
