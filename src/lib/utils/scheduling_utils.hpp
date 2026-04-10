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
#include "utils/assert.hpp"

namespace hyrise {

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
std::vector<std::shared_ptr<AbstractTask>> group_chunks_for_scheduling(const std::shared_ptr<const Table>& table,
                                                                       const Functor functor) {
  const auto chunk_count = table->chunk_count();
  Assert(chunk_count > 0, "Tables without any chunks must be handled by the caller.");

  // If Hyrise is running single-threaded, we do not gain anything for grouping chunks but increase the coordination
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

  // auto task_items = std::make_shared<std::vector<ChunkID>>();
  auto task_items = std::make_shared<boost::container::small_vector<ChunkID, 1>>();
  task_items->reserve(tasks_per_group);
  auto group_id = size_t{0};

  // The concurrent vector of chunks in a table does not allow us to use something like `views::chunk()`.
  for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
    task_items->push_back(chunk_id);

    if (task_items->size() == tasks_per_group) {
      jobs.emplace_back(std::make_shared<JobTask>([&, group_id, task_items]() {
        functor(group_id, std::move(task_items));
      }));
      // task_items = std::make_shared<std::vector<ChunkID>>();
      task_items = std::make_shared<boost::container::small_vector<ChunkID, 1>>();
      task_items->reserve(tasks_per_group);
      ++group_id;
    }
  }

  if (!task_items->empty()) {
    jobs.emplace_back(std::make_shared<JobTask>([&, group_id, task_items]() {
      functor(group_id, std::move(task_items));
    }));
    ++group_id;
  }

  DebugAssert(group_id == jobs.size(), "Group count unexpectedly deviates from number of created jobs.");

  // No jobs have been scheduled at this point. It is up to the caller.
  return jobs;
}

}  // namespace hyrise
