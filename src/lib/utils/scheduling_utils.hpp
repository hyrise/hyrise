#pragma once

#include <memory>
#include <vector>

#include <boost/container/small_vector.hpp>

#include "hyrise.hpp"
#include "scheduler/abstract_task.hpp"
#include "scheduler/job_task.hpp"
#include "scheduler/node_queue_scheduler.hpp"
#include "utils/assert.hpp"

namespace hyrise {

/**
 * For parallelizable processes that do not require any coordination between tasks, we can simply issue one task per
 * partition (e.g., chunks of a table). But if some form of coordination is required, spawning thousands of small tasks
 * for large tables can be problematic (task grouping within the scheduler is orthogonal). Here, spawning just enough
 * tasks (by grouping partitions into tasks) to ensure parallelization but minimizing coordination can be advantageous.
 *
 * One example is the local building of Bloom filters per chunk which are then merged into a global one. Instead of
 * having thousands of jobs merging, local Bloom filters are created for multiple chunks and coordination (here,
 * merging of Bloom filters) is significantly reduced.
 */
template <typename Functor>
std::pair<size_t, std::vector<std::shared_ptr<AbstractTask>>> group_chunks_for_scheduling(const std::shared_ptr<const Table>& table, const Functor& functor) {
  const auto chunk_count = table->chunk_count();
  Assert(chunk_count > 0, "Tables without any chunks must be handled by the caller.");

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
    // The concurrent vector of chunks in a table does not allow us to use something like `views::chunk()`.
  }

  if (!task_items->empty()) {
    jobs.emplace_back(std::make_shared<JobTask>([&, group_id, task_items]() {
      functor(group_id, std::move(task_items));
    }));
    ++group_id;
  }

  // No jobs have been scheduled at this point. It is up to the caller.
  return {group_id, jobs};
}

}  // namespace hyrise

