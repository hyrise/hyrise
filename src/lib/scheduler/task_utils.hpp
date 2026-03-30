#pragma once

#include <memory>
#include <queue>
#include <unordered_set>
#include <vector>

#include "hyrise.hpp"
#include "scheduler/abstract_task.hpp"
#include "scheduler/job_task.hpp"
#include "scheduler/node_queue_scheduler.hpp"
#include "utils/assert.hpp"

namespace hyrise {

enum class TaskVisitation { VisitPredecessors, DoNotVisitPredecessors };

/**
 * Calls the passed @param visitor on @param task and recursively on its PREDECESSORS. The visitor returns
 * `TaskVisitation`, indicating whether the current task's predecessors should be visited as well. The algorithm is
 * breadth-first search. Each task is visited exactly once.
 *
 * @tparam Visitor      Functor called with every task as a param. Returns `TaskVisitation`.
 */
template <typename Task, typename Visitor>
void visit_tasks(const std::shared_ptr<Task>& task, Visitor visitor) {
  using AbstractTaskType = std::conditional_t<std::is_const_v<Task>, const AbstractTask, AbstractTask>;

  auto task_queue = std::queue<std::shared_ptr<AbstractTaskType>>{};
  task_queue.push(task);

  auto visited_tasks = std::unordered_set<std::shared_ptr<AbstractTaskType>>{};

  while (!task_queue.empty()) {
    const auto current_task = task_queue.front();
    task_queue.pop();

    if (!visited_tasks.emplace(current_task).second) {
      continue;
    }

    if (visitor(current_task) == TaskVisitation::VisitPredecessors) {
      for (const auto& predecessor : current_task->predecessors()) {
        task_queue.push(predecessor.get().shared_from_this());
      }
    }
  }
}

enum class TaskUpwardVisitation { VisitSuccessors, DoNotVisitSuccessors };

/**
 * Calls the passed @param visitor on @param task and recursively on its SUCCESSORS. The visitor returns
 * `TaskUpwardVisitation`, indicating whether the current tasks's successors should be visited as well. Each node is
 * visited exactly once.
 *
 * @tparam Visitor      Functor called with every task as a param. Returns `TaskUpwardVisitation`.
 */
template <typename Task, typename Visitor>
void visit_tasks_upwards(const std::shared_ptr<Task>& task, Visitor visitor) {
  using AbstractTaskType = std::conditional_t<std::is_const_v<Task>, const AbstractTask, AbstractTask>;

  auto task_queue = std::queue<std::shared_ptr<AbstractTaskType>>{};
  task_queue.push(task);

  auto visited_tasks = std::unordered_set<std::shared_ptr<AbstractTaskType>>{};

  while (!task_queue.empty()) {
    const auto current_task = task_queue.front();
    task_queue.pop();

    if (!visited_tasks.emplace(current_task).second) {
      continue;
    }

    if (visitor(current_task) == TaskUpwardVisitation::VisitSuccessors) {
      for (const auto& successor : current_task->successors()) {
        task_queue.push(successor.get().shared_from_this());
      }
    }
  }
}

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
template <typename Container>
std::pair<size_t, std::vector<std::shared_ptr<AbstractTask>>> group_chunks_for_scheduling(const std::shared_ptr<Table>& table, const std::function<void(size_t, std::vector<ChunkID>)>& functor) {
  auto group_count = size_t{1};
  if (const auto node_queue_scheduler = std::dynamic_pointer_cast<NodeQueueScheduler>(Hyrise::get().scheduler())) {
    // We use a group count of twice the number of workers. This allows for some degree of straggler mitigation while
    // still not putting too much pressure on the scheduler.
    // Please note: If the load is high, the scheduler might further internally group tasks and execute them
    // sequentially.
    group_count = node_queue_scheduler->workers().size() * 2;
  }

  const auto chunk_count = table->chunk_count();
  const auto tasks_per_group = (chunk_count + group_count - 1) / group_count;  // Ceil of divison.
  auto jobs = std::vector<std::shared_ptr<AbstractTask>>{};
  jobs.reserve(group_count);

  auto task_items = std::vector<ChunkID>{};
  auto group_id = size_t{0};
  for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
    task_items.push_back(chunk_id);

    if (task_items.size() == tasks_per_group) {
      jobs.emplace_back(std::make_shared<JobTask>([&]() {
        functor(group_id, std::move(task_items));
      }));
      task_items = std::vector<ChunkID>{};
      ++group_id;
    }
    // The concurrent vector of chunks in a table does not allow us to use something like `views::chunk()`.
  }

  if (!task_items.empty()) {
    jobs.emplace_back(std::make_shared<JobTask>([&]() {
      functor(group_id, std::move(task_items));
    }));
  }

  // No jobs have been scheduled at this point. It is up to the caller.
  Assert(jobs.size() == group_count, "Number of jobs should equal group count.");
  return {group_count, jobs};
}

}  // namespace hyrise
