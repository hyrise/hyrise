#pragma once

#include <queue>
#include <unordered_set>

#include "scheduler/abstract_task.hpp"

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
    const auto task = task_queue.front();
    task_queue.pop();

    if (!visited_tasks.emplace(task).second) {
      continue;
    }

    if (visitor(task) == TaskVisitation::VisitPredecessors) {
      for (const auto& predecessor_ref : task->predecessors()) {
        const auto predecessor = predecessor_ref.lock();
        Assert(predecessor, "Predecessor expired.");
        task_queue.push(predecessor);
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
        task_queue.push(successor);
      }
    }
  }
}

}  // namespace hyrise
