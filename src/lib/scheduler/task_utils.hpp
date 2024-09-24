#pragma once

#include <functional>
#include <memory>
#include <queue>
#include <unordered_set>

#include "scheduler/abstract_task.hpp"

struct RefWapperAddressHash {
  std::size_t operator()(hyrise::AbstractTask const& obj) const {
    std::hash<hyrise::AbstractTask const*> theHash{};
    return theHash(&obj);
  }
};

struct RefWapperAddressEqual {
  bool operator()(hyrise::AbstractTask const& obj1, hyrise::AbstractTask const& obj2) const {
    return &obj1 == &obj2;
  }
};

namespace std {
template <>
struct hash<hyrise::AbstractTask> {
  size_t operator()(const hyrise::AbstractTask& key) const {
    return hash<const hyrise::AbstractTask*>()(&key);
  }
};

// template<typename T>
// struct hash<std::reference_wrapper<T>>
// {
//     std::size_t operator()(const std::reference_wrapper<T>& ref) const {
//       return std::hash<T&>{}(ref.get());
//     }
// };

template <typename T>
bool operator==(const std::reference_wrapper<T>& left, const std::reference_wrapper<T>& right) {
  return &left.get() == &right.get();
}

}  // namespace std

namespace hyrise {

enum class TaskVisitation { VisitPredecessors, DoNotVisitPredecessors };

// template<typename T>
// struct ref_equals {
//   bool operator()(const T &x, const T &y) const {
//       return &x.get() < &y.get();
//   }
// };

// struct ref_hash {
//   bool operator()(const std::reference_wrapper<const AbstractTask>& x) const {
//       return std::hash<const AbstractTask&>{}(x.get());
//   }
// };

/**
 * Calls the passed @param visitor on @param task and recursively on its PREDECESSORS. The visitor returns
 * `TaskVisitation`, indicating whether the current task's predecessors should be visited as well. The algorithm is
 * breadth-first search. Each task is visited exactly once.
 *
 * @tparam Visitor      Functor called with every task as a param. Returns `TaskVisitation`.
 */
template <typename Task, typename Visitor>
void visit_tasks(const Task& task, Visitor visitor) {
  // using AbstractTaskType = std::conditional_t<std::is_const_v<Task>, const AbstractTask, AbstractTask>;

  auto task_queue = std::queue<std::reference_wrapper<const AbstractTask>>{};
  task_queue.push(std::ref(task));

  auto visited_tasks = std::unordered_set<std::reference_wrapper<const AbstractTask>, RefWapperAddressHash>{};

  while (!task_queue.empty()) {
    const auto current_task = task_queue.front();
    task_queue.pop();

    if (!visited_tasks.emplace(current_task).second) {
      continue;
    }

    if (visitor(current_task.get()) == TaskVisitation::VisitPredecessors) {
      for (const auto& predecessor_ref : current_task.get().predecessors()) {
        const auto predecessor = predecessor_ref.lock();
        Assert(predecessor, "Predecessor expired.");
        task_queue.push(std::ref(*predecessor));
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
void visit_tasks_upwards(const Task& task, Visitor visitor) {
  // using AbstractTaskType = std::conditional_t<std::is_const_v<Task>, const AbstractTask, AbstractTask>;

  // auto task_queue = std::queue<const AbstractTask*>{};
  // task_queue.push(&task);
  auto task_queue = std::queue<std::reference_wrapper<const AbstractTask>>{};
  task_queue.push(std::ref(task));

  // auto visited_tasks = std::unordered_set<const AbstractTask*>{};
  auto visited_tasks = std::unordered_set<std::reference_wrapper<const AbstractTask>, RefWapperAddressHash>{};

  while (!task_queue.empty()) {
    const auto current_task = task_queue.front();
    task_queue.pop();

    if (!visited_tasks.emplace(current_task).second) {
      continue;
    }

    if (visitor(current_task.get()) == TaskUpwardVisitation::VisitSuccessors) {
      for (const auto& successor : current_task.get().successors()) {
        task_queue.push(successor.get());
      }
    }
  }
}

// /**
//  * Calls the passed @param visitor on @param task and recursively on its SUCCESSORS. The visitor returns
//  * `TaskUpwardVisitation`, indicating whether the current tasks's successors should be visited as well. Each node is
//  * visited exactly once.
//  *
//  * @tparam Visitor      Functor called with every task as a param. Returns `TaskUpwardVisitation`.
//  */
// template <typename Task, typename Visitor>
// void visit_tasks_upwards(const Task& task, Visitor visitor) {
//   // using AbstractTaskType = std::conditional_t<std::is_const_v<Task>, const AbstractTask, AbstractTask>;

//   auto task_queue = std::queue<std::reference_wrapper<const AbstractTask>>{};
//   task_queue.push(std::ref(task));

//   auto visited_tasks = std::unordered_set<std::reference_wrapper<const AbstractTask>>{};

//   while (!task_queue.empty()) {
//     const auto current_task = task_queue.front();
//     task_queue.pop();

//     if (!visited_tasks.emplace(current_task).second) {
//       continue;
//     }

//     if (visitor(current_task) == TaskUpwardVisitation::VisitSuccessors) {
//       for (const auto& successor : current_task.get().successors()) {
//         task_queue.push(successor.get());
//       }
//     }
//   }
// }

}  // namespace hyrise
