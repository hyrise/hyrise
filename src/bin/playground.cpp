#include <chrono>
#include <iostream>
#include <memory>

#include "scheduler/abstract_task.hpp"
#include "scheduler/job_task.hpp"
#include "types.hpp"

using namespace hyrise;  // NOLINT(build/namespaces)

struct Task : public std::enable_shared_from_this<Task> {
  explicit Task(int init_id) : id{init_id} {}

  Task(const Task& copy_task) {
    id = copy_task.id;
    successors = copy_task.successors;
    predecessors = copy_task.predecessors;
  }

  ~Task() {
    // std::cout << id << " destructed\n";

    // for (auto& predecessor : predecessors) {
    //   auto p = predecessor.lock();
    //   for (auto& s : p->successors) {
    //     s.reset();
    //   }
    // }

    // for (auto& successor : successors) {
    //   if (successor) {
    //     successor.reset();
    //   }
    // }
  }

  void add_successor(const std::shared_ptr<Task>& successor) {
    // successors.emplace_back(std::ref(successor));
    successors.emplace_back(successor);
    successor->predecessors.emplace_back(shared_from_this());
  }

  int id{};
  std::vector<std::weak_ptr<Task>> successors;
  // std::vector<std::reference_wrapper<const std::shared_ptr<Task>>> successors;
  // std::vector<std::shared_ptr<Task>> successors;
  std::vector<std::weak_ptr<Task>> predecessors;
};

int main() {
  auto start = std::chrono::steady_clock::now();
  {
    auto element_count = 100'000;
    // auto element_count = 40;
    auto tasks = std::vector<std::shared_ptr<Task>>{};

    for (auto i = 0; i < element_count; ++i) {
      tasks.emplace_back(std::make_shared<Task>(i));
    }

    // for (auto i = 0; i < element_count / 4; ++i) {
    //   tasks[i]->add_successor(tasks[element_count-i-1]);
    // }

    // My Hyrise PR
    for (auto i = 0; i < element_count - 10; ++i) {
      tasks[i]->add_successor(tasks[i + 10]);
    }

    // Hyrise Master
    // for (auto i = element_count - 1; i >= 10; --i) {
    //   tasks[i]->add_successor(tasks[i-10]);
    // }

    // for (auto i = 0; i < element_count; ++i) {
    //   std::cout << tasks[i].use_count() << "\n";
    // }

    // std::cerr << "\n\nDone\n\n";
    // start = std::chrono::steady_clock::now();
    // for (auto& task : tasks) {
    //   // task.reset();

    //   for (auto& successor : task->successors) {
    //     if (successor) {
    //       successor.reset();
    //     }
    //   }
    // }
    // tasks.clear();
  }
  const auto end = std::chrono::steady_clock::now();

  std::cerr << "\n\nRuntime " << std::chrono::duration<double>(end - start) * 1'000 << " ms\n\n";

  return 0;
}
