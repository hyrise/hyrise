#include <iostream>

#include "scheduler/abstract_task.hpp"
#include "scheduler/job_task.hpp"
#include "types.hpp"

using namespace hyrise;  // NOLINT(build/namespaces)

struct Task {
  Task(int init_id) : id{init_id} {
    std::cout << i << " constructed\n";
  }

  Task(const Task& copy_task) {
    id = copy_task.id;
    successor = copy_task.successor;
  }

  ~Task() {
    std::cout << i << " destructed\n";
    successor.reset();
  }

  void set_successor(const std::shared_ptr<Task>& init_successor) {
    successor = init_successor;
  }

  int id{};
  std::shared_ptr<Task> successor;
};

int main() {
  auto element_count = 40;
  auto tasks = std::vector<std::shared_ptr<Task>>{};

  for (auto i = 0; i < element_count; ++i) {
    tasks.emplace_back(std::make_shared<Task>(i));
  }

  // for (auto i = 0; i < element_count / 4; ++i) {
  //   tasks[i]->set_successor(tasks[element_count-i-1]);
  // }
  
  for (auto i = 1; i < element_count; ++i) {
    if (i % 10 == 0) {
      continue;
    }
    tasks[i-1]->set_successor(tasks[i]);
  }

  std::cerr << "\n\nDone\n\n";

  return 0;
}
