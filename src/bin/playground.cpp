#include <iostream>

#include "scheduler/abstract_task.hpp"
#include "scheduler/job_task.hpp"
#include "types.hpp"

using namespace hyrise;  // NOLINT(build/namespaces)

int main() {
  std::vector<std::reference_wrapper<const std::shared_ptr<AbstractTask>>> _successors;

  const std::shared_ptr<AbstractTask> task = std::make_shared<JobTask>([]() { return 0; });

  _successors.emplace_back(std::ref(task));
  return 0;
}
