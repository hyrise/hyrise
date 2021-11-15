#include <chrono>
#include <iostream>
#include <thread>

#include "hyrise.hpp"
#include "scheduler/node_queue_scheduler.hpp"
#include "scheduler/job_task.hpp"

using namespace opossum;  // NOLINT
using namespace std::chrono_literals;

int main() {
  setbuf(stdout, NULL);

  std::printf("Playground start.\n");
  const auto job_count = Hyrise::get().topology.num_cpus();
  std::printf("Working with %zu jobs/threads.\n", job_count);

  Hyrise::get().topology.use_non_numa_topology();
  Hyrise::get().set_scheduler(std::make_shared<NodeQueueScheduler>());

  std::printf("Scheduler is set.\n");

  std::printf("Sleep?.\n");
  std::this_thread::sleep_for(2200ms);
  std::printf("Sleep? \n");

  auto jobs = std::vector<std::shared_ptr<AbstractTask>>{};
  for (auto job_id = size_t{0}; job_id < job_count; ++job_id) {
    jobs.emplace_back(std::make_shared<JobTask>([&]() {
      std::printf("Task\n");
    }));  
  }

  const auto begin = std::chrono::high_resolution_clock::now();
  Hyrise::get().scheduler()->schedule_and_wait_for_tasks(jobs);
  const auto end = std::chrono::high_resolution_clock::now();

  std::printf("Elapsed time: %zu mus\n", static_cast<size_t>(std::chrono::duration_cast<std::chrono::microseconds>(end - begin).count()));

  Hyrise::get().scheduler()->finish();

  return 0;
}
