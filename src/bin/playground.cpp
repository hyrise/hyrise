#include <iostream>

#include "types.hpp"
#include "scheduler/current_scheduler.hpp"
#include "scheduler/node_queue_scheduler.hpp"
#include "scheduler/topology.hpp"
#include "scheduler/operator_task.hpp"

// #include "storage/storage_manager.hpp"
#include "sql/sql_pipeline_builder.hpp"
#include "tpch/tpch_db_generator.hpp"
#include "tpch/tpch_queries.hpp"
#include "spdlog/spdlog.h"
#include "spdlog/sinks/stdout_color_sinks.h"

using namespace opossum;  // NOLINT

int main() {
  spdlog::set_level(spdlog::level::debug);
  spdlog::set_level(spdlog::level::info);
  spdlog::set_pattern("[%H:%M:%S.%f] [%l] [thread %^%t%$] %v");
  auto console = spdlog::stdout_color_mt("console");
  console->info("Welcome to spdlog!");

  auto sql = tpch_queries.at(3);

  auto iterations = 1000;
  auto scale = 0.001f;

  Topology::use_numa_topology(1);
  // Topology::get().print();
  CurrentScheduler::set(std::make_shared<NodeQueueScheduler>());

  console->info("Numer of iterations: {}", iterations);
  console->info("Generating TPC-H tables scale {}", scale);
  console->debug("DEBUG");
  opossum::TpchDbGenerator{scale}.generate_and_store();

  auto tasks = std::vector<std::shared_ptr<OperatorTask>>();

  for (auto i = 0 ; i < iterations ; ++i) {
    auto pipeline = SQLPipelineBuilder{sql}.with_mvcc(UseMvcc::No).create_pipeline();

    for (auto task_set : pipeline.get_tasks()) {
      for (auto task : task_set) {
        tasks.emplace_back(std::move(task));
      }
    }
  }

  std::cout << " > Scheduling Tasks (TPCH-3)" << std::endl;
  CurrentScheduler::schedule_and_wait_for_tasks(tasks);

  std::cout << "Done." << std::endl;
  return 0;
}
