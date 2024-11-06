#include <iostream>
#include <cstdlib>

#include "hyrise.hpp"
#include "import_export/data_generation/shared_memory_reader.hpp"
#include "import_export/data_generation/pdgf_process.hpp"
#include "utils/assert.hpp"
#include "utils/format_duration.hpp"
#include "utils/timer.hpp"
#include "scheduler/node_queue_scheduler.hpp"

#define SHARED_MEMORY_NAME "/PDGF_SHARED_MEMORY"
#define DATA_READY_SEM "/PDGF_DATA_READY_SEM"
#define BUFFER_FREE_SEM "/PDGF_BUFFER_FREE_SEM"

using namespace hyrise;  // NOLINT(build/namespaces)

int main(int argc, char* argv[]) {
  Assert(argc >= 5, "Expected scale factor and num cores arguments");
  Assert(strcmp(argv[1], "--scale") == 0, "First argument should be --scale");
  auto scale_factor = static_cast<float>(atof(argv[2]));
  Assert(strcmp(argv[3], "--scheduler") == 0, "Third argument should be --scheduler");
  Assert(strcmp(argv[4], "--data_preparation_cores") == 0, "Fourth argument should be --data_preparation_cores");
  auto num_cores = atoi(argv[5]);

  // Set up scheduler
  Hyrise::get().topology.use_default_topology(num_cores);
  const auto scheduler = std::make_shared<NodeQueueScheduler>(num_cores);
  Hyrise::get().set_scheduler(scheduler);

  // Run
  auto reader = SharedMemoryReader<128, 16>(Chunk::DEFAULT_SIZE, SHARED_MEMORY_NAME, DATA_READY_SEM, BUFFER_FREE_SEM);
  auto pdgf_schema = PdgfProcess::for_schema_generation(PDGF_DIRECTORY_ROOT, num_cores, scale_factor);
  pdgf_schema.run();
  while (reader.has_next_table()) {
    auto table_builder = reader.read_next_schema();
    Hyrise::get().storage_manager.add_table(table_builder->table_name(), table_builder->build_table());
  }
  pdgf_schema.await_teardown();

  reader.reset();

  auto timer = Timer{};
  auto pdgf_data = PdgfProcess::for_data_generation(PDGF_DIRECTORY_ROOT, num_cores, scale_factor);
  pdgf_data.run();
  auto tables = std::map<std::string, std::shared_ptr<Table>>{};
  while (reader.has_next_table()) {
    auto builder = reader.read_next_table(num_cores);
    tables[builder->table_name()] = builder->build_table();
  }
  auto time = timer.lap();
  pdgf_data.await_teardown();
  std::cerr << "Loading/Generating tables done (" << format_duration(time) << ")\n";

  std::cerr << "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n";

  // Teardown
  scheduler->finish();

  return 0;
}
