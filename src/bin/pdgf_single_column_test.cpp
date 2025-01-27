#include <cstdint>
#include <iostream>
#include <cstdlib>
#include <set>
#include <memory>

#include "cxxopts.hpp"

#include "hyrise.hpp"
#include "import_export/data_generation/shared_memory_reader.hpp"
#include "import_export/data_generation/pdgf_process.hpp"
#include "storage/chunk.hpp"
#include "utils/assert.hpp"
#include "utils/format_duration.hpp"
#include "utils/timer.hpp"
#include "scheduler/node_queue_scheduler.hpp"

#define SHARED_MEMORY_NAME "/PDGF_SHARED_MEMORY"
#define DATA_READY_SEM "/PDGF_DATA_READY_SEM"
#define BUFFER_FREE_SEM "/PDGF_BUFFER_FREE_SEM"

using namespace hyrise;  // NOLINT(build/namespaces)

int main(int argc, char* argv[]) {
  auto cli_options = cxxopts::Options{""};
  cli_options.add_options()
      ("scale", "", cxxopts::value<float>())
      ("cores", "", cxxopts::value<uint32_t>())
      ("pdgf_num_cores", "", cxxopts::value<uint32_t>())
      ("pdgf_work_unit_size", "", cxxopts::value<uint32_t>())
      ("column", "", cxxopts::value<std::string>());
  cli_options.allow_unrecognised_options();
  const auto cli_parse_result = cli_options.parse(argc, argv);
  auto scale = cli_parse_result["scale"].as<float>();
  auto num_cores = cli_parse_result["cores"].as<uint32_t>();
  auto pdgf_num_cores = cli_parse_result["pdgf_num_cores"].as<uint32_t>();
  auto work_unit_size = cli_parse_result["pdgf_work_unit_size"].as<uint32_t>();
  auto column = cli_parse_result["column"].as<std::string>();

  auto shm_buffer_num_columns = 16u;

  // Set up scheduler
  Hyrise::get().topology.use_default_topology();
  const auto scheduler = std::make_shared<NodeQueueScheduler>(num_cores);
  Hyrise::get().set_scheduler(scheduler);

  // Run
  auto reader = create_shared_memory_reader(work_unit_size, shm_buffer_num_columns, Chunk::DEFAULT_SIZE, SHARED_MEMORY_NAME, DATA_READY_SEM, BUFFER_FREE_SEM);

  auto timer = Timer{};
  std::cerr << "Receiving table schemas from PDGF!\n";
  auto pdgf_schema = PdgfProcess::for_schema_generation(
    "pdgf-core_config_tpc-h-schema.xml", "default-shm-reflective-generation.xml", PDGF_DIRECTORY_ROOT,
    123456789, work_unit_size, pdgf_num_cores, shm_buffer_num_columns,
    scale);
  pdgf_schema.run();
  while (reader->has_next_table()) {
    auto schema_builder = reader->read_next_schema();
    // Directly add the (empty) table to the storage manager here.
    // This table will be replaced later once we have received data, but we will need the tables to be present in order for the optimizer to
    // be able to tell us which columns we need to generate for our _queries_to_run
    auto table = schema_builder->build_table();
    Hyrise::get().storage_manager.add_table(schema_builder->table_name(), table);
  }
  std::cerr << "Awaiting PDGF teardown\n";
  pdgf_schema.await_teardown();
  std::cout << "- Hyrise PDGF: Loading schema done (" << format_duration(timer.lap()) << ")\n" << std::flush;

  // IMPORTANT: reset reader between invocations
  reader->reset();

  auto pdgf_data = PdgfProcess::for_data_generation("pdgf-core_config_tpc-h-schema.xml", "default-shm-reflective-generation.xml", PDGF_DIRECTORY_ROOT, 123456789, work_unit_size, pdgf_num_cores, shm_buffer_num_columns, scale);
  auto column_filter = std::make_shared<std::set<std::string>>();
  column_filter->insert(column);
  pdgf_data.set_column_filter(column_filter);
  pdgf_data.run();
  auto encoding_config = EncodingConfig();
  while (reader->has_next_table()) {
    auto builder = reader->read_next_table(encoding_config, num_cores);
  }
  auto time = timer.lap();
  std::cerr << "Awaiting PDGF teardown\n";
  pdgf_data.await_teardown();
  std::cerr << "- Hyrise PDGF: Generating tables done (" << format_duration(time) << ")\n";

  std::cerr << "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n";

  // Teardown
  scheduler->finish();

  return 0;
}
