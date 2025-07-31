#include <algorithm>
#include <fstream>
#include <iostream>
#include <random>

// This playground only compiles on Linux as we require Linux's perf and perfetto.
#include "benchmark_config.hpp"
#include "hyrise.hpp"
#include "operators/get_table.hpp"
#include "operators/print.hpp"
#include "operators/sort.hpp"
#include "operators/table_wrapper.hpp"
#include "operators/trace_categories.hpp"
#include "perfcpp/event_counter.h"
#include "perfetto.h"
#include "scheduler/immediate_execution_scheduler.hpp"
#include "scheduler/job_task.hpp"
#include "scheduler/node_queue_scheduler.hpp"
#include "storage/chunk.hpp"
#include "tpcds/tpcds_table_generator.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

using namespace hyrise;  // NOLINT(build/namespaces)

static void silent_tpcds_table_generation(uint32_t scale_factor, std::shared_ptr<BenchmarkConfig> config) {
  auto* initial_buffer = std::cout.rdbuf();

  std::cout.rdbuf(nullptr);
  TPCDSTableGenerator(scale_factor, config).generate_and_store();
  std::cout.rdbuf(initial_buffer);
}

std::tuple<std::shared_ptr<Table>, std::shared_ptr<GetTable>, std::vector<SortColumnDefinition>>
setup_get_table_and_sort_definitions(const auto& table_name, const std::vector<std::string>& sort_column_names,
                                     const std::string& project_column_name) {
  auto& sm = Hyrise::get().storage_manager;

  auto table = sm.get_table(table_name);
  const auto column_count = table->column_count();
  auto unpruned_column_ids = std::vector<ColumnID>{};
  for (const auto& column_name : sort_column_names) {
    const auto column_id = table->column_id_by_name(column_name);
    unpruned_column_ids.emplace_back(column_id);
  }
  unpruned_column_ids.emplace_back(table->column_id_by_name(project_column_name));

  std::ranges::sort(unpruned_column_ids);
  auto all_column_ids = std::vector<ColumnID>(column_count);
  std::iota(all_column_ids.begin(), all_column_ids.end(), ColumnID{0});

  auto pruned_column_ids = std::vector<ColumnID>();
  std::ranges::set_difference(all_column_ids, unpruned_column_ids, std::back_inserter(pruned_column_ids));

  const auto get_table = std::make_shared<GetTable>(table_name, std::vector<ChunkID>{}, pruned_column_ids);
  get_table->never_clear_output();
  get_table->execute();

  auto sort_definitions = std::vector<SortColumnDefinition>{};
  for (const auto& column_name : sort_column_names) {
    const auto column_id = get_table->get_output()->column_id_by_name(column_name);
    sort_definitions.emplace_back(column_id);
  }

  return {table, get_table, sort_definitions};
}

/*
static void DuckDBTPCDS_C_Integers(const uint32_t scale_factor, const EncodingConfig encoding_config) {
  const auto node_queue_scheduler = std::make_shared<NodeQueueScheduler>();
  Hyrise::get().set_scheduler(node_queue_scheduler);

  auto benchmark_config = std::make_shared<BenchmarkConfig>();
  benchmark_config->cache_binary_tables = true;
  benchmark_config->encoding_config = encoding_config;

  silent_tpcds_table_generation(scale_factor, benchmark_config);

  const auto sort_columns = std::vector<std::string>{"c_birth_year", "c_birth_month", "c_birth_day"};
  const auto get_table_and_sort_definitions =
      setup_get_table_and_sort_definitions(std::string{"customer"}, sort_columns, std::string{"c_customer_sk"});
  const auto table = std::get<0>(get_table_and_sort_definitions);
  const auto get_table = std::get<1>(get_table_and_sort_definitions);
  const auto sort_definitions = std::get<2>(get_table_and_sort_definitions);

  auto sort = std::make_shared<Sort>(get_table, sort_definitions, Chunk::DEFAULT_SIZE, Sort::ForceMaterialization::Yes);
  sort->execute();

  node_queue_scheduler->finish();
}
*/

static void DuckDBTPCDS_CS(const uint32_t scale_factor, const EncodingConfig encoding_config) {
  const auto node_queue_scheduler = std::make_shared<NodeQueueScheduler>();
  Hyrise::get().set_scheduler(node_queue_scheduler);

  auto benchmark_config = std::make_shared<BenchmarkConfig>();
  benchmark_config->cache_binary_tables = true;
  benchmark_config->encoding_config = encoding_config;

  silent_tpcds_table_generation(scale_factor, benchmark_config);

  const auto sort_columns =
      std::vector<std::string>{"cs_warehouse_sk", "cs_ship_mode_sk", "cs_promo_sk", "cs_quantity"};
  // OpenMP/clang cause issues with structured bindings.
  const auto get_table_and_sort_definitions =
      setup_get_table_and_sort_definitions("catalog_sales", sort_columns, std::string{"cs_item_sk"});
  const auto table = std::get<0>(get_table_and_sort_definitions);
  const auto get_table = std::get<1>(get_table_and_sort_definitions);
  const auto sort_definitions = std::get<2>(get_table_and_sort_definitions);

  std::cout << "Size of table to sort: " << table->row_count() << " rows over " << table->chunk_count() << " chunks. "
            << std::endl;
  std::cout << "Sorting by " << sort_definitions.size() << " columns." << std::endl;
  std::cout << "Memory Usage: " << table->memory_usage(MemoryUsageCalculationMode::Sampled) << ".\n";

  auto sort = std::make_shared<Sort>(get_table, sort_definitions, Chunk::DEFAULT_SIZE, Sort::ForceMaterialization::Yes);
  sort->execute();

  node_queue_scheduler->finish();
}

// Initialize the Perfetto SDK and register your categories.
void InitializePerfetto() {
  auto args = perfetto::TracingInitArgs{};
  args.backends = perfetto::kInProcessBackend;
  perfetto::Tracing::Initialize(args);
  perfetto::TrackEvent::Register();
}

std::unique_ptr<perfetto::TracingSession> StartTracing() {
  auto track_event_cfg = perfetto::protos::gen::TrackEventConfig{};
  track_event_cfg.add_enabled_categories("Sort");

  auto cfg = perfetto::TraceConfig{};
  cfg.add_buffers()->set_size_kb(4096);
  auto* ds_cfg = cfg.add_data_sources()->mutable_config();
  ds_cfg->set_name("track_event");
  ds_cfg->set_track_event_config_raw(track_event_cfg.SerializeAsString());

  auto tracing_session = std::unique_ptr<perfetto::TracingSession>(perfetto::Tracing::NewTrace());
  tracing_session->Setup(cfg);
  tracing_session->StartBlocking();

  return tracing_session;
}

void StopTracing(std::unique_ptr<perfetto::TracingSession>& tracing_session) {
  tracing_session->StopBlocking();
  auto trace_data = std::vector<char>(tracing_session->ReadTraceBlocking());

  auto output = std::ofstream{};
  output.open("tpc-ds-cs.perfetto-trace", std::ios::out | std::ios::binary);
  output.write(&trace_data[0], trace_data.size());
  output.close();
}

int main(int argc, char* argv[]) {
  InitializePerfetto();
  auto tracing_session = StartTracing();

  const auto tpcds_scale_factor = static_cast<uint32_t>(std::stoi(argv[1]));
  // const auto tpcds_scale_factor = uint32_t{1};
  std::cout << "TPC-DS scale factor: " << tpcds_scale_factor << '\n';

  const auto encoding_config = EncodingConfig{SegmentEncodingSpec{EncodingType::Unencoded}};

  // DuckDBTPCDS_C_Integers(tpcds_scale_factor, encoding_config);
  DuckDBTPCDS_CS(tpcds_scale_factor, encoding_config);

  StopTracing(tracing_session);

  return 0;
}
