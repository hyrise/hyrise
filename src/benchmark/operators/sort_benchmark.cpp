#include <memory>

#include "benchmark_config.hpp"
#include "expression/expression_functional.hpp"
#include "hyrise.hpp"
#include "logical_query_plan/lqp_translator.hpp"
#include "micro_benchmark_basic_fixture.hpp"
#include "micro_benchmark_utils.hpp"
#include "operators/get_table.hpp"
#include "operators/limit.hpp"
#include "operators/sort.hpp"
#include "operators/table_wrapper.hpp"
#include "scheduler/node_queue_scheduler.hpp"
#include "sql/sql_pipeline_builder.hpp"
#include "sql/sql_translator.hpp"
#include "synthetic_table_generator.hpp"
#include "tpcds/tpcds_table_generator.hpp"

namespace hyrise {

const auto INT_TO_ENCODING_CONFIG =
    std::vector<EncodingConfig>{EncodingConfig{SegmentEncodingSpec{EncodingType::Dictionary}},
                                EncodingConfig{SegmentEncodingSpec{EncodingType::Unencoded}},
                                EncodingConfig{SegmentEncodingSpec{EncodingType::LZ4}}};

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
  auto sort_definitions = std::vector<SortColumnDefinition>{};
  for (const auto& column_name : sort_column_names) {
    const auto column_id = table->column_id_by_name(column_name);
    sort_definitions.emplace_back(column_id);
    unpruned_column_ids.emplace_back(column_id);
  }
  unpruned_column_ids.emplace_back(table->column_id_by_name(project_column_name));

  std::ranges::sort(unpruned_column_ids);
  auto all_column_ids = std::vector<ColumnID>(column_count);
  std::iota(all_column_ids.begin(), all_column_ids.end(), ColumnID{0});
  auto pruned_column_ids = std::vector<ColumnID>(column_count);

  const auto [begin, end] = std::ranges::set_difference(all_column_ids, unpruned_column_ids, pruned_column_ids.begin());
  pruned_column_ids.erase(begin, end);

  const auto get_table = std::make_shared<GetTable>(table_name, std::vector<ChunkID>{}, pruned_column_ids);
  get_table->never_clear_output();
  get_table->execute();

  return {table, get_table, sort_definitions};
}

static std::shared_ptr<Table> generate_custom_table(const size_t row_count, const DataType data_type = DataType::Int,
                                                    const float null_ratio = 0.0f) {
  const auto table_generator = std::make_shared<SyntheticTableGenerator>();

  constexpr auto NUM_COLUMNS = 2;
  constexpr auto LARGEST_VALUE = 10'000;

  const auto column_data_types = std::vector<DataType>{NUM_COLUMNS, data_type};

  auto column_specifications = std::vector<ColumnSpecification>(
      NUM_COLUMNS, ColumnSpecification(ColumnDataDistribution::make_uniform_config(0.0, LARGEST_VALUE), data_type,
                                       SegmentEncodingSpec{EncodingType::Unencoded}, std::nullopt, null_ratio));

  return table_generator->generate_table(column_specifications, row_count);
}

static void BM_Sort(benchmark::State& state, const size_t row_count = 40'000, const DataType data_type = DataType::Int,
                    const float null_ratio = 0.0f, const bool multi_column_sort = true,
                    const bool use_reference_segment = false) {
  micro_benchmark_clear_cache();

  const auto input_table = generate_custom_table(row_count, data_type, null_ratio);
  std::shared_ptr<AbstractOperator> input_operator = std::make_shared<TableWrapper>(input_table);
  input_operator->never_clear_output();
  input_operator->execute();
  if (use_reference_segment) {
    input_operator = std::make_shared<Limit>(input_operator,
                                             expression_functional::to_expression(std::numeric_limits<int64_t>::max()));
    input_operator->never_clear_output();
    input_operator->execute();
  }

  auto sort_definitions = std::vector<SortColumnDefinition>{};
  if (multi_column_sort) {
    sort_definitions =
        std::vector<SortColumnDefinition>{{SortColumnDefinition{ColumnID{0}, SortMode::AscendingNullsFirst},
                                           SortColumnDefinition{ColumnID{1}, SortMode::DescendingNullsFirst}}};
  } else {
    sort_definitions =
        std::vector<SortColumnDefinition>{SortColumnDefinition{ColumnID{0}, SortMode::AscendingNullsFirst}};
  }

  for (auto _ : state) {
    auto sort = std::make_shared<Sort>(input_operator, sort_definitions);
    sort->execute();
  }
}

static void BM_Sort(benchmark::State& state) {
  const size_t row_count = state.range(0);
  BM_Sort(state, row_count);
}

static void BM_SortTwoColumns(benchmark::State& state) {
  const size_t row_count = state.range(0);
  BM_Sort(state, row_count, DataType::Int, 0.0f, true);
}

static void BM_SortWithNullValues(benchmark::State& state) {
  const size_t row_count = state.range(0);
  BM_Sort(state, row_count, DataType::Int, 0.2f);
}

static void BM_SortWithReferenceSegments(benchmark::State& state) {
  const size_t row_count = state.range(0);
  BM_Sort(state, row_count, DataType::Int, 0.0f, false, true);
}

static void BM_SortWithReferenceSegmentsTwoColumns(benchmark::State& state) {
  const size_t row_count = state.range(0);
  BM_Sort(state, row_count, DataType::Int, 0.0f, true, true);
}

static void BM_SortWithStrings(benchmark::State& state) {
  const size_t row_count = state.range(0);
  BM_Sort(state, row_count, DataType::String);
}

static void BM_SortDuckDBTPCDS_CS(benchmark::State& state) {
  const auto node_queue_scheduler = std::make_shared<NodeQueueScheduler>();
  Hyrise::get().set_scheduler(node_queue_scheduler);

  const auto scale_factor = static_cast<uint32_t>(state.range(0));
  auto& sm = Hyrise::get().storage_manager;

  auto benchmark_config = std::make_shared<BenchmarkConfig>();
  benchmark_config->cache_binary_tables = true;
  benchmark_config->encoding_config = INT_TO_ENCODING_CONFIG[state.range(1)];

  silent_tpcds_table_generation(scale_factor, benchmark_config);
  auto cs_table = sm.get_table("catalog_sales");
  const auto cs_table_wrapper = std::make_shared<TableWrapper>(cs_table);
  cs_table_wrapper->never_clear_output();
  cs_table_wrapper->execute();

  auto sort_definitions = std::vector<SortColumnDefinition>{};
  const auto sort_columns =
      std::vector<std::string>{"cs_warehouse_sk", "cs_ship_mode_sk", "cs_promo_sk", "cs_quantity"};
  for (const auto& column_name : sort_columns) {
    sort_definitions.emplace_back(cs_table->column_id_by_name(column_name));
  }

  std::cout << "Size of table to sort: " << cs_table->row_count()
            << ". Memory Usage: " << cs_table->memory_usage(MemoryUsageCalculationMode::Sampled) << ".\n";

  for (auto _ : state) {
    auto sort = std::make_shared<Sort>(cs_table_wrapper, sort_definitions);
    sort->execute();
  }

  node_queue_scheduler->finish();
}

static void BM_SortDuckDBTPCDS_C_Strings(benchmark::State& state) {
  const auto node_queue_scheduler = std::make_shared<NodeQueueScheduler>();
  Hyrise::get().set_scheduler(node_queue_scheduler);

  const auto scale_factor = static_cast<uint32_t>(state.range(0));

  auto benchmark_config = std::make_shared<BenchmarkConfig>();
  benchmark_config->cache_binary_tables = true;
  benchmark_config->encoding_config = INT_TO_ENCODING_CONFIG[state.range(1)];

  silent_tpcds_table_generation(scale_factor, benchmark_config);

  const auto sort_columns = std::vector<std::string>{"c_last_name", "c_first_name"};
  auto [table, get_table, sort_definitions] =
      setup_get_table_and_sort_definitions("customer", sort_columns, std::string{"c_customer_sk"});

  std::cout << "Size of table to sort: " << table->row_count()
            << ". Memory Usage: " << table->memory_usage(MemoryUsageCalculationMode::Sampled) << ".\n";

  for (auto _ : state) {
    auto sort =
        std::make_shared<Sort>(get_table, sort_definitions, Chunk::DEFAULT_SIZE, Sort::ForceMaterialization::Yes);
    sort->execute();
  }

  node_queue_scheduler->finish();
}

static void BM_SortDuckDBTPCDS_C_Integers(benchmark::State& state) {
  const auto node_queue_scheduler = std::make_shared<NodeQueueScheduler>();
  Hyrise::get().set_scheduler(node_queue_scheduler);

  const auto scale_factor = static_cast<uint32_t>(state.range(0));

  auto benchmark_config = std::make_shared<BenchmarkConfig>();
  benchmark_config->cache_binary_tables = true;
  benchmark_config->encoding_config = INT_TO_ENCODING_CONFIG[state.range(1)];

  silent_tpcds_table_generation(scale_factor, benchmark_config);

  const auto sort_columns = std::vector<std::string>{"c_birth_year", "c_birth_month", "c_birth_day"};
  auto [table, get_table, sort_definitions] =
      setup_get_table_and_sort_definitions(std::string{"customer"}, sort_columns, std::string{"c_customer_sk"});

  std::cout << "Size of table to sort: " << table->row_count()
            << ". Memory Usage: " << table->memory_usage(MemoryUsageCalculationMode::Sampled) << ".\n";

  for (auto _ : state) {
    auto sort =
        std::make_shared<Sort>(get_table, sort_definitions, Chunk::DEFAULT_SIZE, Sort::ForceMaterialization::Yes);
    sort->execute();
  }

  node_queue_scheduler->finish();
}

BENCHMARK(BM_Sort)->RangeMultiplier(100)->Range(100, 1'000'000);
BENCHMARK(BM_SortTwoColumns)->RangeMultiplier(100)->Range(100, 1'000'000);
BENCHMARK(BM_SortWithNullValues)->RangeMultiplier(100)->Range(100, 1'000'000);
BENCHMARK(BM_SortWithReferenceSegments)->RangeMultiplier(100)->Range(100, 1'000'000);
BENCHMARK(BM_SortWithReferenceSegmentsTwoColumns)->RangeMultiplier(100)->Range(100, 1'000'000);
BENCHMARK(BM_SortWithStrings)->RangeMultiplier(100)->Range(100, 1'000'000);

BENCHMARK(BM_SortDuckDBTPCDS_CS)->ArgsProduct({{1, 10, 100}, {0, 1, 2}});

BENCHMARK(BM_SortDuckDBTPCDS_C_Strings)->Arg(100)->Arg(300);
BENCHMARK(BM_SortDuckDBTPCDS_C_Integers)->Arg(100)->Arg(300);

}  // namespace hyrise
