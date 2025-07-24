#include <algorithm>
#include <fstream>
#include <iostream>
#include <random>

#include "benchmark_config.hpp"
#include "hyrise.hpp"
#include "operators/get_table.hpp"
#include "operators/print.hpp"
#include "operators/sort.hpp"
#include "operators/table_wrapper.hpp"
#include "scheduler/immediate_execution_scheduler.hpp"
#include "scheduler/job_task.hpp"
#include "scheduler/node_queue_scheduler.hpp"
#include "storage/chunk.hpp"
#include "tpcds/tpcds_table_generator.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

using namespace hyrise;  // NOLINT(build/namespaces)

constexpr auto RUN_COUNT = size_t{10} + 1;
constexpr auto FILENAME = "RESULTS.csv";

const auto ENCODING_CONFIGS = std::vector<EncodingConfig>{
    EncodingConfig{SegmentEncodingSpec{EncodingType::Dictionary}},
    EncodingConfig{SegmentEncodingSpec{EncodingType::Unencoded}}
    // EncodingConfig{SegmentEncodingSpec{EncodingType::LZ4}}  // Too slow.
};

static void silent_tpcds_table_generation(uint32_t scale_factor, std::shared_ptr<BenchmarkConfig> config) {
  auto* initial_buffer = std::cout.rdbuf();

  std::cout.rdbuf(nullptr);
  TPCDSTableGenerator(scale_factor, config).generate_and_store();
  std::cout.rdbuf(initial_buffer);
}

void append_to_csv(const std::string& benchmark, const size_t scale, const std::string& encoding,
                   const std::vector<size_t>& runtimes, const std::string note = "") {
  auto out_file = std::ofstream(FILENAME, std::ios::app);

  auto run_id = size_t{0};
  for (const auto& runtime : runtimes) {
    out_file << std::format("\"{}\",{},\"{}\",\"{}\",{},{}\n", benchmark, scale, encoding, note, run_id, runtime);
    ++run_id;
  }
}

void append_to_csv(const std::string& benchmark, const size_t scale, const EncodingConfig& encoding_config,
                   const std::vector<size_t>& runtimes, const std::string note = "") {
  auto sstream = std::stringstream{};
  sstream << encoding_config.default_encoding_spec;
  append_to_csv(benchmark, scale, sstream.str(), runtimes, note);
}

std::pair<size_t, std::vector<size_t>> measure_runtime(const size_t run_count, const std::function<void()> function) {
  auto runtimes = std::vector<size_t>{};
  runtimes.reserve(run_count);

  for (auto run_id = size_t{0}; run_id < run_count; ++run_id) {
    const auto start = std::chrono::steady_clock::now();
    function();
    const auto end = std::chrono::steady_clock::now();

    if (run_id > 0) {
      runtimes.push_back(std::chrono::duration_cast<std::chrono::microseconds>(end - start).count());
    }
  }

  auto result = size_t{0};
  for (const auto runtime : runtimes) {
    result += runtime;
  }

  return {result, runtimes};
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

  const auto [runtime_sum, runtimes] = measure_runtime(RUN_COUNT, [&]() {
    auto sort =
        std::make_shared<Sort>(get_table, sort_definitions, Chunk::DEFAULT_SIZE, Sort::ForceMaterialization::Yes);
    sort->execute();
  });

  append_to_csv("TPC-DS_CatalogSales", scale_factor, encoding_config, runtimes, "multi-threaded");

  node_queue_scheduler->finish();
}

static void DuckDBTPCDS_C_Strings(const uint32_t scale_factor, const EncodingConfig encoding_config) {
  const auto node_queue_scheduler = std::make_shared<NodeQueueScheduler>();
  Hyrise::get().set_scheduler(node_queue_scheduler);

  auto benchmark_config = std::make_shared<BenchmarkConfig>();
  benchmark_config->cache_binary_tables = true;
  benchmark_config->encoding_config = encoding_config;

  silent_tpcds_table_generation(scale_factor, benchmark_config);

  const auto sort_columns = std::vector<std::string>{"c_last_name", "c_first_name"};
  const auto get_table_and_sort_definitions =
      setup_get_table_and_sort_definitions("customer", sort_columns, std::string{"c_customer_sk"});
  const auto table = std::get<0>(get_table_and_sort_definitions);
  const auto get_table = std::get<1>(get_table_and_sort_definitions);
  const auto sort_definitions = std::get<2>(get_table_and_sort_definitions);

  const auto [runtime_sum, runtimes] = measure_runtime(RUN_COUNT, [&]() {
    auto sort =
        std::make_shared<Sort>(get_table, sort_definitions, Chunk::DEFAULT_SIZE, Sort::ForceMaterialization::Yes);
    sort->execute();
  });

  append_to_csv("TPC-DS_Customer_Strings", scale_factor, encoding_config, runtimes, "multi-threaded");

  node_queue_scheduler->finish();
}

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

  const auto [runtime_sum, runtimes] = measure_runtime(RUN_COUNT, [&]() {
    auto sort =
        std::make_shared<Sort>(get_table, sort_definitions, Chunk::DEFAULT_SIZE, Sort::ForceMaterialization::Yes);
    sort->execute();
  });

  append_to_csv("TPC-DS_Customer_Integers", scale_factor, encoding_config, runtimes, "multi-threaded");

  node_queue_scheduler->finish();
}

static void DuckDBSynthetic(const bool type_is_integer) {
  const auto node_queue_scheduler = std::make_shared<NodeQueueScheduler>();
  Hyrise::get().set_scheduler(node_queue_scheduler);

  auto pseudorandom_engine = std::mt19937{17};
  auto probability_dist = std::uniform_real_distribution<float>{-1e9f, 1e9};

  auto generate_element = [&]<typename T>(const int32_t& row_id) {
    if constexpr (std::is_same_v<int32_t, T>) {
      return row_id;
    } else {
      return probability_dist(pseudorandom_engine);
    }
  };  // NOLINT(readability/braces)

  for (auto row_count = 10'000'000; row_count <= 100'000'000; row_count += 10'000'000) {
    auto segments = std::vector<std::shared_ptr<AbstractSegment>>{};

    auto column_definitions = TableColumnDefinitions{};
    if (type_is_integer) {
      column_definitions = TableColumnDefinitions{{"a", DataType::Int, false}};
    } else {
      column_definitions = TableColumnDefinitions{{"a", DataType::Float, false}};
    }
    auto table = std::make_shared<Table>(column_definitions, TableType::Data);

    if (type_is_integer) {
      auto segment = pmr_vector<int32_t>{};
      for (auto row_id = int32_t{0}; row_id < row_count; ++row_id) {
        segment.emplace_back(generate_element.operator()<int32_t>(row_id));

        if (row_id > 0 && row_id % Chunk::DEFAULT_SIZE == 0) {
          std::ranges::shuffle(segment, pseudorandom_engine);
          segments.emplace_back(std::make_shared<ValueSegment<int32_t>>(std::move(segment)));
          segment = pmr_vector<int32_t>{};
        }
      }

      std::ranges::shuffle(segment, pseudorandom_engine);
      segments.emplace_back(std::make_shared<ValueSegment<int32_t>>(std::move(segment)));
    } else {
      auto segment = pmr_vector<float>{};
      for (auto row_id = int32_t{0}; row_id < row_count; ++row_id) {
        segment.emplace_back(generate_element.operator()<int32_t>(row_id));

        if (row_id > 0 && row_id % Chunk::DEFAULT_SIZE == 0) {
          std::ranges::shuffle(segment, pseudorandom_engine);
          segments.emplace_back(std::make_shared<ValueSegment<float>>(std::move(segment)));
          segment = pmr_vector<float>{};
        }
      }

      std::ranges::shuffle(segment, pseudorandom_engine);
      segments.emplace_back(std::make_shared<ValueSegment<float>>(std::move(segment)));
    }

    for (const auto& segment : segments) {
      table->append_chunk(pmr_vector<std::shared_ptr<AbstractSegment>>{segment});
      table->last_chunk()->set_immutable();
    }

    std::cout << "Synthetic sort experiment with " << table->row_count() << " rows. "
              << "Type is " << (type_is_integer ? std::string{"integer"} : std::string{"float"}) << ". "
              << "Memory Usage: " << table->memory_usage(MemoryUsageCalculationMode::Sampled) << ".\n";

    const auto table_wrapper = std::make_shared<TableWrapper>(table);
    table_wrapper->never_clear_output();
    table_wrapper->execute();

    const auto sort_definitions = std::vector{SortColumnDefinition{ColumnID{0}}};
    const auto [runtime_sum, runtimes] = measure_runtime(RUN_COUNT, [&]() {
      auto sort =
          std::make_shared<Sort>(table_wrapper, sort_definitions, Chunk::DEFAULT_SIZE, Sort::ForceMaterialization::No);
      sort->execute();
    });

    append_to_csv(type_is_integer ? std::string{"SyntheticSortingInteger"} : std::string{"SyntheticSortingFloat"},
                  row_count, "Unencoded", runtimes, "multi-threaded");
  }

  node_queue_scheduler->finish();
}

static void StringPrefix() {
  const auto chunk_count = ChunkID{8};  // ~524k rows.
  const auto sort_count = size_t{50};
  const auto thread_count = size_t{2};

  auto pseudorandom_engine = std::mt19937{17};
  auto probability_dist = std::uniform_int_distribution{1, 31};

  const auto node_queue_scheduler = std::make_shared<NodeQueueScheduler>();
  Hyrise::get().set_scheduler(node_queue_scheduler);

  auto column_definitions = TableColumnDefinitions{
      {"col_0", DataType::Int, true}, {"col_1", DataType::String, true}, {"col_2", DataType::Long, true}};
  auto sort_definitions = std::vector<SortColumnDefinition>{
      SortColumnDefinition{ColumnID{1}}, SortColumnDefinition{ColumnID{2}}, SortColumnDefinition{ColumnID{0}}};
  auto table = std::make_shared<Table>(column_definitions, TableType::Data);

  for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
    auto col_0_segment = pmr_vector<int32_t>{};
    auto col_1_segment = pmr_vector<pmr_string>{};
    auto col_2_segment = pmr_vector<int64_t>{};

    for (auto row_id = ChunkOffset{0}; row_id < Chunk::DEFAULT_SIZE; ++row_id) {
      col_0_segment.emplace_back(static_cast<int32_t>(probability_dist(pseudorandom_engine)));
      col_1_segment.emplace_back("2025-07-" + std::format("{:02} 12:{:02}:17", probability_dist(pseudorandom_engine), probability_dist(pseudorandom_engine)));
      col_2_segment.emplace_back(static_cast<int64_t>(probability_dist(pseudorandom_engine)));
    }

    auto segments = pmr_vector<std::shared_ptr<AbstractSegment>>{};
    segments.emplace_back(std::make_shared<ValueSegment<int32_t>>(std::move(col_0_segment)));
    segments.emplace_back(std::make_shared<ValueSegment<pmr_string>>(std::move(col_1_segment)));
    segments.emplace_back(std::make_shared<ValueSegment<int64_t>>(std::move(col_2_segment)));

    table->append_chunk(segments);
    table->last_chunk()->set_immutable();
  }

  const auto table_wrapper = std::make_shared<TableWrapper>(table);
  table_wrapper->never_clear_output();
  table_wrapper->execute();

  // We measure the time it takes to sort the input table `sort_count` times.
  auto runtimes = std::vector<size_t>{};
  for (auto run_id = size_t{0}; run_id < RUN_COUNT; ++run_id) {
    auto threads = std::vector<std::thread>{};
    auto stop_flag = std::atomic_flag{};

    auto sort_counter = std::atomic<size_t>{};
    const auto start = std::chrono::steady_clock::now();
    auto end = std::chrono::steady_clock::now();

    for (auto thread_id = size_t{0}; thread_id < thread_count; ++thread_id) {
      threads.emplace_back([&]() {
        while (!stop_flag.test()) {
          auto sort = std::make_shared<Sort>(table_wrapper, sort_definitions, Chunk::DEFAULT_SIZE);
          sort->execute();
          const auto old_count = sort_counter++;

          if (old_count == sort_count - 1) {
            stop_flag.test_and_set();
            end = std::chrono::steady_clock::now();
          }
        }
      });
    }

    for (auto& thread : threads) {
      thread.join();
    }

    runtimes.push_back(std::chrono::duration_cast<std::chrono::microseconds>(end - start).count());
  }

  append_to_csv("StringPrefix", table->row_count(), "Unencoded", runtimes, "multi-threaded");

  Hyrise::get().scheduler()->finish();
}

static void HiddenTest1() {}

static void HiddenTest2() {}

static void HiddenTest3() {}

static void HiddenTest4() {}

int main(int argc, char* argv[]) {
  if (std::filesystem::exists(FILENAME)) {
    std::filesystem::remove(FILENAME);
    std::cout << "Existing file " << FILENAME << " deleted.\n";
  }

  auto out_file = std::ofstream{FILENAME};
  out_file << "EXPERIMENT,SCALE,ENCODING,NOTE,RUN_ID,RUNTIME_US\n";
  out_file.close();

  if (argc == 2) {
    try {
      const auto tpcds_scale_factor = static_cast<uint32_t>(std::stoi(argv[1]));
      std::cout << "Running TPC-DS with scale factor " << tpcds_scale_factor << '\n';

      for (const auto& encoding_config : ENCODING_CONFIGS) {
        if (tpcds_scale_factor <= 100) {
          DuckDBTPCDS_CS(tpcds_scale_factor, encoding_config);
        }
        DuckDBTPCDS_C_Strings(tpcds_scale_factor, encoding_config);
        DuckDBTPCDS_C_Integers(tpcds_scale_factor, encoding_config);
      }
    } catch (...) {
      const auto argument = std::string{argv[1]};
      if (argument != "hidden") {
        std::cerr << "Unexpected argument\n";
        return 1;
      }

      HiddenTest1();
      HiddenTest2();
      HiddenTest3();
      HiddenTest4();
    }

    return 0;
  }

  DuckDBSynthetic(true);
  DuckDBSynthetic(false);
  StringPrefix();

  return 0;
}
