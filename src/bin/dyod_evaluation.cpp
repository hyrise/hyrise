#include <algorithm>
#include <fstream>
#include <iostream>
#include <memory>
#include <random>
#include <string>
#include <utility>
#include <vector>

#include "benchmark_config.hpp"
#include "expression/expression_functional.hpp"
#include "hyrise.hpp"
#include "operators/aggregate_dyod.hpp"
#include "operators/get_table.hpp"
#include "operators/join_hash.hpp"
#include "operators/operator_join_predicate.hpp"
#include "operators/print.hpp"
#include "operators/table_scan.hpp"
#include "operators/table_wrapper.hpp"
#include "scheduler/immediate_execution_scheduler.hpp"
#include "scheduler/job_task.hpp"
#include "scheduler/node_queue_scheduler.hpp"
#include "sql/sql_pipeline_builder.hpp"
#include "storage/chunk.hpp"
#include "storage/chunk_encoder.hpp"
#include "tpcds/tpcds_table_generator.hpp"
#include "tpch/tpch_constants.hpp"
#include "tpch/tpch_table_generator.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

using namespace hyrise;
using namespace expression_functional;

constexpr auto RUN_COUNT = size_t{10} + 1;  // 1 warm-up run.
constexpr auto FILENAME = "RESULTS.csv";

enum class BenchmarkType : uint8_t { tpch, tpcds };

const auto ENCODING_CONFIGS = std::vector<EncodingConfig>{EncodingConfig{SegmentEncodingSpec{EncodingType::Dictionary}},
                                                          EncodingConfig{SegmentEncodingSpec{EncodingType::Unencoded}}};

static void silent_tpcx_table_generation(const BenchmarkType benchmark_type, float scale_factor,
                                         std::shared_ptr<BenchmarkConfig> config) {
  auto* initial_buffer = std::cout.rdbuf();

  std::cout.rdbuf(nullptr);
  if (benchmark_type == BenchmarkType::tpch) {
    TPCHTableGenerator(scale_factor, ClusteringConfiguration::None, config).generate_and_store();
  } else if (benchmark_type == BenchmarkType::tpcds) {
    TPCDSTableGenerator(static_cast<uint32_t>(scale_factor), config).generate_and_store();
  }
  std::cout.rdbuf(initial_buffer);
}

void append_to_csv(const std::string& benchmark, const float scale, const std::string& encoding,
                   const std::vector<size_t>& runtimes, const std::string note = "") {
  auto out_file = std::ofstream(FILENAME, std::ios::app);

  auto run_id = size_t{0};
  for (const auto& runtime : runtimes) {
    out_file << std::format("\"{}\",{},\"{}\",\"{}\",{},{}\n", benchmark, scale, encoding, note, run_id, runtime);
    ++run_id;
  }
}

void append_to_csv(const std::string& benchmark, const float scale, const EncodingConfig& encoding_config,
                   const std::vector<size_t>& runtimes, const std::string note = "") {
  auto sstream = std::stringstream{};
  sstream << *encoding_config.preferred_encoding_spec;
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

std::pair<std::shared_ptr<Table>, std::shared_ptr<GetTable>> setup_get_table(
    const auto& table_name, const std::vector<std::string>& needed_column_names) {
  auto& sm = Hyrise::get().storage_manager;

  auto table = sm.get_table(table_name);
  const auto column_count = table->column_count();
  auto unpruned_column_ids = std::vector<ColumnID>{};
  for (const auto& column_name : needed_column_names) {
    const auto column_id = table->column_id_by_name(column_name);
    unpruned_column_ids.emplace_back(column_id);
  }

  std::ranges::sort(unpruned_column_ids);
  auto all_column_ids = std::vector<ColumnID>(column_count);
  std::iota(all_column_ids.begin(), all_column_ids.end(), ColumnID{0});

  auto pruned_column_ids = std::vector<ColumnID>();
  std::ranges::set_difference(all_column_ids, unpruned_column_ids, std::back_inserter(pruned_column_ids));

  const auto get_table = std::make_shared<GetTable>(table_name, std::vector<ChunkID>{}, pruned_column_ids);
  get_table->never_clear_output();
  get_table->execute();

  return {table, get_table};
}

static void TPCDSQ97(const float scale_factor, const EncodingConfig encoding_config) {
  for (const auto use_scheduler : {true, false}) {
    const auto node_queue_scheduler = std::make_shared<NodeQueueScheduler>();
    if (use_scheduler) {
      Hyrise::get().set_scheduler(node_queue_scheduler);
    } else {
      Hyrise::get().set_scheduler(std::make_shared<ImmediateExecutionScheduler>());
    }

    const auto [date_dim_t, date_dim_gt] =
        setup_get_table("date_dim", std::vector<std::string>{"d_date_sk", "d_month_seq"});
    const auto [catalog_sales_t, catalog_sales_gt] =
        setup_get_table("store_sales", std::vector<std::string>{"ss_sold_date_sk", "ss_item_sk", "ss_customer_sk"});

    const auto date_dim_month_seq = std::make_shared<PQPColumnExpression>(ColumnID{1}, DataType::Int, "d_month_seq");
    const auto table_scan =
        std::make_shared<TableScan>(date_dim_gt, between_inclusive_(date_dim_month_seq, 1200, 1211));
    table_scan->never_clear_output();
    table_scan->execute();

    // ss_sold_date_sk = d_date_sk
    const auto join = std::make_shared<JoinHash>(
        catalog_sales_gt, date_dim_gt, JoinMode::Inner,
        OperatorJoinPredicate{ColumnIDPair(ColumnID{0}, ColumnID{0}), PredicateCondition::Equals});
    join->never_clear_output();
    join->execute();

    const auto ss_customer_sk = std::make_shared<PQPColumnExpression>(ColumnID{2}, DataType::Int, "ss_customer_sk");
    const auto ss_item_sk = std::make_shared<PQPColumnExpression>(ColumnID{1}, DataType::Int, "ss_item_sk");
    const auto aggregates = std::vector<std::shared_ptr<WindowFunctionExpression>>{};
    const auto groupby_column_ids = std::vector<ColumnID>{ColumnID{2}, ColumnID{1}};

    // GROUP BY ss_customer_sk, ss_item_sk
    const auto [runtime_sum, runtimes] = measure_runtime(RUN_COUNT, [&]() {
      auto aggregate = std::make_shared<AggregateDYOD>(join, aggregates, groupby_column_ids);
      aggregate->execute();
    });

    append_to_csv("TPCDSQ97", scale_factor, encoding_config, runtimes,
                  use_scheduler ? "multi-threaded" : "single-threaded");

    if (use_scheduler) {
      node_queue_scheduler->finish();
    }
  }
}

static void TPCHQ01(const float scale_factor, const EncodingConfig encoding_config) {
  for (const auto use_scheduler : {true, false}) {
    const auto node_queue_scheduler = std::make_shared<NodeQueueScheduler>();
    if (use_scheduler) {
      Hyrise::get().set_scheduler(node_queue_scheduler);
    } else {
      Hyrise::get().set_scheduler(std::make_shared<ImmediateExecutionScheduler>());
    }

    const auto q1_pre_aggregation_sql =
        " SELECT"
        "      l_returnflag,"
        "      l_linestatus,"
        "      l_quantity as qty,"
        "      l_extendedprice as base_price,"
        "      l_extendedprice*(1-l_discount) as disc_price,"
        "      l_extendedprice*(1-l_discount)*(1+l_tax) as charge,"
        "      l_discount as disc"
        " FROM"
        "      lineitem"
        " WHERE"
        "      l_shipdate <= date '1998-12-01' - interval '30' day";
    auto sql_pipeline = SQLPipelineBuilder{q1_pre_aggregation_sql}.create_pipeline();
    const auto result = sql_pipeline.get_result_table();
    Assert(result.first, "Preparation query failed.");

    const auto l_returnflag = std::make_shared<PQPColumnExpression>(ColumnID{0}, DataType::String, "l_returnflag");
    const auto l_linestatus = std::make_shared<PQPColumnExpression>(ColumnID{1}, DataType::String, "l_linestatus");
    const auto qty = std::make_shared<PQPColumnExpression>(ColumnID{2}, DataType::Float, "qty");
    const auto base_price = std::make_shared<PQPColumnExpression>(ColumnID{3}, DataType::Float, "base_price");
    const auto disc_price = std::make_shared<PQPColumnExpression>(ColumnID{4}, DataType::Float, "disc_price");
    const auto charge = std::make_shared<PQPColumnExpression>(ColumnID{5}, DataType::Float, "charge");
    const auto disc = std::make_shared<PQPColumnExpression>(ColumnID{6}, DataType::Float, "disc");

    const auto aggregates = std::vector<std::shared_ptr<WindowFunctionExpression>>{
        sum_(qty),
        sum_(base_price),
        sum_(disc_price),
        sum_(charge),
        avg_(qty),
        avg_(base_price),
        avg_(disc),
        std::make_shared<WindowFunctionExpression>(WindowFunction::Count,
                                                   pqp_column_(INVALID_COLUMN_ID, DataType::Long, "*"))};
    const auto groupby_column_ids = std::vector<ColumnID>{ColumnID{0}, ColumnID{1}};

    const auto result_table = result.second;
    const auto table_wrapper = std::make_shared<TableWrapper>(result_table);
    table_wrapper->never_clear_output();
    table_wrapper->execute();

    const auto [runtime_sum, runtimes] = measure_runtime(RUN_COUNT, [&]() {
      auto aggregate = std::make_shared<AggregateDYOD>(table_wrapper, aggregates, groupby_column_ids);
      aggregate->execute();
    });

    auto aggregate = std::make_shared<AggregateDYOD>(table_wrapper, aggregates, groupby_column_ids);
    aggregate->never_clear_output();
    aggregate->execute();
    Assert(aggregate->get_output()->row_count() == 4, "Unexpected result size.");

    append_to_csv("TPCHQ01", scale_factor, encoding_config, runtimes,
                  use_scheduler ? "multi-threaded" : "single-threaded");

    if (use_scheduler) {
      node_queue_scheduler->finish();
    }
  }
}

static void TPCHQ18(const float scale_factor, const EncodingConfig encoding_config) {
  for (const auto use_scheduler : {true, false}) {
    const auto node_queue_scheduler = std::make_shared<NodeQueueScheduler>();
    if (use_scheduler) {
      Hyrise::get().set_scheduler(node_queue_scheduler);
    } else {
      Hyrise::get().set_scheduler(std::make_shared<ImmediateExecutionScheduler>());
    }

    const auto [lineitem_t, lineitem_gt] =
        setup_get_table("lineitem", std::vector<std::string>{"l_orderkey", "l_quantity"});

    const auto l_orderkey = std::make_shared<PQPColumnExpression>(ColumnID{0}, DataType::Int, "l_orderkey");
    const auto l_quantity = std::make_shared<PQPColumnExpression>(ColumnID{1}, DataType::Float, "l_quantity");

    const auto aggregates = std::vector<std::shared_ptr<WindowFunctionExpression>>{sum_(l_quantity)};
    const auto groupby_column_ids = std::vector<ColumnID>{ColumnID{0}};

    const auto [runtime_sum, runtimes] = measure_runtime(RUN_COUNT, [&]() {
      auto aggregate = std::make_shared<AggregateDYOD>(lineitem_gt, aggregates, groupby_column_ids);
      aggregate->execute();
    });

    append_to_csv("TPCHQ18", scale_factor, encoding_config, runtimes,
                  use_scheduler ? "multi-threaded" : "single-threaded");

    if (use_scheduler) {
      node_queue_scheduler->finish();
    }
  }
}

static void JOBlikeMINMAX(const EncodingConfig encoding_config) {
  const auto chunk_count = ChunkID{512};  // ~33 M rows.
  const auto agg_execution_count = size_t{30};
  const auto thread_count = size_t{4};

  auto pseudorandom_engine = std::mt19937{17};
  auto probability_dist = std::uniform_int_distribution{1, 31};

  for (const auto use_scheduler : {true, false}) {
    const auto node_queue_scheduler = std::make_shared<NodeQueueScheduler>();
    if (use_scheduler) {
      Hyrise::get().set_scheduler(node_queue_scheduler);
    } else {
      Hyrise::get().set_scheduler(std::make_shared<ImmediateExecutionScheduler>());
    }

    auto column_definitions = TableColumnDefinitions{
        {"col_0", DataType::Int, true}, {"col_1", DataType::String, true}, {"col_2", DataType::Long, true}};
    auto table = std::make_shared<Table>(column_definitions, TableType::Data);

    for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
      auto col_0_segment = pmr_vector<int32_t>{};
      auto col_1_segment = pmr_vector<pmr_string>{};
      auto col_2_segment = pmr_vector<int64_t>{};

      for (auto row_id = ChunkOffset{0}; row_id < Chunk::DEFAULT_SIZE; ++row_id) {
        col_0_segment.emplace_back(static_cast<int32_t>(probability_dist(pseudorandom_engine)));
        auto string_value = "2025-07-" + std::format("{:02} 12:{:02}:17", probability_dist(pseudorandom_engine),
                                                     probability_dist(pseudorandom_engine));
        if (row_id == 4'913) {
          string_value +=
              " // Important note: for unknown reasons there is a long string comment appended to this"
              " row. People are still not sure what the purpose is.";
        }
        col_1_segment.emplace_back(string_value);
        col_2_segment.emplace_back(static_cast<int64_t>(probability_dist(pseudorandom_engine)));
      }

      auto segments = pmr_vector<std::shared_ptr<AbstractSegment>>{};
      segments.emplace_back(std::make_shared<ValueSegment<int32_t>>(std::move(col_0_segment)));
      segments.emplace_back(std::make_shared<ValueSegment<pmr_string>>(std::move(col_1_segment)));
      segments.emplace_back(std::make_shared<ValueSegment<int64_t>>(std::move(col_2_segment)));

      table->append_chunk(segments);
      table->last_chunk()->set_immutable();
    }

    const auto encoding_spec = *encoding_config.preferred_encoding_spec;
    ChunkEncoder::encode_all_chunks(table, encoding_spec);

    const auto table_wrapper = std::make_shared<TableWrapper>(table);
    table_wrapper->never_clear_output();
    table_wrapper->execute();

    // We measure the time it takes to aggregate the input table `agg_execution_count` times.
    auto runtimes = std::vector<size_t>{};
    for (auto run_id = size_t{0}; run_id < RUN_COUNT; ++run_id) {
      auto threads = std::vector<std::thread>{};
      auto stop_flag = std::atomic_flag{};

      auto agg_execution_counter = std::atomic<size_t>{};

      const auto col_1 = std::make_shared<PQPColumnExpression>(ColumnID{1}, DataType::String, "col_1");

      const auto aggregates = std::vector<std::shared_ptr<WindowFunctionExpression>>{min_(col_1), max_(col_1)};
      const auto groupby_column_ids = std::vector<ColumnID>{};

      auto end = std::chrono::steady_clock::now();
      const auto start = std::chrono::steady_clock::now();

      for (auto thread_id = size_t{0}; thread_id < thread_count; ++thread_id) {
        threads.emplace_back([&]() {
          while (!stop_flag.test()) {
            auto aggregate = std::make_shared<AggregateDYOD>(table_wrapper, aggregates, groupby_column_ids);
            aggregate->execute();
            const auto old_count = agg_execution_counter++;

            if (old_count == agg_execution_count - 1) {
              stop_flag.test_and_set();
              end = std::chrono::steady_clock::now();
            }
          }
        });
      }

      for (auto& thread : threads) {
        thread.join();
      }

      Assert(end > start, "Benchmark did not run properly: negative runtime.");

      if (run_id > 0) {
        runtimes.push_back(std::chrono::duration_cast<std::chrono::microseconds>(end - start).count());
      }
    }

    auto sstream = std::stringstream{};
    sstream << encoding_spec;
    append_to_csv("JOBlikeMINMAX", static_cast<float>(table->row_count()), sstream.str(), runtimes,
                  use_scheduler ? "multi-threaded" : "single-threaded");

    if (use_scheduler) {
      node_queue_scheduler->finish();
    }
  }
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
    // try {
    const auto scale_factor = std::stof(argv[1]);
    std::cout << "Running TPC benchmarks with scale factor " << scale_factor << ".\n";

    for (const auto& encoding_config : ENCODING_CONFIGS) {
      auto benchmark_config = std::make_shared<BenchmarkConfig>();
      benchmark_config->cache_binary_tables = true;
      benchmark_config->encoding_config = encoding_config;

      silent_tpcx_table_generation(BenchmarkType::tpch, scale_factor, benchmark_config);
      silent_tpcx_table_generation(BenchmarkType::tpcds, scale_factor, benchmark_config);

      TPCHQ01(scale_factor, encoding_config);
      TPCHQ18(scale_factor, encoding_config);
      TPCDSQ97(scale_factor, encoding_config);
    }
    // } catch (...) {
    // const auto argument = std::string{argv[1]};
    // if (argument != "hidden") {
    //   std::cerr << "Unexpected argument\n";
    //   return 1;
    // }

    HiddenTest1();
    HiddenTest2();
    HiddenTest3();
    HiddenTest4();
    // }

    return 0;
  }

  for (const auto& encoding_config : ENCODING_CONFIGS) {
    JOBlikeMINMAX(encoding_config);
  }

  return 0;
}
