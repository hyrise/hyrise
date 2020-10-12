#include <chrono>
#include <fstream>

#include "hyrise.hpp"
#include "utils/plugin_manager.hpp"

#include "benchmark_config.hpp"
#include "benchmark_runner.hpp"
#include "cli_config_parser.hpp"
#include "file_based_benchmark_item_runner.hpp"
#include "file_based_table_generator.hpp"
#include "sql/sql_pipeline_builder.hpp"
#include "tpcc/tpcc_benchmark_item_runner.hpp"
#include "tpcc/tpcc_table_generator.hpp"
#include "tpcds/tpcds_table_generator.hpp"
#include "tpch/tpch_benchmark_item_runner.hpp"
#include "tpch/tpch_table_generator.hpp"
#include "utils/timer.hpp"

using namespace opossum;  // NOLINT
using namespace std::chrono_literals;

namespace {

size_t get_all_segments_memory_usage() {
  auto result = size_t{0};
  for (const auto& [table_name, table] : Hyrise::get().storage_manager.tables()) {
    const auto chunk_count = table->chunk_count();
    for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
      if (!table->get_chunk(chunk_id)) {
        continue;
      }

      const auto& chunk = table->get_chunk(chunk_id);
      const auto column_count = chunk->column_count();
      for (auto column_id = ColumnID{0}; column_id < column_count; ++column_id) {
        const auto segment = chunk->get_segment(column_id);
        result += segment->memory_usage(MemoryUsageCalculationMode::Sampled);
      }
    }
  }
  return result;
}

}  // namespace

int main(int argc, const char* argv[]) {
  if (argc > 1) {
    // Default path for calibration and PQP exporting.
    for (auto plugin_id = 1; plugin_id < argc; ++plugin_id) {
      const std::filesystem::path plugin_path(argv[plugin_id]);
      const auto plugin_name = plugin_name_from_path(plugin_path);
      Hyrise::get().plugin_manager.load_plugin(plugin_path);
    }
    return 0;
  }

  const auto BENCHMARKS = std::vector<std::string>{"TPC-C", "TPC-DS", "JOB", "TPC-H"};

  const auto env_var_playback = std::getenv("BENCHMARK_TO_EVALUATE");
  if (env_var_playback == nullptr) {
    std::cerr << "Please pass environment variable \"BENCHMARK_TO_EVALUATE\" to set a target benchmark.\nExiting Plugin." << std::flush;
    exit(17);
  }
  auto BENCHMARK_TO_EVALUATE = std::string(env_var_playback);

  auto SCALE_FACTOR = 10.0f;

  std::string path = "../compression_selection_v3/configurations_CH/";
  if (BENCHMARK_TO_EVALUATE == "TPC-H") {
    auto start_config = std::make_shared<BenchmarkConfig>(BenchmarkConfig::get_default_config());
    start_config->max_runs = 5;
    start_config->enable_visualization = false;
    start_config->cache_binary_tables = false;

    const bool use_prepared_statements = false;
    auto start_context = BenchmarkRunner::create_context(*start_config);

    const std::vector<BenchmarkItemID> tpch_query_ids_warmup = {BenchmarkItemID{5}};
    auto start_item_runner = std::make_unique<TPCHBenchmarkItemRunner>(start_config, use_prepared_statements, SCALE_FACTOR, tpch_query_ids_warmup);
    BenchmarkRunner(*start_config, std::move(start_item_runner), std::make_unique<TPCHTableGenerator>(SCALE_FACTOR, start_config), start_context).run();

    for (const auto& entry : std::filesystem::directory_iterator(path)) {
      const auto conf_path = entry.path();
      const auto conf_name = conf_path.stem();
      const auto filename = conf_path.filename().string();

      if (filename.find("conf") != 0 || filename.find(".json") != std::string::npos) {
        std::cout << "Skipping " << conf_path << std::endl;
        continue;
      }

      std::cout << "######\n#####\n##### Benchmarking: " << conf_name << "\n#####\n#####" << std::endl;

      
      // To speed up the table generation, the node scheduler is used. To not interfere with any settings for the actual
      // Hyrise process (e.g., the test runner or the calibration), the current scheduler is stored, replaced, and
      // eventually set again.
      Hyrise::get().scheduler()->wait_for_all_tasks();
      const auto previous_scheduler = Hyrise::get().scheduler();
      Hyrise::get().set_scheduler(std::make_shared<NodeQueueScheduler>());

      auto config = std::make_shared<BenchmarkConfig>(BenchmarkConfig::get_default_config());
      config->max_runs = 10;
      config->enable_visualization = false;
      config->output_file_path = conf_name.string() + ".json";
      config->cache_binary_tables = false;
      config->max_duration = std::chrono::seconds(600);

      std::vector<std::shared_ptr<AbstractTask>> jobs;

      auto context = BenchmarkRunner::create_context(*config);

      std::ifstream configuration_file(entry.path().string());
      const auto line_count = std::count(std::istreambuf_iterator<char>(configuration_file), std::istreambuf_iterator<char>(), '\n');
      jobs.reserve(line_count + 10);
      std::string line;
      configuration_file.seekg(0, std::ios::beg);
      std::cout << "Reencoding: " << std::flush;
      while (std::getline(configuration_file, line))
      {
        std::vector<std::string> line_values;
        std::istringstream linestream(line);
        std::string value;
        while (std::getline(linestream, value, ','))
        {
          line_values.push_back(value);
        }

        const auto table_name = line_values[0];
        const auto column_name = line_values[1];
        const auto chunk_id = ChunkID{static_cast<uint32_t>(std::stoi(line_values[2]))};
        const auto encoding_type_str = line_values[3];
        const auto vector_compression_type_str = line_values[4];

        const auto& table = Hyrise::get().storage_manager.get_table(table_name);
        if (chunk_id >= table->chunk_count()) {
          continue;
        }
        const auto& chunk = table->get_chunk(chunk_id);
        const auto& column_id = table->column_id_by_name(column_name);
        const auto& segment = chunk->get_segment(column_id);
        const auto& data_type = table->column_data_type(column_id);

        const auto encoding_type = encoding_type_to_string.right.at(encoding_type_str);

        SegmentEncodingSpec spec = {encoding_type};
        if (vector_compression_type_str != "None") {
          const auto vector_compression_type = vector_compression_type_to_string.right.at(vector_compression_type_str);
          spec.vector_compression_type = vector_compression_type;
        }

        jobs.emplace_back(std::make_shared<JobTask>([chunk, segment, data_type, spec, column_id]() {
          const auto& encoded_segment = ChunkEncoder::encode_segment(segment, data_type, spec);
          chunk->replace_segment(column_id, encoded_segment);
        }));
        jobs.back()->schedule();
      }
      configuration_file.close();

      Hyrise::get().scheduler()->wait_for_tasks(jobs);
      Hyrise::get().scheduler()->wait_for_all_tasks();
      Hyrise::get().set_scheduler(previous_scheduler);  // set scheduler back to previous one.

      std::cout << " done." << std::endl;
      std::cout << "Starting benchmark." << std::endl;

      auto item_runner = std::make_unique<TPCHBenchmarkItemRunner>(config, use_prepared_statements, SCALE_FACTOR);//, tpch_query_ids_benchmark);
      BenchmarkRunner(*config, std::move(item_runner), nullptr, context).run();

      std::ofstream size_result;
      size_result.open(conf_name.string() + ".size");
      size_result << get_all_segments_memory_usage();
      size_result.close();
    }
  } else if (BENCHMARK_TO_EVALUATE == "CH" || BENCHMARK_TO_EVALUATE == "TPC-C") {
    auto config = std::make_shared<BenchmarkConfig>(BenchmarkConfig::get_default_config());
    auto warehouse_count = int{1};
    config->max_duration = std::chrono::seconds{20};

    if (BENCHMARK_TO_EVALUATE == "CH") {
      warehouse_count = 1;
      config->max_duration = std::chrono::seconds{30};
    }

    config->max_runs = -1;
    config->benchmark_mode = BenchmarkMode::Shuffled;
    config->warmup_duration = std::chrono::seconds(0);
    config->enable_scheduler = true;
    config->clients = 5;
    config->cores = 10;

    auto ch_benchmark_queries = std::vector<std::string>{};
    if (BENCHMARK_TO_EVALUATE == "CH") {
      constexpr auto TPC_H_SCALE_FACTOR = 0.1f;
      auto tpch_table_generator = std::make_unique<TPCHTableGenerator>(TPC_H_SCALE_FACTOR, config);
      tpch_table_generator->generate_and_store();

      const auto ch_benchmark_queries_path = "hyrise/resources/ch_benchmark_queries.sql";
      std::ifstream ch_benchmark_queries_file(ch_benchmark_queries_path);

      std::string sql_query_string;
      while (std::getline(ch_benchmark_queries_file, sql_query_string)) {
        if (sql_query_string.size() > 0 && !sql_query_string.starts_with("--")) {
          ch_benchmark_queries.emplace_back(sql_query_string);
        }
      }
      Assert(ch_benchmark_queries.size() > 0, "Failed to read CH-benCHmark queries.");
    }

    for (const auto& entry : std::filesystem::directory_iterator(path)) {
      const auto conf_path = entry.path();
      const auto conf_name = conf_path.stem();
      const auto filename = conf_path.filename().string();

      if (filename.find("conf") != 0 || filename.find(".json") != std::string::npos) {
        std::cout << "Skipping " << conf_path << std::endl;
        continue;
      }

      std::cout << "######\n#####\n##### Benchmarking: " << conf_name << "\n#####\n#####" << std::endl;

      std::set<std::string> tpcc_tables = {"ITEM", "WAREHOUSE", "STOCK", "DISTRICT", "CUSTOMER", "HISTORY", "ORDER", "ORDER_LINE", "NEW_ORDER"};
      for (const auto& [table_name, table] : Hyrise::get().storage_manager.tables()) {
        if (tpcc_tables.contains(table_name)) {
          Hyrise::get().storage_manager.drop_table(table_name);
        }
      }
      TPCCTableGenerator(warehouse_count, config).generate_and_store();

      // To speed up the table generation, the node scheduler is used. To not interfere with any settings for the actual
      // Hyrise process (e.g., the test runner or the calibration), the current scheduler is stored, replaced, and
      // eventually set again.
      Hyrise::get().scheduler()->wait_for_all_tasks();
      const auto previous_scheduler = Hyrise::get().scheduler();
      Hyrise::get().set_scheduler(std::make_shared<NodeQueueScheduler>());

      config->output_file_path = conf_name.string() + ".json";

      std::vector<std::shared_ptr<AbstractTask>> jobs;
      auto context = BenchmarkRunner::create_context(*config);
      context.emplace("scale_factor", warehouse_count);

      std::ifstream configuration_file(entry.path().string());
      const auto line_count = std::count(std::istreambuf_iterator<char>(configuration_file), std::istreambuf_iterator<char>(), '\n');
      jobs.reserve(line_count + 10);
      std::string line;
      configuration_file.seekg(0, std::ios::beg);
      std::cout << "Reencoding: " << std::flush;
      while (std::getline(configuration_file, line))
      {
        std::vector<std::string> line_values;
        std::istringstream linestream(line);
        std::string value;
        while (std::getline(linestream, value, ','))
        {
          line_values.push_back(value);
        }

        const auto table_name = line_values[0];
        const auto column_name = line_values[1];
        const auto chunk_id = ChunkID{static_cast<uint32_t>(std::stoi(line_values[2]))};
        const auto encoding_type_str = line_values[3];
        const auto vector_compression_type_str = line_values[4];

        const auto& table = Hyrise::get().storage_manager.get_table(table_name);
        if (chunk_id >= table->chunk_count()) {
          continue;
        }
        const auto& chunk = table->get_chunk(chunk_id);
        const auto& column_id = table->column_id_by_name(column_name);
        const auto& segment = chunk->get_segment(column_id);
        const auto& data_type = table->column_data_type(column_id);

        const auto encoding_type = encoding_type_to_string.right.at(encoding_type_str);

        SegmentEncodingSpec spec = {encoding_type};
        if (vector_compression_type_str != "None") {
          const auto vector_compression_type = vector_compression_type_to_string.right.at(vector_compression_type_str);
          spec.vector_compression_type = vector_compression_type;
        }

        jobs.emplace_back(std::make_shared<JobTask>([chunk, segment, data_type, spec, column_id]() {
          const auto& encoded_segment = ChunkEncoder::encode_segment(segment, data_type, spec);
          chunk->replace_segment(column_id, encoded_segment);
        }));
        jobs.back()->schedule();
      }
      configuration_file.close();

      Hyrise::get().scheduler()->wait_for_tasks(jobs);
      Hyrise::get().scheduler()->wait_for_all_tasks();
      Hyrise::get().set_scheduler(previous_scheduler);  // set scheduler back to previous one.

      std::cout << " done." << std::endl;
      std::cout << "Starting benchmark." << std::endl;

      auto run_ch_benchmark_queries = std::atomic<bool>{false};
      auto ch_benchmark_thread = std::thread([&ch_benchmark_queries, &run_ch_benchmark_queries]() {
        // Obtain a debuggable ID for view rewriting per thread
        const auto timestamp_str = std::to_string(std::chrono::system_clock::now().time_since_epoch() /
                                                  std::chrono::milliseconds(1));

        auto& storage_manager = Hyrise::get().storage_manager;
        while (true) {
          while (!run_ch_benchmark_queries) {
            std::this_thread::sleep_for(1s);
          }

          while (run_ch_benchmark_queries && !storage_manager.has_table("ITEM")) {
            std::this_thread::sleep_for(1s);
          }

          std::cout << "Starting CH-benCHmark queries in 1s." << std::endl;
          std::this_thread::sleep_for(1s);

          auto query_id = size_t{0};
          const auto query_count = ch_benchmark_queries.size();
          while (run_ch_benchmark_queries) {
            query_id = query_id % query_count;
            std::cout << "CH-benCHmark - Query #" << query_id << ": ";
            const auto adapted_query = std::regex_replace(ch_benchmark_queries[query_id],
                                                          std::regex("REVENUE_VIEW"),
                                                          "REVENUE__" + timestamp_str);
            auto sql_pipeline = SQLPipelineBuilder{adapted_query}.create_pipeline();
            Timer timer;
            const auto& [status, result] = sql_pipeline.get_result_table();
            std::cout << static_cast<size_t>(timer.lap().count()) << " ns (execution finished)." << std::endl << std::flush;

            Assert(status == SQLPipelineStatus::Success, "Execution of query #" + std::to_string(query_id) + " did not succeed.");
            ++query_id;
          }
        }
      });

      if (BENCHMARK_TO_EVALUATE == "CH") {
        run_ch_benchmark_queries = true;
      }

      auto item_runner = std::make_unique<TPCCBenchmarkItemRunner>(config, warehouse_count);
      auto benchmark_runner = std::make_shared<BenchmarkRunner>(*config, std::move(item_runner), nullptr, context);

      Hyrise::get().benchmark_runner = benchmark_runner;
      benchmark_runner->run();
      std::cout << "TPC-C done." << std::endl;

      if (BENCHMARK_TO_EVALUATE == "CH") {
        run_ch_benchmark_queries = false;
      }

      std::ofstream size_result;
      size_result.open(conf_name.string() + ".size");
      size_result << get_all_segments_memory_usage();
      size_result.close();

      // This is bad!
      ch_benchmark_thread.detach();
    }
    std::this_thread::sleep_for(180s);
  }

  const auto env_var = std::getenv("BENCHMARK_TO_RUN");
  if (env_var == nullptr) {
    std::cerr << "Please pass environment variable \"BENCHMARK_TO_RUN\" to set a target benchmark.\nExiting Plugin." << std::flush;
    exit(17);
  } else if (strncmp(env_var, "foobar", 6ul)) {

    /**
     *
     *    WE ARE DONE HERE.
        Just some code copied to ensure that benchmarklib does include all necessary code. Other solutions include some unnice CMakeList modifications ...
     *
     */
    exit(0);
  }

  if (env_var != nullptr) {
    std::cout << EncodingConfig(CLIConfigParser::parse_encoding_config(std::string{env_var})).to_json() << std::endl;
  }

  auto BENCHMARK = std::string(env_var);
  if (std::find(BENCHMARKS.begin(), BENCHMARKS.end(), BENCHMARK) == BENCHMARKS.end()) {
    std::cerr << "Benchmark \"" << BENCHMARK << "\" not supported. Supported benchmarks: ";
    for (const auto& benchmark : BENCHMARKS) std::cout << "\"" << benchmark << "\" ";
    std::cerr << "\nExiting." << std::flush;
    exit(17);
  }
  std::cout << "Running " << BENCHMARK << " ... " << std::endl;

  auto config = std::make_shared<BenchmarkConfig>(BenchmarkConfig::get_default_config());
  config->max_runs = 10;
  config->enable_visualization = false;
  config->chunk_size = 100'000;
  config->cache_binary_tables = true;

  constexpr auto USE_PREPARED_STATEMENTS = false;


  //
  //  TPC-H
  //
  if (BENCHMARK == "TPC-H") {
    SCALE_FACTOR = 1.0f;
    config->max_runs = 100;
    // const std::vector<BenchmarkItemID> tpch_query_ids_benchmark = {BenchmarkItemID{5}};
    // auto item_runner = std::make_ unique<TPCHBenchmarkItemRunner>(config, USE_PREPARED_STATEMENTS, SCALE_FACTOR, tpch_query_ids_benchmark);
    auto item_runner = std::make_unique<TPCHBenchmarkItemRunner>(config, USE_PREPARED_STATEMENTS, SCALE_FACTOR);
    auto benchmark_runner = std::make_shared<BenchmarkRunner>(
        *config, std::move(item_runner), std::make_unique<TPCHTableGenerator>(SCALE_FACTOR, config), BenchmarkRunner::create_context(*config));
    Hyrise::get().benchmark_runner = benchmark_runner;
    benchmark_runner->run();
  }
  //
  //  /TPC-H
  //


  //
  //  TPC-DS
  //
  else if (BENCHMARK == "TPC-DS") {
    SCALE_FACTOR = 1.0f;
    config->max_runs = 1;
    const std::string query_path = "hyrise/resources/benchmark/tpcds/tpcds-result-reproduction/query_qualification";

    auto query_generator = std::make_unique<FileBasedBenchmarkItemRunner>(config, query_path, std::unordered_set<std::string>{});
    auto table_generator = std::make_unique<TpcdsTableGenerator>(SCALE_FACTOR, config);
    auto benchmark_runner = std::make_shared<BenchmarkRunner>(*config, std::move(query_generator), std::move(table_generator),
                                                              opossum::BenchmarkRunner::create_context(*config));
    Hyrise::get().benchmark_runner = benchmark_runner;
    benchmark_runner->run();
  }
  //
  //  /TPC-DS
  //

  //
  //  JOB
  //
  else if (BENCHMARK == "JOB") {
    config->max_runs = 1;

    const auto table_path = "hyrise/imdb_data";
    const auto query_path = "hyrise/third_party/join-order-benchmark";
    const auto non_query_file_names = std::unordered_set<std::string>{"fkindexes.sql", "schema.sql"};

    auto benchmark_item_runner = std::make_unique<FileBasedBenchmarkItemRunner>(config, query_path, non_query_file_names);
    auto table_generator = std::make_unique<FileBasedTableGenerator>(config, table_path);
    auto benchmark_runner = std::make_shared<BenchmarkRunner>(*config, std::move(benchmark_item_runner), std::move(table_generator),
                                                              BenchmarkRunner::create_context(*config));

    Hyrise::get().benchmark_runner = benchmark_runner;
    benchmark_runner->run();
  }
  //
  //  /JOB
  //

  //
  //  TPC-C
  //
  else if (BENCHMARK == "TPC-C") {
    constexpr auto WAREHOUSES = int{2};

    auto context = BenchmarkRunner::create_context(*config);
    context.emplace("scale_factor", WAREHOUSES);

    auto item_runner = std::make_unique<TPCCBenchmarkItemRunner>(config, WAREHOUSES);
    auto benchmark_runner = std::make_shared<BenchmarkRunner>(*config, std::move(item_runner),
                                                              std::make_unique<TPCCTableGenerator>(WAREHOUSES, config),
                                                              context);

    Hyrise::get().benchmark_runner = benchmark_runner;
  }
  //
  //  /TPC-C
  //

  return 0;
}
