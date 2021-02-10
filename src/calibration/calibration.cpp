#include "calibration_benchmark_runner.hpp"
#include "calibration_lqp_generator.hpp"
#include "calibration_table_generator.hpp"
#include "cli_config_parser.hpp"
#include "cxxopts.hpp"
#include "hyrise.hpp"
#include "operators/pqp_utils.hpp"
#include "types.hpp"

using namespace opossum;  // NOLINT

void execute_calibration(const std::string& data_path, const std::shared_ptr<BenchmarkConfig>& config);

int main(int argc, char** argv) {
  // Data generation settings
  //const std::vector<BenchmarkType> BENCHMARK_TYPES = {BenchmarkType::TPC_H, BenchmarkType::TPC_DS, BenchmarkType::TPC_C,
  //                                                    BenchmarkType::JCC_H, BenchmarkType::JOB};
  const std::map<std::string, BenchmarkType> BENCHMARK_TYPES{
      {"tpch", BenchmarkType::TPC_H}, {"tpcds", BenchmarkType::TPC_DS}, {"tpcc", BenchmarkType::TPC_C},
      {"jcch", BenchmarkType::JCC_H}, {"job", BenchmarkType::JOB},      {"calibration", BenchmarkType::Calibration}};

  // create benchmark config
  auto cli_options = BenchmarkRunner::get_basic_cli_options("What-If Clustering Statistics Generator");

  // clang-format off
  cli_options.add_options()
    ("s,scale", "Database scale factor (1.0 ~ 1GB)", cxxopts::value<float>()->default_value("1"))
    ("benchmark", "Benchmark to run. Choose one of tpch, tpcds, tpcc, jcch, job", cxxopts::value<std::string>());

  // clang-format on

  const auto cli_parse_result = cli_options.parse(argc, argv);

  const auto benchmark_name = cli_parse_result["benchmark"].as<std::string>();
  const auto& benchmark_type = BENCHMARK_TYPES.at(benchmark_name);

  auto config = std::make_shared<BenchmarkConfig>(CLIConfigParser::parse_cli_options(cli_parse_result));
  config->cache_binary_tables = false;

  constexpr int NUMBER_BENCHMARK_EXECUTIONS = 1;
  const auto scale_factor = cli_parse_result["scale"].as<float>();
  constexpr bool SKEW_JCCH = false;

  // Export directory
  std::string DATA_PATH = "./data/" + benchmark_name;
  std::filesystem::create_directories(DATA_PATH);

  auto start = std::chrono::system_clock::now();

  std::cout << "Generating data" << std::endl;
  if (benchmark_type != BenchmarkType::Calibration) {
    // Execute benchmark
    auto benchmark_runner = CalibrationBenchmarkRunner(DATA_PATH, config, SKEW_JCCH);
    std::cout << "Run " << magic_enum::enum_name(benchmark_type) << std::endl;
    benchmark_runner.run_benchmark(benchmark_type, scale_factor, NUMBER_BENCHMARK_EXECUTIONS);
  } else {
    execute_calibration(DATA_PATH, config);
  }

  const auto test_duration =
      std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now() - start).count();
  std::cout << "Generated data in " << test_duration << " s" << std::endl;
}

void execute_calibration(const std::string& data_path, const std::shared_ptr<BenchmarkConfig>& config) {
  std::cout << "Run calibration" << std::endl;
  const std::set<DataType> TABLE_DATA_TYPES = {DataType::Double, DataType::Float,  DataType::Int,
                                               DataType::Long,   DataType::String, DataType::Null};
  const std::set<EncodingType> COLUMN_ENCODING_TYPES = {EncodingType::Dictionary};
  const std::vector<ColumnDataDistribution> COLUMN_DATA_DISTRIBUTIONS = {
      ColumnDataDistribution::make_uniform_config(0.0, 300'000.0)};
  const std::set<ChunkOffset> CHUNK_SIZES = {config->chunk_size};
  const std::set<int> ROW_COUNTS = {100'000, 6'000'000};
  const auto table_config = std::make_shared<TableGeneratorConfig>(TableGeneratorConfig{
      TABLE_DATA_TYPES, COLUMN_ENCODING_TYPES, COLUMN_DATA_DISTRIBUTIONS, CHUNK_SIZES, ROW_COUNTS});

  std::cout << " - Generating tables" << std::endl;
  auto table_generator = CalibrationTableGenerator(table_config);
  auto tables = table_generator.generate();

  std::cout << " - Generating LQPS" << std::endl;
  auto feature_exporter = OperatorFeatureExporter(data_path);
  auto lqp_generator = CalibrationLQPGenerator();
  auto table_exporter = TableFeatureExporter(data_path);
  for (const auto& table : tables) {
    Hyrise::get().storage_manager.add_table(table->get_name(), table->get_table());
    lqp_generator.generate(OperatorType::TableScan, table);
  }
  lqp_generator.generate_joins(tables);
  lqp_generator.generate_aggregates(tables);
  const auto lqps = lqp_generator.lqps();
  const auto lqp_count = lqps.size();

  std::cout << " - Running " << lqps.size() << " LQPs" << std::endl << "   ";
  size_t latest_percentage = 0;
  size_t current_lqp = 1;
  const auto transaction_context = Hyrise::get().transaction_manager.new_transaction_context(AutoCommit::Yes);

  for (size_t i{0}; i < 50; ++i) {
    std::cout << "-";
  }
  std::cout << std::endl << "   ";

  for (const std::shared_ptr<AbstractLQPNode>& lqp : lqps) {
    const auto pqp = LQPTranslator{}.translate_node(lqp);
    pqp->set_transaction_context_recursively(transaction_context);
    const auto tasks = OperatorTask::make_tasks_from_operator(pqp);
    Hyrise::get().scheduler()->schedule_and_wait_for_tasks(tasks);

    // Export PQP directly after execution
    feature_exporter.export_to_csv(pqp);

    // clear outputs to free up space
    visit_pqp(pqp, [&](const auto& op) {
      op->clear_output();
      return PQPVisitation::VisitInputs;
    });
    const auto percentage = (static_cast<float>(current_lqp) / static_cast<float>(lqp_count)) * 100;
    if (percentage >= latest_percentage + 2) {
      latest_percentage += 2;
      std::cout << "|" << std::flush;
    }
    ++current_lqp;
  }

  std::cout << std::endl << "Exporting data for calibration" << std::endl;
  feature_exporter.flush();
  for (const auto& table : tables) {
    table_exporter.export_table(table);
    Hyrise::get().storage_manager.drop_table(table->get_name());
  }
  table_exporter.flush();
}
