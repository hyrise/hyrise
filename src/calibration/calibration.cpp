#include "calibration_benchmark_runner.hpp"

#include "cli_config_parser.hpp"
#include "cxxopts.hpp"
#include "hyrise.hpp"
#include "types.hpp"


using namespace opossum;  // NOLINT

int main(int argc, char** argv) {
  // Data generation settings
  //const std::vector<BenchmarkType> BENCHMARK_TYPES = {BenchmarkType::TPC_H, BenchmarkType::TPC_DS, BenchmarkType::TPC_C,
  //                                                    BenchmarkType::JCC_H, BenchmarkType::JOB};  
  const std::map<std::string, BenchmarkType> BENCHMARK_TYPES {{"tpch", BenchmarkType::TPC_H}, {"tpcds", BenchmarkType::TPC_DS}, {"tpcc", BenchmarkType::TPC_C}, {"jcch", BenchmarkType::JCC_H}, {"job", BenchmarkType::JOB}};

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


  // Execute calibration
  std::cout << "Generating data" << std::endl;
  auto start = std::chrono::system_clock::now();
  auto benchmark_runner = CalibrationBenchmarkRunner(DATA_PATH, config, SKEW_JCCH);
  std::cout << "Run " << magic_enum::enum_name(benchmark_type) << std::endl;
  benchmark_runner.run_benchmark(benchmark_type, scale_factor, NUMBER_BENCHMARK_EXECUTIONS);

  const auto test_duration =
      std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now() - start).count();
  std::cout << "Generated data in " << test_duration << " s" << std::endl;
}
