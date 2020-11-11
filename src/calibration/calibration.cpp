#include "calibration_benchmark_runner.hpp"
#include "hyrise.hpp"
#include "types.hpp"

using namespace opossum;  // NOLINT

int main() {
  // Export directory
  constexpr auto DATA_PATH = "./data";
  std::filesystem::create_directories(DATA_PATH);

  // Data generation settings
  //const std::vector<BenchmarkType> BENCHMARK_TYPES = {BenchmarkType::TPC_H, BenchmarkType::TPC_DS, BenchmarkType::TPC_C,
  //                                                    BenchmarkType::JCC_H, BenchmarkType::JOB};
  const std::vector<BenchmarkType> BENCHMARK_TYPES = {BenchmarkType::TPC_H};

  constexpr float SCALE_FACTOR = 1.0f;
  constexpr int NUMBER_BENCHMARK_EXECUTIONS = 1;
  constexpr int NUMBER_BENCHMARK_ITEM_RUNS = 10;
  constexpr int NUMBER_JOB_ITEM_RUNS = 2;
  constexpr bool SKEW_JCCH = false;

  // Execute calibration
  std::cout << "Generating data" << std::endl;
  auto start = std::chrono::system_clock::now();
  auto benchmark_runner = CalibrationBenchmarkRunner(DATA_PATH, SKEW_JCCH);
  for (const auto type : BENCHMARK_TYPES) {
    std::cout << "Run " << magic_enum::enum_name(type) << std::endl;
    const auto item_runs = type == BenchmarkType::JOB ? NUMBER_JOB_ITEM_RUNS : NUMBER_BENCHMARK_ITEM_RUNS;
    benchmark_runner.run_benchmark(type, SCALE_FACTOR, NUMBER_BENCHMARK_EXECUTIONS, item_runs);
  }
  const auto test_duration =
      std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now() - start).count();
  std::cout << "Generated data in " << test_duration << " s" << std::endl;
}
