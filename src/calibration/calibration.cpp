#include "hyrise.hpp"
#include "logical_query_plan/lqp_translator.hpp"
#include "logical_query_plan/mock_node.hpp"
#include "scheduler/operator_task.hpp"
#include "types.hpp"

#include "calibration_benchmark_runner.hpp"
#include "calibration_lqp_generator.hpp"
#include "calibration_table_generator.hpp"
#include "operator_feature_exporter.hpp"
#include "table_feature_exporter.hpp"

using namespace opossum;  // NOLINT

int main() {
  // Export directories
  constexpr auto PATH_TRAIN = "./data/train";
  constexpr auto PATH_TEST = "./data/test";

  // table generation settings
  const std::set<DataType> TABLE_DATA_TYPES = {DataType::Double, DataType::Float,  DataType::Int,
                                               DataType::Long,   DataType::String, DataType::Null};
  const std::set<EncodingType> COLUMN_ENCODING_TYPES = {EncodingType::Dictionary};
  const std::vector<ColumnDataDistribution> COLUMN_DATA_DISTRIBUTIONS = {
      ColumnDataDistribution::make_uniform_config(0.0, 1000.0)};
  const std::set<ChunkOffset> CHUNK_SIZES = {Chunk::DEFAULT_SIZE};
  const std::set<int> ROW_COUNTS = {1500, 3000, 6000, 10000, 20000, 30000, 60175, 25, 15000, 2000, 8000, 5, 100};
  const bool GENERATE_SORTED_TABLES = true;

  // test data generation settings
  constexpr bool GENERATE_TEST_DATA = true;
  constexpr BenchmarkType BENCHMARK_TYPE = BenchmarkType::TCPH;
  constexpr float SCALE_FACTOR = 0.01f;
  constexpr int NUMBER_BENCHMARK_EXECUTIONS = 1;

  // Execute calibration

  auto table_config = std::make_shared<TableGeneratorConfig>(
      TableGeneratorConfig{TABLE_DATA_TYPES, COLUMN_ENCODING_TYPES, COLUMN_DATA_DISTRIBUTIONS, CHUNK_SIZES, ROW_COUNTS,
                           GENERATE_SORTED_TABLES});

  auto table_generator = CalibrationTableGenerator(table_config);
  const auto tables = table_generator.generate();

  if (GENERATE_TEST_DATA) {
    std::cout << "- Generating test data" << std::endl;
    auto test_start = std::chrono::system_clock::now();
    auto benchmark_runner = CalibrationBenchmarkRunner(PATH_TEST);
    benchmark_runner.run_benchmark(BENCHMARK_TYPE, SCALE_FACTOR, NUMBER_BENCHMARK_EXECUTIONS);
    const auto test_duration =
      std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now() - test_start).count();
    std::cout << "- Generated test data in " << test_duration << " s" << std::endl;

  }

  std::cout << "- Generating training data" << std::endl;
  std::cout << "\t- Generating LQPS" << std::endl;
  auto generation_start = std::chrono::system_clock::now();

  auto feature_exporter = OperatorFeatureExporter(PATH_TRAIN);
  auto lqp_generator = CalibrationLQPGenerator();
  auto table_exporter = TableFeatureExporter(PATH_TRAIN);

  for (const auto& table : tables) {
    Hyrise::get().storage_manager.add_table(table->get_name(), table->get_table());

    lqp_generator.generate(OperatorType::TableScan, table);
  }

  lqp_generator.generate_joins(tables);
  const auto generation_duration =
      std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now() - generation_start).count();
  std::cout << "\t- Generated LQPs in " << generation_duration << " s" << std::endl;

  std::cout << "\t- Running queries" << std::endl;
  auto training_start = std::chrono::system_clock::now();
  const auto lqps = lqp_generator.get_lqps();
  for (const std::shared_ptr<AbstractLQPNode>& lqp : lqps) {
    const auto pqp = LQPTranslator{}.translate_node(lqp);
    const auto tasks = OperatorTask::make_tasks_from_operator(pqp);
    Hyrise::get().scheduler()->schedule_and_wait_for_tasks(tasks);

    // Export PQP directly after generation
    feature_exporter.export_to_csv(pqp);
  }

  const auto training_duration =
      std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now() - training_start).count();
  std::cout << "\t- Ran queries in " << training_duration << " s" << std::endl;

  std::cout << "- Exporting training data" << std::endl;
  auto export_start = std::chrono::system_clock::now();

  feature_exporter.flush();

  for (const auto& table : tables) {
    table_exporter.export_table(table);
    Hyrise::get().storage_manager.drop_table(table->get_name());
  }

  table_exporter.flush();

   const auto export_duration =
      std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now() - export_start).count();
  std::cout << "- Exported training data in " << export_duration << " s" << std::endl;
}
