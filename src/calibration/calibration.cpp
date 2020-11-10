#include "calibration_benchmark_runner.hpp"
#include "calibration_lqp_generator.hpp"
#include "calibration_table_generator.hpp"
#include "hyrise.hpp"
#include "logical_query_plan/lqp_translator.hpp"
#include "logical_query_plan/mock_node.hpp"
#include "operator_feature_exporter.hpp"
#include "operators/pqp_utils.hpp"
#include "scheduler/operator_task.hpp"
#include "sql/sql_pipeline_builder.hpp"
#include "table_feature_exporter.hpp"
#include "types.hpp"

using namespace opossum;  // NOLINT

int main() {
  // Export directories
  constexpr auto PATH_TRAIN = "./data/train";
  constexpr auto PATH_TEST = "./data/test";

  std::filesystem::create_directories(PATH_TRAIN);
  std::filesystem::create_directories(PATH_TEST);

  // table generation settings
  const std::set<DataType> TABLE_DATA_TYPES = {DataType::Double, DataType::Float,  DataType::Int,
                                               DataType::Long,   DataType::String, DataType::Null};
  const std::set<EncodingType> COLUMN_ENCODING_TYPES = {EncodingType::Dictionary};
  const std::vector<ColumnDataDistribution> COLUMN_DATA_DISTRIBUTIONS = {
      ColumnDataDistribution::make_uniform_config(0.0, 1000.0)};
  const std::set<ChunkOffset> CHUNK_SIZES = {Chunk::DEFAULT_SIZE};
  const std::set<int> ROW_COUNTS = {5,       25,      100,       1500,      2000,      3000,     6000,   8000,
                                    10'000,  15'000,  20'000,    30'000,    40'000,    50'000,   60'175, 100'000,
                                    250'000, 500'000, 1'000'000, 2'500'000, 5'000'000, 6'000'000};
  const bool GENERATE_SORTED_TABLES = true;
  const bool GENERATE_FOREIGN_KEY_TABLES = true;
  const int MAX_FOREIGN_KEY_TABLE_SIZE = 3000;

  // test data generation settings
  constexpr bool GENERATE_TEST_DATA = true;
  const std::vector<BenchmarkType> BENCHMARK_TYPES = {BenchmarkType::TPC_H, BenchmarkType::TPC_DS, BenchmarkType::TPC_C,
                                                      BenchmarkType::JOB, BenchmarkType::JCC_H};
  constexpr float SCALE_FACTOR = 1.0f;
  constexpr int NUMBER_BENCHMARK_EXECUTIONS = 1;
  constexpr int NUMBER_BENCHMARK_ITEM_RUNS = 100;
  constexpr int NUMBER_JOB_ITEM_RUNS = 2;
  constexpr bool SKEW_JCCH = false;

  // Execute calibration
  auto table_config = std::make_shared<TableGeneratorConfig>(
      TableGeneratorConfig{TABLE_DATA_TYPES, COLUMN_ENCODING_TYPES, COLUMN_DATA_DISTRIBUTIONS, CHUNK_SIZES, ROW_COUNTS,
                           GENERATE_SORTED_TABLES, GENERATE_FOREIGN_KEY_TABLES, MAX_FOREIGN_KEY_TABLE_SIZE});

  std::cout << "Generating tables" << std::endl;
  auto table_generation_start = std::chrono::system_clock::now();
  auto table_generator = CalibrationTableGenerator(table_config);
  const auto tables = table_generator.generate();
  const auto table_generation_duration =
      std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now() - table_generation_start)
          .count();
  std::cout << "Generated tables in " << table_generation_duration << " s" << std::endl;

  if (GENERATE_TEST_DATA) {
    std::cout << "Generating test data" << std::endl;
    auto test_start = std::chrono::system_clock::now();
    auto benchmark_runner = CalibrationBenchmarkRunner(PATH_TEST, SKEW_JCCH);
    for (const auto type : BENCHMARK_TYPES) {
      std::cout << "- Run " << magic_enum::enum_name(type) << std::endl;
      const auto item_runs = type == BenchmarkType::JOB ? NUMBER_JOB_ITEM_RUNS : NUMBER_BENCHMARK_ITEM_RUNS;
      benchmark_runner.run_benchmark(type, SCALE_FACTOR, NUMBER_BENCHMARK_EXECUTIONS, item_runs);
    }
    const auto test_duration =
        std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now() - test_start).count();
    std::cout << "Generated test data in " << test_duration << " s" << std::endl;
  }

  std::cout << "Generating training data" << std::endl;
  std::cout << "- Generating LQPS" << std::endl;

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
  std::cout << "- Generated LQPs in " << generation_duration << " s" << std::endl;

  std::cout << "- Running queries" << std::endl;
  auto training_start = std::chrono::system_clock::now();
  const auto transaction_context = Hyrise::get().transaction_manager.new_transaction_context(AutoCommit::Yes);
  const auto lqps = lqp_generator.get_lqps();
  for (const std::shared_ptr<AbstractLQPNode>& lqp : lqps) {
    const auto pqp = LQPTranslator{}.translate_node(lqp);
    pqp->set_transaction_context_recursively(transaction_context);
    const auto tasks = OperatorTask::make_tasks_from_operator(pqp);
    Hyrise::get().scheduler()->schedule_and_wait_for_tasks(tasks);

    // Export PQP directly after execution
    feature_exporter.export_to_csv(pqp);

    // clear outputs to free up space
    visit_pqp(pqp, [&](const auto& node) {
      node->clear_output();
      return PQPVisitation::VisitInputs;
    });
  }

  const auto training_duration =
      std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now() - training_start).count();
  std::cout << "- Ran queries in " << training_duration << " s" << std::endl;

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
