#include <benchmark_config.hpp>
#include <tpch/tpch_benchmark_item_runner.hpp>
#include <tpch/tpch_table_generator.hpp>
#include <sql/sql_pipeline_builder.hpp>
#include "hyrise.hpp"
#include "types.hpp"
#include "logical_query_plan/lqp_translator.hpp"
#include "logical_query_plan/mock_node.hpp"
#include "operators/table_wrapper.hpp"
#include "scheduler/operator_task.hpp"

#include "lqp_generator.hpp"
#include "operator_feature_export.hpp"
#include "calibration_table_generator.hpp"
#include "table_feature_export.hpp"
#include "calibration_benchmark_runner.hpp"

#include "benchmark_config.hpp"
#include "benchmark_runner.hpp"

using namespace opossum;  // NOLINT

int main() {

  auto table_config = std::make_shared<TableGeneratorConfig>(TableGeneratorConfig{
          {DataType::Double, DataType::Float, DataType::Int, DataType::Long, DataType::String, DataType::Null},
          {EncodingType::Dictionary},
          {ColumnDataDistribution::make_uniform_config(0.0, 1000.0)},
          {100000},
          {1500, 3000, 6000, 10000, 20000, 30000, 60175, 25, 15000, 2000, 8000, 5, 100}
  });
  auto table_generator = CalibrationTableGenerator(table_config);
  const auto tables = table_generator.generate();

  auto const path_train = "./data/train";
  auto const path_test = "./data/test";

  const auto feature_export = OperatorFeatureExport(path_train);
  auto lqp_generator = LQPGenerator();
  auto table_export = TableFeatureExport(path_train);

  auto benchmark_runner = CalibrationBenchmarkRunner(path_test);

  benchmark_runner.run_benchmark(BenchmarkType::TCPH, 0.01f, 10);

  for (const auto &table : tables) {
    Hyrise::get().storage_manager.add_table(table->get_name(), table->get_table());

    lqp_generator.generate(OperatorType::TableScan, table);
  }

    const auto lqps = lqp_generator.get_lqps();
    // Execution of lpqs; In the future a good scheduler as replacement for following code would be awesome.
    for (const std::shared_ptr<AbstractLQPNode>& lqp : lqps) {
      const auto pqp = LQPTranslator{}.translate_node(lqp);
      const auto tasks = OperatorTask::make_tasks_from_operator(pqp, CleanupTemporaries::No);
      Hyrise::get().scheduler()->schedule_and_wait_for_tasks(tasks);

      // Execute LQP directly after generation
      feature_export.export_to_csv(pqp);
    }

  for (const auto &table : tables){
    table_export.export_table(table);
    Hyrise::get().storage_manager.drop_table(table->get_name());
  }
}
