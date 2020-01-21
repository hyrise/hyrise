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
#include "measurement_export.hpp"
#include "table_generator.hpp"
#include "table_export.hpp"

#include "benchmark_config.hpp"
#include "benchmark_runner.hpp"

using namespace opossum;  // NOLINT

void export_tcph(){
  constexpr auto USE_PREPARED_STATEMENTS = false;
  const auto measurement_export = MeasurementExport("./.data/test");
  const auto table_export = TableExport("./.data/test");

  auto config = std::make_shared<BenchmarkConfig>(BenchmarkConfig::get_default_config());
  config->max_runs = 1;
  config->enable_visualization = false;
  config->chunk_size = 100'000;
  config->cache_binary_tables = true;

  auto const SCALE_FACTOR = 0.01f;
  // const std::vector<BenchmarkItemID> tpch_query_ids_benchmark = {BenchmarkItemID{5}};
  // auto item_runner = std::make_unique<TPCHBenchmarkItemRunner>(config, USE_PREPARED_STATEMENTS, SCALE_FACTOR, tpch_query_ids_benchmark);
  auto item_runner = std::make_unique<TPCHBenchmarkItemRunner>(config, USE_PREPARED_STATEMENTS, SCALE_FACTOR);
  auto benchmark_runner = std::make_shared<BenchmarkRunner>(
          *config, std::move(item_runner), std::make_unique<TPCHTableGenerator>(SCALE_FACTOR, config), BenchmarkRunner::create_context(*config));
  Hyrise::get().benchmark_runner = benchmark_runner;
  benchmark_runner->run();

  // TODO fix issue in range expression
  // hyrise/src/calibration/calibration.cpp:42:56: error: invalid range expression of type 'opossum::Cache<std::shared_ptr<opossum::AbstractOperator>, std::__cxx11::basic_string<char, std::char_traits<char>, std::allocator<char> > >'; no viable 'begin' function available
//  for (const auto& [query_string, physical_query_plan] : *SQLPipelineBuilder::default_pqp_cache) {
//    measurement_export.export_to_csv(physical_query_plan);
//    //std::cout << *physical_query_plan << std::endl;
//  }


  const std::vector<std::string> table_names = Hyrise::get().storage_manager.table_names();
  for (const auto &table_name : table_names){
    auto table = Hyrise::get().storage_manager.get_table(table_name);
    table_export.export_table(std::make_shared<CalibrationTableWrapper>(CalibrationTableWrapper(table, table_name)));
  }
}

int main() {
  auto table_config = std::make_shared<TableGeneratorConfig>(TableGeneratorConfig{
          {DataType::Double, DataType::Float, DataType::Int, DataType::Long, DataType::String, DataType::Null},
          {EncodingType::Dictionary, EncodingType::FixedStringDictionary, EncodingType ::FrameOfReference, EncodingType::LZ4, EncodingType::RunLength, EncodingType::Unencoded},
          {ColumnDataDistribution::make_uniform_config(0.0, 1000.0)},
          {1000}, //TODO rename to chunk_size
          {100, 1000, 10000, 100000}
  });
  auto table_generator = TableGenerator(table_config);
  const auto tables = table_generator.generate();

  auto const path = "./.data/train";
  const auto measurement_export = MeasurementExport(path);

  export_tcph();

  /*

  const auto lqp_generator = LQPGenerator();
  const auto table_export = TableExport(path);

  for (const auto &table : tables){
    Hyrise::get().storage_manager.add_table(table->get_name(), table->get_table());

    const auto lqps = lqp_generator.generate(OperatorType::TableScan, table);

    // Execution of lpqs; In the future a good scheduler as replacement for following code would be awesome.
    for (const std::shared_ptr<AbstractLQPNode>& lqp : lqps) {
      const auto pqp = LQPTranslator{}.translate_node(lqp);
      const auto tasks = OperatorTask::make_tasks_from_operator(pqp, CleanupTemporaries::No);
      Hyrise::get().scheduler()->schedule_and_wait_for_tasks(tasks);

      // Execute LQP directly after generation
      measurement_export.export_to_csv(pqp);
    }
    table_export.export_table(table);
    Hyrise::get().storage_manager.drop_table(table->get_name());
  }
  */
}