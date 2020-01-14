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

using namespace opossum;  // NOLINT

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

  const auto lqp_generator = LQPGenerator();

  auto const path = ".";
  const auto measurement_export = MeasurementExport(path);
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

}