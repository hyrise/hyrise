
#include <logical_query_plan/mock_node.hpp>
#include <synthetic_table_generator.hpp>
#include <cost_calibration/table_generator.hpp>
#include <cost_calibration/measurement_export.hpp>
#include <logical_query_plan/lqp_translator.hpp>
#include <scheduler/operator_task.hpp>
#include <cost_calibration/lqp_generator.hpp>
#include <operators/table_wrapper.hpp>
#include <operators/export_csv.hpp>
#include "types.hpp"
#include "hyrise.hpp"

using namespace opossum;  // NOLINT

// START FIX
// The following needs to stay here in order to be able to compile with GCC
// TODO find you actual problem and fix it
std::vector<DataType> data_types_collection;
std::vector<SegmentEncodingSpec> segment_encoding_spec_collection;
std::vector<ColumnDataDistribution> column_data_distribution_collection;
std::vector<int> chunk_offsets;
std::vector<int> row_counts;
std::vector<std::string> column_names;

std::vector<std::shared_ptr<opossum::Table>> generate() {
    auto tables = std::vector<std::shared_ptr<opossum::Table>>();
    auto table_generator = std::make_shared<SyntheticTableGenerator>();

    for (int chunk_size : chunk_offsets) {
        for (int row_count : row_counts){
            const auto table = table_generator->generate_table(
                    column_data_distribution_collection,
                    data_types_collection,
                    row_count,
                    chunk_size,
                    {segment_encoding_spec_collection},
                    {column_names},
                    UseMvcc::Yes    // MVCC = Multiversion concurrency control
            );
            tables.emplace_back(table);
        }
    }
    return tables;
}
// END FIX


int main() {
  auto table_config = std::make_shared<TableGeneratorConfig>(TableGeneratorConfig{
          {DataType::Double, DataType::Float, DataType::Int, DataType::Long, DataType::String, DataType::Null},
          {EncodingType::Dictionary, EncodingType::FixedStringDictionary, EncodingType ::FrameOfReference, EncodingType::LZ4, EncodingType::RunLength, EncodingType::Unencoded},
          {ColumnDataDistribution::make_uniform_config(0.0, 1000.0)},
          {1000},
          {100, 1000, 10000, 100000}
  });
  auto table_generator = TableGenerator(table_config);
  const auto tables = table_generator.generate();

  auto const path = ".";
  auto measurement_export = MeasurementExport(path);
  auto lqp_generator = LQPGenerator();

  for (const auto &table : tables){
    Hyrise::get().storage_manager.add_table(table->get_name(), table->get_table());

    auto table_wrapper = std::make_shared<TableWrapper>(table->get_table());
    table_wrapper->execute();
    auto ex = std::make_shared<opossum::ExportCsv>(table_wrapper, "./test.csv");
    ex->execute();

    const auto lqps = lqp_generator.generate(OperatorType::TableScan, table);

    //Execution of lpqs; In the future a good scheduler as replacement for following code would be awesome.
    for (const std::shared_ptr<AbstractLQPNode>& lqp : lqps) {
      const auto pqp = LQPTranslator{}.translate_node(lqp);
      const auto tasks = OperatorTask::make_tasks_from_operator(pqp, CleanupTemporaries::Yes);
      Hyrise::get().scheduler()->schedule_and_wait_for_tasks(tasks);

      //Execute LQP directly after generation
      measurement_export.export_to_csv(pqp);
    }

    table->export_table_meta_data(path); //TODO meta_data_only_export
    Hyrise::get().storage_manager.drop_table(table->get_name());
  }

}