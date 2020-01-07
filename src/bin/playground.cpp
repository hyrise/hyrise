
#include <logical_query_plan/mock_node.hpp>
#include <synthetic_table_generator.hpp>
#include <cost_calibration/table_generator.hpp>
#include <cost_calibration/lqp_generator/table_scan.hpp>
#include <cost_calibration/measurement_export.hpp>

#include "types.hpp"
#include "hyrise.hpp"

using namespace opossum;  // NOLINT

int main() {
  auto table_config = std::make_shared<TableGeneratorConfig>(TableGeneratorConfig{
          {DataType::Double, DataType::Float, DataType::Int, DataType::Long, DataType::String, DataType::Null},
          {EncodingType::Dictionary, EncodingType::FixedStringDictionary, EncodingType ::FrameOfReference, EncodingType::LZ4, EncodingType::RunLength, EncodingType::Unencoded},
          {ColumnDataDistribution::make_uniform_config(0.0, 1000.0)},
          {1000},
          {100, 1000, 10000, 100000, 1000000}
  });
  auto table_generator = TableGenerator(table_config);

  const auto tables = table_generator.generate();

  for (const auto &table : tables){
    Hyrise::get().storage_manager.add_table(table->getTableName(), table->getTable());

    auto lqp_generator = TableScanLQPGenerator(table);
    lqp_generator.generate();
    lqp_generator.execute();

    Hyrise::get().storage_manager.drop_table(table->getTableName());
  }

}