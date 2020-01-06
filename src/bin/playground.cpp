
#include <logical_query_plan/mock_node.hpp>
#include <synthetic_table_generator.hpp>
#include <cost_calibration/table_generator.hpp>
#include <cost_calibration/lqp_generator/table_scan.hpp>

#include "types.hpp"


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
          {100, 1000, 10000, 100000, 100}
  });
  auto table_generator = TableGenerator(table_config);
  const auto table = (table_generator.generate().at(0));

  const auto lqp_gen = TableScanLQPGenerator(table);
  lqp_gen.execute();
  // lqp_gen.get();
}