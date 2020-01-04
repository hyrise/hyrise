
#include <logical_query_plan/mock_node.hpp>
#include <synthetic_table_generator.hpp>
#include <cost_calibration/table_generator.hpp>
#include <cost_calibration/lqp_generator/table_scan.hpp>

#include "types.hpp"


using namespace opossum;  // NOLINT



std::vector<DataType> data_types_collection;
std::vector<SegmentEncodingSpec> segment_encoding_spec_collection;
std::vector<ColumnDataDistribution> column_data_distribution_collection;
std::vector<int> chunk_offsets;
std::vector<int> row_counts;
std::vector<std::string> column_names;

    void populate(std::shared_ptr<TableGeneratorConfig> config) {
        // Generate all possible permutations of column types
        for (DataType data_type : config->data_types){
            for (EncodingType encoding_type : config->encoding_types){

                if (encoding_supports_data_type(encoding_type, data_type)){
                    for (ColumnDataDistribution column_data_distribution : config->column_data_distribution){
                        data_types_collection.emplace_back(data_type);
                        segment_encoding_spec_collection.emplace_back(SegmentEncodingSpec(encoding_type));
                        column_data_distribution_collection.emplace_back(column_data_distribution);

                        std::stringstream column_name_str;

                        column_name_str << data_type << "_" << encoding_type << "_" << column_data_distribution;
                        column_names.emplace_back(column_name_str.str());
                    }
                }

            }
        }

        chunk_offsets.assign(config->chunk_offsets.begin(), config->chunk_offsets.end());
        row_counts.assign(config->row_counts.begin(), config->row_counts.end());
    }

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

int main() {

  auto table_config = std::make_shared<TableGeneratorConfig>(TableGeneratorConfig{
          {DataType::Double, DataType::Float, DataType::Int, DataType::Long, DataType::String, DataType::Null},
          {EncodingType::Dictionary, EncodingType::FixedStringDictionary, EncodingType ::FrameOfReference, EncodingType::LZ4, EncodingType::RunLength, EncodingType::Unencoded},
          {ColumnDataDistribution::make_uniform_config(0.0, 1000.0)},
          {100000},
          {100, 1000, 10000, 100000, 1000000}
  });

  auto table_generator = TableGenerator(table_config);
  auto tables = table_generator.generate();

}