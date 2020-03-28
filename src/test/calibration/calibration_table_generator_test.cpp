#include "base_test.hpp"

#include <synthetic_table_generator.hpp>
#include "../../calibration/calibration_table_generator.hpp"
#include "storage/table.hpp"

namespace opossum {

class CalibrationTableGeneratorTest : public BaseTest {
 protected:
  struct CalibrationTableGeneratorTableSetting {
    int chunk_size;
    int row_count;
  };

  struct CalibrationTableGeneratorColumnSetting {
    DataType data_type;
    EncodingType encoding_type;
    ColumnDataDistribution column_data_distribution;
  };

  void SetUp() override {
    auto table_config = std::make_shared<TableGeneratorConfig>(TableGeneratorConfig{
        {DataType::Float, DataType::Double, DataType::String},
        {EncodingType::Dictionary, EncodingType::RunLength},
        {ColumnDataDistribution::make_uniform_config(0.0, 100.0), ColumnDataDistribution::make_pareto_config(1, 1)},
        {100000, 500000},
        {1000, 10000}});

    _generator = std::make_shared<CalibrationTableGenerator>(table_config);
  }

  std::shared_ptr<CalibrationTableGenerator> _generator;
};

TEST_F(CalibrationTableGeneratorTest, GenerateExpectedTables) {
  // Reminder: Order is important here (acending order; left to right e.g. chunk_size )
  const auto expected_tables = std::vector<CalibrationTableGeneratorTableSetting>(
      {{100000, 1000}, {100000, 10000}, {500000, 1000}, {500000, 10000}});

  const auto uniform_column_distribution = ColumnDataDistribution::make_uniform_config(0.0, 100.0);
  const auto pareto_column_distribution = ColumnDataDistribution::make_pareto_config(1, 1);

  // Reminder: Order is important here (order is determined by the order of the respective enums)
  // In the implementation we convert a sorted map into a vector -> enums are ordered.
  const auto expected_columns = std::vector<CalibrationTableGeneratorColumnSetting>(
      {{DataType::Float, EncodingType::Dictionary, uniform_column_distribution},
       {DataType::Float, EncodingType::Dictionary, pareto_column_distribution},
       {DataType::Float, EncodingType::RunLength, uniform_column_distribution},
       {DataType::Float, EncodingType::RunLength, pareto_column_distribution},
       {DataType::Double, EncodingType::Dictionary, uniform_column_distribution},
       {DataType::Double, EncodingType::Dictionary, pareto_column_distribution},
       {DataType::Double, EncodingType::RunLength, uniform_column_distribution},
       {DataType::Double, EncodingType::RunLength, pareto_column_distribution},
       {DataType::String, EncodingType::Dictionary, uniform_column_distribution},
       {DataType::String, EncodingType::Dictionary, pareto_column_distribution},
       {DataType::String, EncodingType::RunLength, uniform_column_distribution},
       {DataType::String, EncodingType::RunLength, pareto_column_distribution}});

  const auto wrapped_tables = _generator->generate();

  // Generate function must generate equal number of tables
  EXPECT_EQ(wrapped_tables.size(), expected_tables.size());

  const auto number_of_tables = expected_tables.size();
  // Check if settings for all tables are as expected
  for (uint16_t table_index = 0; table_index < number_of_tables; ++table_index) {
    const auto table = wrapped_tables.at(table_index)->get_table();
    const auto expected_table_setting = expected_tables.at(table_index);

    EXPECT_EQ(table->row_count(), expected_table_setting.row_count);
    // Check if columns have correct data types
    const auto number_of_columns = expected_columns.size();
    EXPECT_EQ(table->column_count(), number_of_columns);

    for (uint16_t column_index = 0; column_index < number_of_columns; ++column_index) {
      const auto expected_data_type = expected_columns.at(column_index).data_type;
      const auto expected_encoding = expected_columns.at(column_index).encoding_type;
      ColumnDataDistribution expected_column_data_distribution =
          expected_columns.at(column_index).column_data_distribution;

      const auto column_id = ColumnID{column_index};
      EXPECT_EQ(table->column_data_type(column_id), expected_data_type);

      // Column Distribution
      ColumnDataDistribution true_column_data_distribution =
          wrapped_tables.at(table_index)->get_column_data_distribution(column_id);

      EXPECT_TRUE(true_column_data_distribution == expected_column_data_distribution);

      // at the moment there is no easy way to validate the encoding type here; that is why we
      auto const segment = table->get_chunk(ChunkID{0})->get_segment(column_id);
      const auto encoded_segment =
          std::dynamic_pointer_cast<BaseEncodedSegment>(segment);  // dynamic_past must be valid

      const auto true_encoding = encoded_segment->encoding_type();

      EXPECT_EQ(true_encoding, expected_encoding);
    }
  }
}
}  // namespace opossum
