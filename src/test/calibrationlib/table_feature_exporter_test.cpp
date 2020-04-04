#include <fstream>
#include <boost/algorithm/string.hpp>
#include "base_test.hpp"
#include "calibration_table_generator.hpp"
#include "table_feature_exporter.hpp"

namespace opossum {

class TableFeatureExporterTest : public BaseTest {
 protected:
  void SetUp() override {
    _table_config = std::make_shared<TableGeneratorConfig>(
        TableGeneratorConfig{{DataType::Int, DataType::String},
                             {EncodingType::Dictionary, EncodingType::LZ4},
                             {ColumnDataDistribution::make_uniform_config(0.0, 100.0)},
                             {40, 100},
                             {100, 250}});

    // We use the calibration table generator to generate values too
    const auto table_generator = CalibrationTableGenerator(_table_config);
    _tables = table_generator.generate();

    TableFeatureExporter feature_exporter = TableFeatureExporter(_dir_path);

    // Export data of table
    for (const auto& table : _tables) {
      feature_exporter.export_table(table);
    }
  }

  std::vector<std::shared_ptr<const CalibrationTableWrapper>> _tables;
  std::shared_ptr<TableGeneratorConfig> _table_config;
  const std::string _dir_path = (std::filesystem::temp_directory_path() / "calibrationTest").string();

  bool equal_headers(std::string line, std::vector<std::string> expected_headers) {
    std::vector<std::string> row_values;
    boost::split(row_values, line, boost::is_any_of(","));

    const auto num_headers = expected_headers.size();
    for (u_int64_t header_index = 0; header_index < num_headers; ++header_index) {
      if (expected_headers.at(header_index) != row_values.at(header_index)) {
        return false;
      }
    }
    return true;
  }

  void validate_file(std::string path, std::vector<std::string> headers, u_int64_t expected_row_count) {
    std::string line;
    std::ifstream f(path);
    std::getline(f, line);

    EXPECT_TRUE(equal_headers(line, headers));

    u_int64_t row_count = 0;
    while (std::getline(f, line)) {
      row_count++;
    }
    EXPECT_EQ(row_count, expected_row_count);
  }
};

// Following tests validate the header and the number of entries per file.

TEST_F(TableFeatureExporterTest, TableSegment) {
  const auto headers = std::vector<std::string>({"TABLE_NAME", "ROW_COUNT", "CHUNK_SIZE"});
  const auto expected_row_count = _tables.size();
  validate_file(_dir_path + "/table_meta.csv", headers, expected_row_count);
}

TEST_F(TableFeatureExporterTest, ColumnExport) {
  const auto headers = std::vector<std::string>({"TABLE_NAME", "COLUMN_NAME", "COLUMN_DATA_TYPE"});

  const auto table_count = _table_config->chunk_sizes.size() * _table_config->row_counts.size();
  const auto columns_per_table = _tables.at(0)->get_table()->column_count();  // all tables have same columns;

  const auto expected_row_count = table_count * columns_per_table;
  validate_file(_dir_path + "/column_meta.csv", headers, expected_row_count);
}

TEST_F(TableFeatureExporterTest, SegmentExport) {
  const auto headers =
      std::vector<std::string>({"TABLE_NAME", "COLUMN_NAME", "CHUNK_ID", "ENCODING_TYPE", "COMPRESSION_TYPE"});
  uint64_t expected_row_count = 0;
  for (const auto& wrapped_table : _tables) {
    const auto chunk_count = wrapped_table->get_table()->chunk_count();
    const auto column_count = wrapped_table->get_table()->column_count();
    expected_row_count += chunk_count * column_count;
  }
  validate_file(_dir_path + "/segment_meta.csv", headers, expected_row_count);
}
}  // namespace opossum
