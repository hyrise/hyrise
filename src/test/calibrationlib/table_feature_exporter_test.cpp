#include <fstream>

#include <boost/algorithm/string.hpp>
#include "base_test.hpp"
#include "calibration_table_generator.hpp"
#include "table_feature_exporter.hpp"

namespace opossum {

class TableFeatureExporterTest : public BaseTest {
 protected:
  void SetUp() override {
    constexpr ChunkOffset CHUNK_SIZE = 2;

    _tables = std::vector<std::shared_ptr<const CalibrationTableWrapper>>({
        std::make_shared<CalibrationTableWrapper>(load_table("resources/test_data/tbl/float_int.tbl", CHUNK_SIZE),
                                                  "float_int"),
        std::make_shared<CalibrationTableWrapper>(load_table("resources/test_data/tbl/int_int.tbl", CHUNK_SIZE),
                                                  "int_int"),
        std::make_shared<CalibrationTableWrapper>(load_table("resources/test_data/tbl/string.tbl", CHUNK_SIZE),
                                                  "string"),
        std::make_shared<CalibrationTableWrapper>(
            load_table("resources/test_data/tbl/float_float_float.tbl", CHUNK_SIZE), "float_float_float"),
    });

    // Export data of table
    for (const auto& table : _tables) {
      _feature_exporter.export_table(table);
    }
  }

  std::vector<std::shared_ptr<const CalibrationTableWrapper>> _tables;
  const std::string _dir_path = (std::filesystem::temp_directory_path() / "calibrationTest").string();
  TableFeatureExporter _feature_exporter = TableFeatureExporter(_dir_path);

  bool equal_headers(std::string line, std::vector<std::string> expected_headers) {
    std::vector<std::string> row_values;
    boost::split(row_values, line, boost::is_any_of(","));

    const auto num_headers = expected_headers.size();
    for (size_t header_index = 0; header_index < num_headers; ++header_index) {
      if (expected_headers.at(header_index) != row_values.at(header_index)) {
        return false;
      }
    }
    return true;
  }

  void validate_file(std::string path, std::vector<std::string> headers, size_t expected_row_count) {
    std::string line;
    std::ifstream f(path);
    std::getline(f, line);

    EXPECT_TRUE(equal_headers(line, headers));

    size_t row_count = 0;
    while (std::getline(f, line)) {
      row_count++;
    }
    EXPECT_EQ(row_count, expected_row_count);
  }
};

// Following tests validate the header and the number of entries per file.
/*
TEST_F(TableFeatureExporterTest, TableSegment) {
  const auto headers = _feature_exporter.headers.at(TableFeatureExportType::TABLE);
  const auto expected_row_count = _tables.size();
  validate_file(_dir_path + "/table_meta.csv", headers, expected_row_count);
}

TEST_F(TableFeatureExporterTest, ColumnExport) {
  const auto headers = _feature_exporter.headers.at(TableFeatureExportType::COLUMN);

  const auto table_count = _tables.size();
  const auto columns_per_table = _tables.at(0)->get_table()->column_count();  // all tables have same columns;

  const auto expected_row_count = table_count * columns_per_table;
  validate_file(_dir_path + "/column_meta.csv", headers, expected_row_count);
}

TEST_F(TableFeatureExporterTest, SegmentExport) {
  const auto headers = _feature_exporter.headers.at(TableFeatureExportType::SEGMENT);
  uint64_t expected_row_count = 0;
  for (const auto& wrapped_table : _tables) {
    const auto chunk_count = wrapped_table->get_table()->chunk_count();
    const auto column_count = wrapped_table->get_table()->column_count();
    expected_row_count += chunk_count * column_count;
  }
  validate_file(_dir_path + "/segment_meta.csv", headers, expected_row_count);
}
*/
}  // namespace opossum
