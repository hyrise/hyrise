#include <fstream>

#include <boost/algorithm/string.hpp>
#include "base_test.hpp"
#include "calibration_lqp_generator.hpp"
#include "calibration_table_generator.hpp"
#include "operator_feature_exporter.hpp"
#include "storage/table.hpp"

namespace opossum {

class OperatorFeatureExporterTest : public BaseTest {
 protected:
  void SetUp() override {
    auto table_config = std::make_shared<TableGeneratorConfig>(
        TableGeneratorConfig{{DataType::Int, DataType::String},
                             {EncodingType::Dictionary},
                             {ColumnDataDistribution::make_uniform_config(0.0, 100.0)},
                             {100000},
                             {100}});

    const auto table_generator = CalibrationTableGenerator(table_config);
    _table = table_generator.generate().at(0);
    Hyrise::get().storage_manager.add_table(_table->get_name(), _table->get_table());
  }

  ~OperatorFeatureExporterTest() override {
    Hyrise::get().storage_manager.drop_table(_table->get_name());
    std::filesystem::remove_all(_dir_path);
  }

  void execute_and_export_pqps(std::vector<std::shared_ptr<AbstractLQPNode>> lqps) {
    for (const std::shared_ptr<AbstractLQPNode>& lqp : lqps) {
      const auto pqp = LQPTranslator{}.translate_node(lqp);
      const auto tasks = OperatorTask::make_tasks_from_operator(pqp);
      Hyrise::get().scheduler()->schedule_and_wait_for_tasks(tasks);
      _feature_exporter.export_to_csv(pqp);
    }
  }

  std::shared_ptr<const CalibrationTableWrapper> _table;

  std::string _dir_path = (std::filesystem::temp_directory_path() / "calibrationTest").string();
  OperatorFeatureExporter _feature_exporter = OperatorFeatureExporter(_dir_path);
};

// Check if performance data is added to csv.
// This does not check if the export of specific table_scan implementations works. (e.g. Table)
TEST_F(OperatorFeatureExporterTest, TableScanExport) {
  const auto headers = std::vector<std::string>({"INPUT_ROWS_LEFT", "OUTPUT_ROWS", "RUNTIME_NS", "SCAN_TYPE",
                                                 "TABLE_NAME", "COLUMN_NAME", "SCAN_IMPLEMENTATION"});

  // Generate LQPs to export
  auto lqp_generator = CalibrationLQPGenerator();
  lqp_generator.generate(OperatorType::TableScan, _table);
  auto const lqps = lqp_generator.get_lqps();

  execute_and_export_pqps(lqps);

  std::string line;
  std::ifstream f(_dir_path + "/TableScan.csv");
  std::getline(f, line);

  std::vector<std::string> row_values;
  boost::split(row_values, line, boost::is_any_of(","));

  // Check if we inserted the correct headers
  const auto num_headers = headers.size();
  for (u_int64_t header_index = 0; header_index < num_headers; ++header_index) {
    EXPECT_EQ(headers.at(header_index), row_values.at(header_index));
  }

  // Check if values where inserted (we executed some table_scans therefore we should export some values)
  while (std::getline(f, line)) {
    const auto raw_values = boost::split(row_values, line, boost::is_any_of(","));
    EXPECT_EQ(raw_values.size(), headers.size());
  }
}

}  // namespace opossum
