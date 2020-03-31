#include "../../calibrationlib/calibration_table_wrapper.hpp"
#include "base_test.hpp"
#include "storage/table.hpp"

namespace opossum {

class CalibrationTableWrapperTest : public BaseTest {
 protected:
  void SetUp() override {
    TableColumnDefinitions column_definitions;

    column_definitions.emplace_back("column_1", DataType::Int, false);
    column_definitions.emplace_back("column_2", DataType::String, false);

    _unwrapped_table = std::make_shared<Table>(column_definitions, TableType::Data, 2);
    _table_name = "test_table";
    _column_distributions =
        std::vector<ColumnDataDistribution>({ColumnDataDistribution::make_uniform_config(0.0, 100.0),
                                             ColumnDataDistribution::make_uniform_config(0.0, 100.0)});

    _wrapped_table = std::make_shared<CalibrationTableWrapper>(_unwrapped_table, _table_name, _column_distributions);
  }

  std::shared_ptr<CalibrationTableWrapper> _wrapped_table;
  std::shared_ptr<Table> _unwrapped_table;
  std::string _table_name;
  std::vector<ColumnDataDistribution> _column_distributions;
};

TEST_F(CalibrationTableWrapperTest, GettersReturnCorrectValues) {
  EXPECT_EQ(_wrapped_table->get_table().get(), _unwrapped_table.get());
  EXPECT_EQ(_wrapped_table->get_name(), _table_name);
  EXPECT_TRUE(_wrapped_table->get_column_data_distribution(ColumnID{0}) == _column_distributions.at(0));
}

}  // namespace opossum
