#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "all_parameter_variant.hpp"
#include "common.hpp"
#include "operators/table_scan.hpp"
#include "operators/table_wrapper.hpp"
#include "optimizer/table_statistics.hpp"

namespace opossum {

class TableStatisticsTest : public BaseTest {
 protected:
  // Because of Operator::get_output() returns a const table, we need another way of adding statistics to it. Here
  // you go, have some nice boilerplate....
  struct TableWithStatistics {
    std::shared_ptr<const Table> table;
    std::shared_ptr<TableStatistics> statistics;
  };

  void SetUp() override {
    auto table = load_table("src/test/tables/int_float_double_string.tbl", 0);
    _table_a_with_statistics.statistics = std::make_shared<TableStatistics>(table);
    table->set_table_statistics(_table_a_with_statistics.statistics);
    _table_a_with_statistics.table = table;
  }

  /**
   * Predict output size of one TableScan with statistics and compare with actual output size of an actual TableScan.
   */
  TableWithStatistics check_statistic_with_table_scan(const TableWithStatistics& table_with_statistics,
                                                      const ColumnID column_id, const ScanType scan_type,
                                                      const AllParameterVariant value,
                                                      const optional<AllTypeVariant> value2 = nullopt) {
    auto table_wrapper = std::make_shared<TableWrapper>(table_with_statistics.table);
    table_wrapper->execute();
    auto table_scan = std::make_shared<TableScan>(table_wrapper, column_id, scan_type, value, value2);
    table_scan->execute();
    auto post_table_scan_statistics =
        table_with_statistics.statistics->predicate_statistics(column_id, scan_type, value, value2);
    TableWithStatistics output;
    output.table = table_scan->get_output();
    output.statistics = post_table_scan_statistics;

    auto predicted = round(output.statistics->row_count());
    auto actual = output.table->row_count();
    EXPECT_FLOAT_EQ(predicted, actual);

    return output;
  }

  /**
   * Predict output sizes of table scans with one value and compare with actual output sizes.
   * Does not work with ValuePlaceholder of stored procedures.
   */
  template <typename T>
  void check_column_with_values(const TableWithStatistics& table_with_statistics, const ColumnID column_id,
                                const ScanType scan_type, const std::vector<T>& values) {
    for (const auto& value : values) {
      check_statistic_with_table_scan(table_with_statistics, column_id, scan_type, AllParameterVariant(value));
    }
  }

  /**
   * Predict output sizes of table scans with two values (scan type = OpBetween) and compare with actual output sizes.
   * Does not work with ValuePlaceholder of stored procedures.
   */
  template <typename T>
  void check_column_with_values(const TableWithStatistics& table_with_statistics, const ColumnID column_id,
                                const ScanType scan_type, const std::vector<std::pair<T, T>>& values) {
    for (const auto& value_pair : values) {
      check_statistic_with_table_scan(table_with_statistics, column_id, scan_type,
                                      AllParameterVariant(value_pair.first), AllTypeVariant(value_pair.second));
    }
  }

  TableWithStatistics _table_a_with_statistics;

  //  {below min, min, max, above max}
  std::vector<int32_t> _int_values{0, 1, 6, 7};
  std::vector<float> _float_values{0.f, 1.f, 6.f, 7.f};
  std::vector<double> _double_values{0., 1., 6., 7.};
  std::vector<std::string> _string_values{"a", "b", "g", "h"};
};

TEST_F(TableStatisticsTest, GetTableTest) {
  EXPECT_FLOAT_EQ(round(_table_a_with_statistics.statistics->row_count()), _table_a_with_statistics.table->row_count());
}

TEST_F(TableStatisticsTest, NotEqualTest) {
  ScanType scan_type = ScanType::OpNotEquals;
  check_column_with_values(_table_a_with_statistics, ColumnID{0}, scan_type, _int_values);
  check_column_with_values(_table_a_with_statistics, ColumnID{1}, scan_type, _float_values);
  check_column_with_values(_table_a_with_statistics, ColumnID{2}, scan_type, _double_values);
  check_column_with_values(_table_a_with_statistics, ColumnID{3}, scan_type, _string_values);
}

TEST_F(TableStatisticsTest, EqualsTest) {
  ScanType scan_type = ScanType::OpEquals;
  check_column_with_values(_table_a_with_statistics, ColumnID{0}, scan_type, _int_values);
  check_column_with_values(_table_a_with_statistics, ColumnID{1}, scan_type, _float_values);
  check_column_with_values(_table_a_with_statistics, ColumnID{2}, scan_type, _double_values);
  check_column_with_values(_table_a_with_statistics, ColumnID{3}, scan_type, _string_values);
}

TEST_F(TableStatisticsTest, LessThanTest) {
  ScanType scan_type = ScanType::OpLessThan;
  check_column_with_values(_table_a_with_statistics, ColumnID{0}, scan_type, _int_values);
  //  table statistics assigns for floating point values greater and greater equals same selectivity
  std::vector<float> custom_float_values{0.f, 1.f, 5.1f, 7.f};
  check_column_with_values(_table_a_with_statistics, ColumnID{1}, scan_type, custom_float_values);
  std::vector<double> custom_double_values{0., 1., 5.1, 7.};
  check_column_with_values(_table_a_with_statistics, ColumnID{2}, scan_type, custom_double_values);
  //  table statistics for string columns not implemented for less table scans
  //  check_column_with_values(_table_a_with_statistics, "s", scan_type, _string_values);
}

TEST_F(TableStatisticsTest, LessEqualThanTest) {
  ScanType scan_type = ScanType::OpLessThanEquals;
  check_column_with_values(_table_a_with_statistics, ColumnID{0}, scan_type, _int_values);
  std::vector<float> custom_float_values{0.f, 1.9f, 5.f, 7.f};
  check_column_with_values(_table_a_with_statistics, ColumnID{1}, scan_type, custom_float_values);
  std::vector<double> custom_double_values{0., 1.9, 5., 7.};
  check_column_with_values(_table_a_with_statistics, ColumnID{2}, scan_type, custom_double_values);
  //  table statistics for string columns not implemented for less equal table scans
  //  check_column_with_values(_table_a_with_statistics, "s", scan_type, _string_values);
}

TEST_F(TableStatisticsTest, GreaterThanTest) {
  ScanType scan_type = ScanType::OpGreaterThan;
  check_column_with_values(_table_a_with_statistics, ColumnID{0}, scan_type, _int_values);
  //  table statistics assigns for floating point values greater and greater equals same selectivity
  std::vector<float> custom_float_values{0.f, 1.5f, 6.f, 7.f};
  check_column_with_values(_table_a_with_statistics, ColumnID{1}, scan_type, custom_float_values);
  std::vector<double> custom_double_values{0., 1.5, 6., 7.};
  check_column_with_values(_table_a_with_statistics, ColumnID{2}, scan_type, custom_double_values);
  //  table statistics for string columns not implemented for greater equal table scans
  //  check_column_with_values(_table_a_with_statistics, "s", scan_type, _string_values);
}

TEST_F(TableStatisticsTest, GreaterEqualThanTest) {
  ScanType scan_type = ScanType::OpGreaterThanEquals;
  check_column_with_values(_table_a_with_statistics, ColumnID{0}, scan_type, _int_values);
  std::vector<float> custom_float_values{0.f, 1.f, 5.1f, 7.f};
  check_column_with_values(_table_a_with_statistics, ColumnID{1}, scan_type, custom_float_values);
  std::vector<double> custom_double_values{0., 1., 5.1, 7.};
  check_column_with_values(_table_a_with_statistics, ColumnID{2}, scan_type, custom_double_values);
  //  table statistics for string columns not implemented for greater equal table scans
  //  check_column_with_values(_table_a_with_statistics, "s", scan_type, _string_values);
}

TEST_F(TableStatisticsTest, BetweenTest) {
  ScanType scan_type = ScanType::OpBetween;
  std::vector<std::pair<int32_t, int32_t>> int_values{{-1, 0}, {-1, 2}, {1, 2}, {0, 7}, {5, 6}, {5, 8}, {7, 8}};
  check_column_with_values(_table_a_with_statistics, ColumnID{0}, scan_type, int_values);
  std::vector<std::pair<float, float>> float_values{{-1.f, 0.f}, {-1.f, 1.9f}, {1.f, 1.9f}, {0.f, 7.f},
                                                    {5.1f, 6.f}, {5.1f, 8.f},  {7.f, 8.f}};
  check_column_with_values(_table_a_with_statistics, ColumnID{1}, scan_type, float_values);
  std::vector<std::pair<double, double>> double_values{{-1., 0.}, {-1., 1.9}, {1., 1.9}, {0., 7.},
                                                       {5.1, 6.}, {5.1, 8.},  {7., 8.}};
  check_column_with_values(_table_a_with_statistics, ColumnID{2}, scan_type, double_values);
  std::vector<std::pair<std::string, std::string>> string_values{{"a", "a"}, {"a", "c"}, {"a", "b"}, {"a", "h"},
                                                                 {"f", "g"}, {"f", "i"}, {"h", "i"}};
  //  table statistics for string columns not implemented for between table scans
  //  check_column_with_values(_table_a_with_statistics, "s", scan_type, _string_values);
}

TEST_F(TableStatisticsTest, MultipleColumnTableScans) {
  auto container = check_statistic_with_table_scan(_table_a_with_statistics, ColumnID{2}, ScanType::OpBetween,
                                                   AllParameterVariant(2.), AllTypeVariant(5.));
  container =
      check_statistic_with_table_scan(container, ColumnID{0}, ScanType::OpGreaterThanEquals, AllParameterVariant(4));
}

TEST_F(TableStatisticsTest, NotOverlappingTableScans) {
  /**
   * check that min and max values of columns are set
   */
  auto container = check_statistic_with_table_scan(_table_a_with_statistics, ColumnID{3}, ScanType::OpEquals,
                                                   AllParameterVariant("f"));
  check_statistic_with_table_scan(container, ColumnID{3}, ScanType::OpNotEquals, AllParameterVariant("f"));

  container = check_statistic_with_table_scan(_table_a_with_statistics, ColumnID{1}, ScanType::OpLessThanEquals,
                                              AllParameterVariant(3.5f));
  check_statistic_with_table_scan(container, ColumnID{1}, ScanType::OpGreaterThan, AllParameterVariant(3.5f));

  container = check_statistic_with_table_scan(_table_a_with_statistics, ColumnID{0}, ScanType::OpLessThan,
                                              AllParameterVariant(4));
  container = check_statistic_with_table_scan(container, ColumnID{0}, ScanType::OpGreaterThan, AllParameterVariant(2));
  check_statistic_with_table_scan(container, ColumnID{0}, ScanType::OpEquals, AllParameterVariant(3));
}

}  // namespace opossum
