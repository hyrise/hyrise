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
#include "storage/storage_manager.hpp"

namespace opossum {

class TableStatisticsTest : public BaseTest {
 protected:
  struct TableContainer {
    std::shared_ptr<TableStatistics> statistics;
    std::shared_ptr<const Table> table;
  };
  void SetUp() override {
    std::shared_ptr<Table> table_a = load_table("src/test/tables/int_float_double_string.tbl", 0);
    StorageManager::get().add_table("table_a", table_a);

    auto table_a_stats = opossum::StorageManager::get().get_table("table_a")->get_table_statistics();
    table_a_container = TableContainer{table_a_stats, table_a};
  }

  TableContainer table_a_container;
  TableContainer check_statistic_with_table_scan(const TableContainer& table_container, const std::string& column_name,
                                                 const ScanType scan_type, const AllParameterVariant value,
                                                 const optional<AllTypeVariant> value2 = nullopt) {
    auto statistics = table_container.statistics->predicate_statistics(column_name, scan_type, value, value2);
    auto table_wraper = std::make_shared<TableWrapper>(table_container.table);
    table_wraper->execute();
    auto table_scan = std::make_shared<TableScan>(table_wraper, column_name, scan_type, value, value2);
    table_scan->execute();
    TableContainer output_container{statistics, table_scan->get_output()};
    assert_equal_row_count(output_container);
    return output_container;
  }

  // ASSERT_EQ does not work in member function
  struct {
    void operator()(const TableContainer& table_container) {
      ASSERT_EQ(static_cast<int>(table_container.statistics->row_count()),
                static_cast<int>(table_container.table->row_count()));
    }
  } assert_equal_row_count;

  template <typename T>
  void check_column_with_values(const TableContainer& table_container, const std::string& column_name,
                                const ScanType scan_type, const std::vector<T>& values) {
    for (const auto& value : values) {
      check_statistic_with_table_scan(table_container, column_name, scan_type, opossum::AllParameterVariant(value));
    }
  }

  template <typename T>
  void check_column_with_values(const TableContainer& table_container, const std::string& column_name,
                                const ScanType scan_type, const std::vector<std::pair<T, T>>& values) {
    for (const auto& value_pair : values) {
      check_statistic_with_table_scan(table_container, column_name, scan_type,
                                      opossum::AllParameterVariant(value_pair.first),
                                      opossum::AllTypeVariant(value_pair.second));
    }
  }

  std::vector<int> int_values{0, 1, 6, 7};
  std::vector<float> float_values{0.f, 1.f, 6.f, 7.f};
  std::vector<double> double_values{0., 1., 6., 7.};
  std::vector<std::string> string_values{"a", "b", "g", "h"};
};

TEST_F(TableStatisticsTest, GetTableTest) { assert_equal_row_count(table_a_container); }

TEST_F(TableStatisticsTest, NotEqualTest) {
  ScanType scan_type = ScanType::OpNotEquals;
  check_column_with_values(table_a_container, "i", scan_type, int_values);
  check_column_with_values(table_a_container, "f", scan_type, float_values);
  check_column_with_values(table_a_container, "d", scan_type, double_values);
  check_column_with_values(table_a_container, "s", scan_type, string_values);
}

TEST_F(TableStatisticsTest, EqualTest) {
  ScanType scan_type = ScanType::OpEquals;
  check_column_with_values(table_a_container, "i", scan_type, int_values);
  check_column_with_values(table_a_container, "f", scan_type, float_values);
  check_column_with_values(table_a_container, "d", scan_type, double_values);
  check_column_with_values(table_a_container, "s", scan_type, string_values);
}

TEST_F(TableStatisticsTest, LessThanTest) {
  ScanType scan_type = ScanType::OpLessThan;
  check_column_with_values(table_a_container, "i", scan_type, int_values);
  std::vector<float> custom_float_values{0.f, 1.f, 5.5f, 7.f};
  check_column_with_values(table_a_container, "f", scan_type, custom_float_values);
  std::vector<double> custom_double_values{0., 1., 5.5, 7.};
  check_column_with_values(table_a_container, "d", scan_type, custom_double_values);
  //  check_column_with_values(table_a_container, "s", scan_type, string_values);
}

TEST_F(TableStatisticsTest, LessEqualThanTest) {
  ScanType scan_type = ScanType::OpLessThanEquals;
  check_column_with_values(table_a_container, "i", scan_type, int_values);
  check_column_with_values(table_a_container, "f", scan_type, float_values);
  check_column_with_values(table_a_container, "d", scan_type, double_values);
  //  check_column_with_values(table_a_container, "s", scan_type, string_values);
}

TEST_F(TableStatisticsTest, GreaterThanTest) {
  ScanType scan_type = ScanType::OpGreaterThan;
  check_column_with_values(table_a_container, "i", scan_type, int_values);
  std::vector<float> custom_float_values{0.f, 1.5f, 6.f, 7.f};
  check_column_with_values(table_a_container, "f", scan_type, custom_float_values);
  std::vector<double> custom_double_values{0., 1.5, 6., 7.};
  check_column_with_values(table_a_container, "d", scan_type, custom_double_values);
  //  check_column_with_values(table_a_container, "s", scan_type, string_values);
}

TEST_F(TableStatisticsTest, GreaterEqualThanTest) {
  ScanType scan_type = ScanType::OpGreaterThanEquals;
  check_column_with_values(table_a_container, "i", scan_type, int_values);
  check_column_with_values(table_a_container, "f", scan_type, float_values);
  check_column_with_values(table_a_container, "d", scan_type, double_values);
  //  check_column_with_values(table_a_container, "s", scan_type, string_values);
}

TEST_F(TableStatisticsTest, BetweenTest) {
  ScanType scan_type = ScanType::OpBetween;
  std::vector<std::pair<int, int>> int_values{{-1, 0}, {-1, 2}, {1, 2}, {0, 7}, {5, 6}, {5, 8}, {7, 8}};
  check_column_with_values(table_a_container, "i", scan_type, int_values);
  std::vector<std::pair<float, float>> float_values{{-1.f, 0.f}, {-1.f, 2.f}, {1.f, 2.f}, {0.f, 7.f},
                                                    {5.f, 6.f},  {5.f, 8.f},  {7.f, 8.f}};
  check_column_with_values(table_a_container, "f", scan_type, float_values);
  std::vector<std::pair<double, double>> double_values{{-1., 0.}, {-1., 2.}, {1., 2.}, {0., 7.},
                                                       {5., 6.},  {5., 8.},  {7., 8.}};
  check_column_with_values(table_a_container, "d", scan_type, double_values);
  std::vector<std::pair<std::string, std::string>> string_values{{"a", "a"}, {"a", "c"}, {"a", "b"}, {"a", "h"},
                                                                 {"f", "g"}, {"f", "i"}, {"h", "i"}};
  //  check_column_with_values(table_a_container, "s", scan_type, string_values);
}

TEST_F(TableStatisticsTest, MultipleColumns) {
  auto container = check_statistic_with_table_scan(table_a_container, "d", ScanType::OpBetween,
                                                   opossum::AllParameterVariant(2.), opossum::AllTypeVariant(5.));
  container =
      check_statistic_with_table_scan(container, "i", ScanType::OpGreaterThanEquals, opossum::AllParameterVariant(4));
}

TEST_F(TableStatisticsTest, NotOverlappingScans) {
  /**
   * check that min and max values of columns are set
   */
  auto container =
      check_statistic_with_table_scan(table_a_container, "s", ScanType::OpEquals, opossum::AllParameterVariant("f"));
  container = check_statistic_with_table_scan(container, "s", ScanType::OpNotEquals, opossum::AllParameterVariant("f"));

  container = check_statistic_with_table_scan(table_a_container, "f", ScanType::OpLessThanEquals,
                                              opossum::AllParameterVariant(4.));
  container =
      check_statistic_with_table_scan(container, "f", ScanType::OpGreaterThan, opossum::AllParameterVariant(4.));

  container =
      check_statistic_with_table_scan(table_a_container, "i", ScanType::OpLessThan, opossum::AllParameterVariant(4));
  container = check_statistic_with_table_scan(container, "i", ScanType::OpGreaterThan, opossum::AllParameterVariant(2));
  container = check_statistic_with_table_scan(container, "i", ScanType::OpEquals, opossum::AllParameterVariant(3));
}

}  // namespace opossum
