#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "all_parameter_variant.hpp"
#include "common.hpp"
#include "operators/join_nested_loop_a.hpp"
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

    auto table_uniform_distribution = load_table("src/test/tables/int_equal_distribution.tbl", 0);
    _table_uniform_distribution_with_stats.statistics = std::make_shared<TableStatistics>(table_uniform_distribution);
    table_uniform_distribution->set_table_statistics(_table_uniform_distribution_with_stats.statistics);
    _table_uniform_distribution_with_stats.table = table_uniform_distribution;
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

  /**
   * For a table with statistics, all possible column join combinations are tested and result row count compared to
   * predicted row count.
   */
  void predict_join_row_counts_and_compare(const TableWithStatistics& table_with_statistics, const JoinMode mode,
                                           const ScanType scan_type) {
    auto table_wrapper = std::make_shared<TableWrapper>(table_with_statistics.table);
    table_wrapper->execute();
    for (ColumnID::base_type column_1 = 0; column_1 < table_with_statistics.table->col_count(); ++column_1) {
      for (ColumnID::base_type column_2 = 0; column_2 < table_with_statistics.table->col_count(); ++column_2) {
        auto column_ids = std::make_pair(ColumnID{column_1}, ColumnID{column_2});
        auto join_stats = table_with_statistics.statistics->join_statistics(table_with_statistics.statistics, mode,
                                                                            column_ids, scan_type);
        auto join = std::make_shared<JoinNestedLoopA>(table_wrapper, table_wrapper, mode, column_ids, scan_type);
        join->execute();
        auto result_row_count = join->get_output()->row_count();
        auto stats_row_count = join_stats->row_count();
        if (std::abs(result_row_count - stats_row_count) > 0.1f) {
          EXPECT_NEAR(stats_row_count, result_row_count, 0.01f);
        }
        EXPECT_NEAR(stats_row_count, result_row_count, 0.01f);
      }
    }
  }

  TableWithStatistics _table_a_with_statistics;
  TableWithStatistics _table_uniform_distribution_with_stats;

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

TEST_F(TableStatisticsTest, InnerJoinRealDataTest) {
  // test selectivity calculations for join_modes which do not produce null values in the result, scan types and column
  // combinations of int_equal_distribution.tbl
  std::vector<JoinMode> join_modes{JoinMode::Inner, JoinMode::Self};
  std::vector<ScanType> scan_types{ScanType::OpEquals,         ScanType::OpNotEquals,   ScanType::OpLessThan,
                                   ScanType::OpLessThanEquals, ScanType::OpGreaterThan, ScanType::OpGreaterThanEquals};

  for (const auto join_mode : join_modes) {
    for (const auto scan_type : scan_types) {
      predict_join_row_counts_and_compare(_table_uniform_distribution_with_stats, join_mode, scan_type);
    }
  }
}

TEST_F(TableStatisticsTest, CrossJoinRealDataTest) {
  auto table_row_count = _table_uniform_distribution_with_stats.table->row_count();
  auto table_stats = _table_uniform_distribution_with_stats.statistics;
  auto join_stats = table_stats->join_statistics(table_stats, JoinMode::Cross);
  EXPECT_FLOAT_EQ(join_stats->row_count(), table_row_count * table_row_count);
}

TEST_F(TableStatisticsTest, OuterJoinsRealDataTest) {
  // test selectivity calculations for all join_modes which can produce null values in the result, scan types and column
  // combinations of int_equal_distribution.tbl currently, the statistics component generates for a two column predicate
  // with ScanType::OpLessThan and canType::OpGreaterThan a column statistics with a too high distinct count E.g. two
  // columns which have the same min and max values of 1 and 3.
  //
  std::vector<JoinMode> join_modes{JoinMode::Right, JoinMode::Outer, JoinMode::Left};
  std::vector<ScanType> scan_types{ScanType::OpEquals, ScanType::OpNotEquals,  // ScanType::OpLessThan,
                                   ScanType::OpLessThanEquals,
                                   ScanType::OpGreaterThanEquals};  // ScanType::OpGreaterThan,

  for (const auto join_mode : join_modes) {
    for (const auto scan_type : scan_types) {
      predict_join_row_counts_and_compare(_table_uniform_distribution_with_stats, join_mode, scan_type);
    }
  }
}

}  // namespace opossum
