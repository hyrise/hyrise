#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "all_parameter_variant.hpp"
#include "base_test.hpp"
#include "gtest/gtest.h"
#include "operators/table_scan.hpp"
#include "operators/table_wrapper.hpp"
#include "statistics/base_column_statistics.hpp"
#include "statistics/generate_table_statistics.hpp"
#include "statistics/table_statistics.hpp"

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
    auto table = load_table("src/test/tables/int_float_double_string.tbl", Chunk::MAX_SIZE);
    _table_a_with_statistics.statistics = std::make_shared<TableStatistics>(generate_table_statistics(*table));
    table->set_table_statistics(_table_a_with_statistics.statistics);
    _table_a_with_statistics.table = table;
  }

  /**
   * Predict output size of one TableScan with statistics and compare with actual output size of an actual TableScan.
   */
  TableWithStatistics check_statistic_with_table_scan(const TableWithStatistics& table_with_statistics,
                                                      const ColumnID column_id,
                                                      const PredicateCondition predicate_condition,
                                                      const AllParameterVariant value,
                                                      const std::optional<AllTypeVariant> value2 = std::nullopt) {
    auto table_wrapper = std::make_shared<TableWrapper>(table_with_statistics.table);
    table_wrapper->execute();

    std::shared_ptr<TableScan> table_scan;
    if (predicate_condition == PredicateCondition::Between) {
      auto first_table_scan = std::make_shared<TableScan>(
          table_wrapper, OperatorScanPredicate{column_id, PredicateCondition::GreaterThanEquals, value});
      first_table_scan->execute();

      table_scan = std::make_shared<TableScan>(
          first_table_scan, OperatorScanPredicate{column_id, PredicateCondition::LessThanEquals, *value2});
    } else {
      table_scan =
          std::make_shared<TableScan>(table_wrapper, OperatorScanPredicate{column_id, predicate_condition, value});
    }
    table_scan->execute();

    auto post_table_scan_statistics = std::make_shared<TableStatistics>(
        table_with_statistics.statistics->estimate_predicate(column_id, predicate_condition, value, value2));
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
                                const PredicateCondition predicate_condition, const std::vector<T>& values) {
    for (const auto& value : values) {
      check_statistic_with_table_scan(table_with_statistics, column_id, predicate_condition,
                                      AllParameterVariant(value));
    }
  }

  /**
   * Predict output sizes of table scans with two values (predicate condition = OpBetween) and compare with actual output sizes.
   * Does not work with ValuePlaceholder of stored procedures.
   */
  template <typename T>
  void check_column_with_values(const TableWithStatistics& table_with_statistics, const ColumnID column_id,
                                const PredicateCondition predicate_condition,
                                const std::vector<std::pair<T, T>>& values) {
    for (const auto& value_pair : values) {
      check_statistic_with_table_scan(table_with_statistics, column_id, predicate_condition,
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
  PredicateCondition predicate_condition = PredicateCondition::NotEquals;
  check_column_with_values(_table_a_with_statistics, ColumnID{0}, predicate_condition, _int_values);
  check_column_with_values(_table_a_with_statistics, ColumnID{1}, predicate_condition, _float_values);
  check_column_with_values(_table_a_with_statistics, ColumnID{2}, predicate_condition, _double_values);
  check_column_with_values(_table_a_with_statistics, ColumnID{3}, predicate_condition, _string_values);
}

TEST_F(TableStatisticsTest, EqualsTest) {
  PredicateCondition predicate_condition = PredicateCondition::Equals;
  check_column_with_values(_table_a_with_statistics, ColumnID{0}, predicate_condition, _int_values);
  check_column_with_values(_table_a_with_statistics, ColumnID{1}, predicate_condition, _float_values);
  check_column_with_values(_table_a_with_statistics, ColumnID{2}, predicate_condition, _double_values);
  check_column_with_values(_table_a_with_statistics, ColumnID{3}, predicate_condition, _string_values);
}

TEST_F(TableStatisticsTest, LessThanTest) {
  PredicateCondition predicate_condition = PredicateCondition::LessThan;
  check_column_with_values(_table_a_with_statistics, ColumnID{0}, predicate_condition, _int_values);
  //  table statistics assigns for floating point values greater and greater equals same selectivity
  std::vector<float> custom_float_values{0.f, 1.f, 5.1f, 7.f};
  check_column_with_values(_table_a_with_statistics, ColumnID{1}, predicate_condition, custom_float_values);
  std::vector<double> custom_double_values{0., 1., 5.1, 7.};
  check_column_with_values(_table_a_with_statistics, ColumnID{2}, predicate_condition, custom_double_values);
  //  table statistics for string columns not implemented for less table scans
  //  check_column_with_values(_table_a_with_statistics, "s", predicate_condition, _string_values);
}

TEST_F(TableStatisticsTest, LessEqualThanTest) {
  PredicateCondition predicate_condition = PredicateCondition::LessThanEquals;
  check_column_with_values(_table_a_with_statistics, ColumnID{0}, predicate_condition, _int_values);
  std::vector<float> custom_float_values{0.f, 1.9f, 5.f, 7.f};
  check_column_with_values(_table_a_with_statistics, ColumnID{1}, predicate_condition, custom_float_values);
  std::vector<double> custom_double_values{0., 1.9, 5., 7.};
  check_column_with_values(_table_a_with_statistics, ColumnID{2}, predicate_condition, custom_double_values);
  //  table statistics for string columns not implemented for less equal table scans
  //  check_column_with_values(_table_a_with_statistics, "s", predicate_condition, _string_values);
}

TEST_F(TableStatisticsTest, GreaterThanTest) {
  PredicateCondition predicate_condition = PredicateCondition::GreaterThan;
  check_column_with_values(_table_a_with_statistics, ColumnID{0}, predicate_condition, _int_values);
  //  table statistics assigns for floating point values greater and greater equals same selectivity
  std::vector<float> custom_float_values{0.f, 1.5f, 6.f, 7.f};
  check_column_with_values(_table_a_with_statistics, ColumnID{1}, predicate_condition, custom_float_values);
  std::vector<double> custom_double_values{0., 1.5, 6., 7.};
  check_column_with_values(_table_a_with_statistics, ColumnID{2}, predicate_condition, custom_double_values);
  //  table statistics for string columns not implemented for greater equal table scans
  //  check_column_with_values(_table_a_with_statistics, "s", predicate_condition, _string_values);
}

TEST_F(TableStatisticsTest, GreaterEqualThanTest) {
  PredicateCondition predicate_condition = PredicateCondition::GreaterThanEquals;
  check_column_with_values(_table_a_with_statistics, ColumnID{0}, predicate_condition, _int_values);
  std::vector<float> custom_float_values{0.f, 1.f, 5.1f, 7.f};
  check_column_with_values(_table_a_with_statistics, ColumnID{1}, predicate_condition, custom_float_values);
  std::vector<double> custom_double_values{0., 1., 5.1, 7.};
  check_column_with_values(_table_a_with_statistics, ColumnID{2}, predicate_condition, custom_double_values);
  //  table statistics for string columns not implemented for greater equal table scans
  //  check_column_with_values(_table_a_with_statistics, "s", predicate_condition, _string_values);
}

TEST_F(TableStatisticsTest, BetweenTest) {
  PredicateCondition predicate_condition = PredicateCondition::Between;
  std::vector<std::pair<int32_t, int32_t>> int_values{{-1, 0}, {-1, 2}, {1, 2}, {0, 7}, {5, 6}, {5, 8}, {7, 8}};
  check_column_with_values(_table_a_with_statistics, ColumnID{0}, predicate_condition, int_values);
  std::vector<std::pair<float, float>> float_values{{-1.f, 0.f}, {-1.f, 1.9f}, {1.f, 1.9f}, {0.f, 7.f},
                                                    {5.1f, 6.f}, {5.1f, 8.f},  {7.f, 8.f}};
  check_column_with_values(_table_a_with_statistics, ColumnID{1}, predicate_condition, float_values);
  std::vector<std::pair<double, double>> double_values{{-1., 0.}, {-1., 1.9}, {1., 1.9}, {0., 7.},
                                                       {5.1, 6.}, {5.1, 8.},  {7., 8.}};
  check_column_with_values(_table_a_with_statistics, ColumnID{2}, predicate_condition, double_values);
  std::vector<std::pair<std::string, std::string>> string_values{{"a", "a"}, {"a", "c"}, {"a", "b"}, {"a", "h"},
                                                                 {"f", "g"}, {"f", "i"}, {"h", "i"}};
  //  table statistics for string columns not implemented for between table scans
  //  check_column_with_values(_table_a_with_statistics, "s", predicate_condition, _string_values);
}

TEST_F(TableStatisticsTest, MultipleColumnTableScans) {
  auto container = check_statistic_with_table_scan(_table_a_with_statistics, ColumnID{2}, PredicateCondition::Between,
                                                   AllParameterVariant(2.), AllTypeVariant(5.));
  container = check_statistic_with_table_scan(container, ColumnID{0}, PredicateCondition::GreaterThanEquals,
                                              AllParameterVariant(4), AllTypeVariant(5));
}

TEST_F(TableStatisticsTest, NotOverlappingTableScans) {
  /**
   * check that min and max values of columns are set
   */
  auto container = check_statistic_with_table_scan(_table_a_with_statistics, ColumnID{3}, PredicateCondition::Equals,
                                                   AllParameterVariant("f"));
  check_statistic_with_table_scan(container, ColumnID{3}, PredicateCondition::NotEquals, AllParameterVariant("f"));

  container = check_statistic_with_table_scan(_table_a_with_statistics, ColumnID{1}, PredicateCondition::LessThanEquals,
                                              AllParameterVariant(3.5f));
  check_statistic_with_table_scan(container, ColumnID{1}, PredicateCondition::GreaterThan, AllParameterVariant(3.5f));

  container = check_statistic_with_table_scan(_table_a_with_statistics, ColumnID{0}, PredicateCondition::LessThan,
                                              AllParameterVariant(4));
  container =
      check_statistic_with_table_scan(container, ColumnID{0}, PredicateCondition::GreaterThan, AllParameterVariant(2));
  check_statistic_with_table_scan(container, ColumnID{0}, PredicateCondition::Equals, AllParameterVariant(3));
}

TEST_F(TableStatisticsTest, DirectlyAccessColumnStatistics) {
  /**
     * check that column statistics are generated even without a predicate
     */
  auto column_statistics = _table_a_with_statistics.statistics->column_statistics();
  EXPECT_EQ(column_statistics.size(), 4u);
  for (auto column_id = ColumnID{0}; column_id < column_statistics.size(); ++column_id) {
    EXPECT_TRUE(column_statistics.at(column_id));
    EXPECT_FLOAT_EQ(column_statistics.at(column_id)->distinct_count(), 6.f);
  }
}

TEST_F(TableStatisticsTest, TableType) {
  const auto table_a_statistics = _table_a_with_statistics.statistics;

  EXPECT_EQ(table_a_statistics->table_type(), TableType::Data);

  const auto post_predicate_statistics = std::make_shared<TableStatistics>(
      table_a_statistics->estimate_predicate(ColumnID{0}, PredicateCondition::Equals, 1));
  const auto post_join_statistics = std::make_shared<TableStatistics>(table_a_statistics->estimate_predicated_join(
      *table_a_statistics, JoinMode::Inner, {ColumnID{0}, ColumnID{0}}, PredicateCondition::Equals));

  EXPECT_EQ(post_predicate_statistics->table_type(), TableType::References);
  EXPECT_EQ(post_join_statistics->table_type(), TableType::References);
}

TEST_F(TableStatisticsTest, TwoColumnScan) {
  const auto int_int_float = load_table("src/test/tables/int_int_float.tbl");
  const auto int_int_float_statistics = generate_table_statistics(*int_int_float);

  // Make sure that the distinct counts are as expected, mainly that they are different. We compare them later.
  EXPECT_EQ(int_int_float_statistics.column_statistics()[0]->distinct_count(), 3.f);
  EXPECT_EQ(int_int_float_statistics.column_statistics()[1]->distinct_count(), 1.f);

  const auto post_predicate_statistics = std::make_shared<TableStatistics>(
      int_int_float_statistics.estimate_predicate(ColumnID{0}, PredicateCondition::NotEquals, ColumnID{1}));

  // Make sure that row count decreased according to NotEquals logic.
  EXPECT_FLOAT_EQ(post_predicate_statistics->row_count(), int_int_float_statistics.row_count() * 2.f / 3.f);

  // Make sure that the column statistics for both columns have been updated.
  EXPECT_NE(int_int_float_statistics.column_statistics()[0], post_predicate_statistics->column_statistics()[0]);
  EXPECT_NE(int_int_float_statistics.column_statistics()[1], post_predicate_statistics->column_statistics()[1]);

  // Sanity check that the third column's statistics have not been updated, since the predicate does not concern them.
  EXPECT_EQ(int_int_float_statistics.column_statistics()[2], post_predicate_statistics->column_statistics()[2]);

  // Make sure that the distinct counts are still the same, because we don't know better for NotEquals predicates.
  // This test is intended to make sure that the correct statistics for the column are at the correct index.
  EXPECT_EQ(int_int_float_statistics.column_statistics()[0]->distinct_count(),
            post_predicate_statistics->column_statistics()[0]->distinct_count());
  EXPECT_EQ(int_int_float_statistics.column_statistics()[1]->distinct_count(),
            post_predicate_statistics->column_statistics()[1]->distinct_count());
}

}  // namespace opossum
