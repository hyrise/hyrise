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
#include "statistics/base_cxlumn_statistics.hpp"
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
                                                      const CxlumnID cxlumn_id,
                                                      const PredicateCondition predicate_condition,
                                                      const AllParameterVariant value,
                                                      const std::optional<AllTypeVariant> value2 = std::nullopt) {
    auto table_wrapper = std::make_shared<TableWrapper>(table_with_statistics.table);
    table_wrapper->execute();

    std::shared_ptr<TableScan> table_scan;
    if (predicate_condition == PredicateCondition::Between) {
      auto first_table_scan =
          std::make_shared<TableScan>(table_wrapper, cxlumn_id, PredicateCondition::GreaterThanEquals, value);
      first_table_scan->execute();

      table_scan =
          std::make_shared<TableScan>(first_table_scan, cxlumn_id, PredicateCondition::LessThanEquals, *value2);
    } else {
      table_scan = std::make_shared<TableScan>(table_wrapper, cxlumn_id, predicate_condition, value);
    }
    table_scan->execute();

    auto post_table_scan_statistics = std::make_shared<TableStatistics>(
        table_with_statistics.statistics->estimate_predicate(cxlumn_id, predicate_condition, value, value2));
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
  void check_cxlumn_with_values(const TableWithStatistics& table_with_statistics, const CxlumnID cxlumn_id,
                                const PredicateCondition predicate_condition, const std::vector<T>& values) {
    for (const auto& value : values) {
      check_statistic_with_table_scan(table_with_statistics, cxlumn_id, predicate_condition,
                                      AllParameterVariant(value));
    }
  }

  /**
   * Predict output sizes of table scans with two values (predicate condition = OpBetween) and compare with actual output sizes.
   * Does not work with ValuePlaceholder of stored procedures.
   */
  template <typename T>
  void check_cxlumn_with_values(const TableWithStatistics& table_with_statistics, const CxlumnID cxlumn_id,
                                const PredicateCondition predicate_condition,
                                const std::vector<std::pair<T, T>>& values) {
    for (const auto& value_pair : values) {
      check_statistic_with_table_scan(table_with_statistics, cxlumn_id, predicate_condition,
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
  check_cxlumn_with_values(_table_a_with_statistics, CxlumnID{0}, predicate_condition, _int_values);
  check_cxlumn_with_values(_table_a_with_statistics, CxlumnID{1}, predicate_condition, _float_values);
  check_cxlumn_with_values(_table_a_with_statistics, CxlumnID{2}, predicate_condition, _double_values);
  check_cxlumn_with_values(_table_a_with_statistics, CxlumnID{3}, predicate_condition, _string_values);
}

TEST_F(TableStatisticsTest, EqualsTest) {
  PredicateCondition predicate_condition = PredicateCondition::Equals;
  check_cxlumn_with_values(_table_a_with_statistics, CxlumnID{0}, predicate_condition, _int_values);
  check_cxlumn_with_values(_table_a_with_statistics, CxlumnID{1}, predicate_condition, _float_values);
  check_cxlumn_with_values(_table_a_with_statistics, CxlumnID{2}, predicate_condition, _double_values);
  check_cxlumn_with_values(_table_a_with_statistics, CxlumnID{3}, predicate_condition, _string_values);
}

TEST_F(TableStatisticsTest, LessThanTest) {
  PredicateCondition predicate_condition = PredicateCondition::LessThan;
  check_cxlumn_with_values(_table_a_with_statistics, CxlumnID{0}, predicate_condition, _int_values);
  //  table statistics assigns for floating point values greater and greater equals same selectivity
  std::vector<float> custom_float_values{0.f, 1.f, 5.1f, 7.f};
  check_cxlumn_with_values(_table_a_with_statistics, CxlumnID{1}, predicate_condition, custom_float_values);
  std::vector<double> custom_double_values{0., 1., 5.1, 7.};
  check_cxlumn_with_values(_table_a_with_statistics, CxlumnID{2}, predicate_condition, custom_double_values);
  //  table statistics for string cxlumns not implemented for less table scans
  //  check_cxlumn_with_values(_table_a_with_statistics, "s", predicate_condition, _string_values);
}

TEST_F(TableStatisticsTest, LessEqualThanTest) {
  PredicateCondition predicate_condition = PredicateCondition::LessThanEquals;
  check_cxlumn_with_values(_table_a_with_statistics, CxlumnID{0}, predicate_condition, _int_values);
  std::vector<float> custom_float_values{0.f, 1.9f, 5.f, 7.f};
  check_cxlumn_with_values(_table_a_with_statistics, CxlumnID{1}, predicate_condition, custom_float_values);
  std::vector<double> custom_double_values{0., 1.9, 5., 7.};
  check_cxlumn_with_values(_table_a_with_statistics, CxlumnID{2}, predicate_condition, custom_double_values);
  //  table statistics for string cxlumns not implemented for less equal table scans
  //  check_cxlumn_with_values(_table_a_with_statistics, "s", predicate_condition, _string_values);
}

TEST_F(TableStatisticsTest, GreaterThanTest) {
  PredicateCondition predicate_condition = PredicateCondition::GreaterThan;
  check_cxlumn_with_values(_table_a_with_statistics, CxlumnID{0}, predicate_condition, _int_values);
  //  table statistics assigns for floating point values greater and greater equals same selectivity
  std::vector<float> custom_float_values{0.f, 1.5f, 6.f, 7.f};
  check_cxlumn_with_values(_table_a_with_statistics, CxlumnID{1}, predicate_condition, custom_float_values);
  std::vector<double> custom_double_values{0., 1.5, 6., 7.};
  check_cxlumn_with_values(_table_a_with_statistics, CxlumnID{2}, predicate_condition, custom_double_values);
  //  table statistics for string cxlumns not implemented for greater equal table scans
  //  check_cxlumn_with_values(_table_a_with_statistics, "s", predicate_condition, _string_values);
}

TEST_F(TableStatisticsTest, GreaterEqualThanTest) {
  PredicateCondition predicate_condition = PredicateCondition::GreaterThanEquals;
  check_cxlumn_with_values(_table_a_with_statistics, CxlumnID{0}, predicate_condition, _int_values);
  std::vector<float> custom_float_values{0.f, 1.f, 5.1f, 7.f};
  check_cxlumn_with_values(_table_a_with_statistics, CxlumnID{1}, predicate_condition, custom_float_values);
  std::vector<double> custom_double_values{0., 1., 5.1, 7.};
  check_cxlumn_with_values(_table_a_with_statistics, CxlumnID{2}, predicate_condition, custom_double_values);
  //  table statistics for string cxlumns not implemented for greater equal table scans
  //  check_cxlumn_with_values(_table_a_with_statistics, "s", predicate_condition, _string_values);
}

TEST_F(TableStatisticsTest, BetweenTest) {
  PredicateCondition predicate_condition = PredicateCondition::Between;
  std::vector<std::pair<int32_t, int32_t>> int_values{{-1, 0}, {-1, 2}, {1, 2}, {0, 7}, {5, 6}, {5, 8}, {7, 8}};
  check_cxlumn_with_values(_table_a_with_statistics, CxlumnID{0}, predicate_condition, int_values);
  std::vector<std::pair<float, float>> float_values{{-1.f, 0.f}, {-1.f, 1.9f}, {1.f, 1.9f}, {0.f, 7.f},
                                                    {5.1f, 6.f}, {5.1f, 8.f},  {7.f, 8.f}};
  check_cxlumn_with_values(_table_a_with_statistics, CxlumnID{1}, predicate_condition, float_values);
  std::vector<std::pair<double, double>> double_values{{-1., 0.}, {-1., 1.9}, {1., 1.9}, {0., 7.},
                                                       {5.1, 6.}, {5.1, 8.},  {7., 8.}};
  check_cxlumn_with_values(_table_a_with_statistics, CxlumnID{2}, predicate_condition, double_values);
  std::vector<std::pair<std::string, std::string>> string_values{{"a", "a"}, {"a", "c"}, {"a", "b"}, {"a", "h"},
                                                                 {"f", "g"}, {"f", "i"}, {"h", "i"}};
  //  table statistics for string cxlumns not implemented for between table scans
  //  check_cxlumn_with_values(_table_a_with_statistics, "s", predicate_condition, _string_values);
}

TEST_F(TableStatisticsTest, MultipleCxlumnTableScans) {
  auto container = check_statistic_with_table_scan(_table_a_with_statistics, CxlumnID{2}, PredicateCondition::Between,
                                                   AllParameterVariant(2.), AllTypeVariant(5.));
  container = check_statistic_with_table_scan(container, CxlumnID{0}, PredicateCondition::GreaterThanEquals,
                                              AllParameterVariant(4), AllTypeVariant(5));
}

TEST_F(TableStatisticsTest, NotOverlappingTableScans) {
  /**
   * check that min and max values of cxlumns are set
   */
  auto container = check_statistic_with_table_scan(_table_a_with_statistics, CxlumnID{3}, PredicateCondition::Equals,
                                                   AllParameterVariant("f"));
  check_statistic_with_table_scan(container, CxlumnID{3}, PredicateCondition::NotEquals, AllParameterVariant("f"));

  container = check_statistic_with_table_scan(_table_a_with_statistics, CxlumnID{1}, PredicateCondition::LessThanEquals,
                                              AllParameterVariant(3.5f));
  check_statistic_with_table_scan(container, CxlumnID{1}, PredicateCondition::GreaterThan, AllParameterVariant(3.5f));

  container = check_statistic_with_table_scan(_table_a_with_statistics, CxlumnID{0}, PredicateCondition::LessThan,
                                              AllParameterVariant(4));
  container =
      check_statistic_with_table_scan(container, CxlumnID{0}, PredicateCondition::GreaterThan, AllParameterVariant(2));
  check_statistic_with_table_scan(container, CxlumnID{0}, PredicateCondition::Equals, AllParameterVariant(3));
}

TEST_F(TableStatisticsTest, DirectlyAccessCxlumnStatistics) {
  /**
     * check that cxlumn statistics are generated even without a predicate
     */
  auto cxlumn_statistics = _table_a_with_statistics.statistics->cxlumn_statistics();
  EXPECT_EQ(cxlumn_statistics.size(), 4u);
  for (auto cxlumn_id = CxlumnID{0}; cxlumn_id < cxlumn_statistics.size(); ++cxlumn_id) {
    EXPECT_TRUE(cxlumn_statistics.at(cxlumn_id));
    EXPECT_FLOAT_EQ(cxlumn_statistics.at(cxlumn_id)->distinct_count(), 6.f);
  }
}

TEST_F(TableStatisticsTest, TableType) {
  const auto table_a_statistics = _table_a_with_statistics.statistics;

  EXPECT_EQ(table_a_statistics->table_type(), TableType::Data);

  const auto post_predicate_statistics = std::make_shared<TableStatistics>(
      table_a_statistics->estimate_predicate(CxlumnID{0}, PredicateCondition::Equals, 1));
  const auto post_join_statistics = std::make_shared<TableStatistics>(table_a_statistics->estimate_predicated_join(
      *table_a_statistics, JoinMode::Inner, {CxlumnID{0}, CxlumnID{0}}, PredicateCondition::Equals));

  EXPECT_EQ(post_predicate_statistics->table_type(), TableType::References);
  EXPECT_EQ(post_join_statistics->table_type(), TableType::References);
}

}  // namespace opossum
