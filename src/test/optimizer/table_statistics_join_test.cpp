#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "common.hpp"
#include "operators/join_nested_loop_a.hpp"
#include "operators/table_wrapper.hpp"
#include "optimizer/table_statistics.hpp"

namespace opossum {

class TableStatisticsJoinTest : public BaseTest {
 protected:
  // Because of Operator::get_output() returns a const table, we need another way of adding statistics to it. Here
  // you go, have some nice boilerplate....
  struct TableWithStatistics {
    std::shared_ptr<const Table> table;
    std::shared_ptr<TableStatistics> statistics;
  };

  void SetUp() override {
    auto table_uniform_distribution = load_table("src/test/tables/int_equal_distribution.tbl", 0);
    _table_uniform_distribution_with_stats.statistics = std::make_shared<TableStatistics>(table_uniform_distribution);
    table_uniform_distribution->set_table_statistics(_table_uniform_distribution_with_stats.statistics);
    _table_uniform_distribution_with_stats.table = table_uniform_distribution;
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

  TableWithStatistics _table_uniform_distribution_with_stats;
};

TEST_F(TableStatisticsJoinTest, InnerJoinRealDataTest) {
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

TEST_F(TableStatisticsJoinTest, CrossJoinRealDataTest) {
  auto table_row_count = _table_uniform_distribution_with_stats.table->row_count();
  auto table_stats = _table_uniform_distribution_with_stats.statistics;
  auto join_stats = table_stats->join_statistics(table_stats, JoinMode::Cross);
  EXPECT_FLOAT_EQ(join_stats->row_count(), table_row_count * table_row_count);
}

TEST_F(TableStatisticsJoinTest, OuterJoinsRealDataTest) {
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
