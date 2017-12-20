#include <memory>
#include <utility>
#include <vector>

#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "operators/join_nested_loop.hpp"
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
    auto table_uniform_distribution = load_table("src/test/tables/int_equal_distribution.tbl", Chunk::MAX_SIZE);
    _table_uniform_distribution_with_stats.statistics = std::make_shared<TableStatistics>(table_uniform_distribution);
    table_uniform_distribution->set_table_statistics(_table_uniform_distribution_with_stats.statistics);
    _table_uniform_distribution_with_stats.table = table_uniform_distribution;
  }

  /**
   * For a table with statistics, all possible column join combinations are tested and actual result row count is
   * compared to predicted row count.
   */
  void predict_join_row_counts_and_compare(const TableWithStatistics& table_with_statistics, const JoinMode mode,
                                           const ScanType scan_type) {
    auto table_wrapper = std::make_shared<TableWrapper>(table_with_statistics.table);
    table_wrapper->execute();
    for (ColumnID::base_type column_1 = 0; column_1 < table_with_statistics.table->column_count(); ++column_1) {
      for (ColumnID::base_type column_2 = 0; column_2 < table_with_statistics.table->column_count(); ++column_2) {
        auto column_ids = std::make_pair(ColumnID{column_1}, ColumnID{column_2});
        auto join_stats = table_with_statistics.statistics->generate_predicated_join_statistics(
            table_with_statistics.statistics, mode, column_ids, scan_type);
        auto join = std::make_shared<JoinNestedLoop>(table_wrapper, table_wrapper, mode, column_ids, scan_type);
        join->execute();
        auto result = join->get_output();
        EXPECT_FLOAT_EQ(result->row_count(), join_stats->row_count());
      }
    }
  }

  /**
   * For a table with statistics, all possible column join combinations are tested and cached result row count is
   * compared to predicted row count.
   */
  void predict_join_row_counts_and_compare(const TableWithStatistics& table_with_statistics, const JoinMode mode,
                                           const ScanType scan_type, const std::vector<uint32_t> row_counts) {
    for (ColumnID::base_type column_1 = 0; column_1 < table_with_statistics.table->column_count(); ++column_1) {
      for (ColumnID::base_type column_2 = 0; column_2 < table_with_statistics.table->column_count(); ++column_2) {
        auto column_ids = std::make_pair(ColumnID{column_1}, ColumnID{column_2});
        auto join_stats = table_with_statistics.statistics->generate_predicated_join_statistics(
            table_with_statistics.statistics, mode, column_ids, scan_type);
        auto cached_row_count = row_counts.at(table_with_statistics.table->column_count() * column_1 + column_2);
        EXPECT_FLOAT_EQ(cached_row_count, join_stats->row_count());
      }
    }
  }

  TableWithStatistics _table_uniform_distribution_with_stats;
};

TEST_F(TableStatisticsJoinTest, InnerJoinTest) {
  // test selectivity calculations for join_modes which do not produce null values in the result, scan types and column
  // combinations of int_equal_distribution.tbl
  std::vector<JoinMode> join_modes{JoinMode::Inner, JoinMode::Self};
  std::vector<ScanType> scan_types{ScanType::Equals,         ScanType::NotEquals,   ScanType::LessThan,
                                   ScanType::LessThanEquals, ScanType::GreaterThan, ScanType::GreaterThanEquals};

  // 3 dimensional table of cached row count results
  // [ join_modes index ][ scan_types index ][ column combination index = 4 * col1_index + col2_index ]
  const std::vector<std::vector<std::vector<uint32_t>>> row_counts{
      {
          {5400, 5400, 5400, 5400, 5400, 10800, 10800, 4320, 5400, 10800, 16200, 6480, 5400, 4320, 6480, 6480},
          {27000, 27000, 27000, 27000, 27000, 21600, 21600, 28080, 27000, 21600, 16200, 25920, 27000, 28080, 25920,
           25920},
          {13500, 5400, 8100, 16200, 21600, 10800, 16200, 25920, 18900, 5400, 8100, 22680, 10800, 2160, 3240, 12960},
          {18900, 10800, 13500, 21600, 27000, 21600, 27000, 30240, 24300, 16200, 24300, 29160, 16200, 6480, 9720,
           19440},
          {13500, 21600, 18900, 10800, 5400, 10800, 5400, 2160, 8100, 16200, 8100, 3240, 16200, 25920, 22680, 12960},
          {18900, 27000, 24300, 16200, 10800, 21600, 16200, 6480, 13500, 27000, 24300, 9720, 21600, 30240, 29160,
           19440},
      },
      {
          {5400, 5400, 5400, 5400, 5400, 10800, 10800, 4320, 5400, 10800, 16200, 6480, 5400, 4320, 6480, 6480},
          {27000, 27000, 27000, 27000, 27000, 21600, 21600, 28080, 27000, 21600, 16200, 25920, 27000, 28080, 25920,
           25920},
          {13500, 5400, 8100, 16200, 21600, 10800, 16200, 25920, 18900, 5400, 8100, 22680, 10800, 2160, 3240, 12960},
          {18900, 10800, 13500, 21600, 27000, 21600, 27000, 30240, 24300, 16200, 24300, 29160, 16200, 6480, 9720,
           19440},
          {13500, 21600, 18900, 10800, 5400, 10800, 5400, 2160, 8100, 16200, 8100, 3240, 16200, 25920, 22680, 12960},
          {18900, 27000, 24300, 16200, 10800, 21600, 16200, 6480, 13500, 27000, 24300, 9720, 21600, 30240, 29160,
           19440},
      }};

  for (auto join_modes_index = 0u; join_modes_index < join_modes.size(); ++join_modes_index) {
    for (auto scan_types_index = 0u; scan_types_index < scan_types.size(); ++scan_types_index) {
      predict_join_row_counts_and_compare(_table_uniform_distribution_with_stats, join_modes[join_modes_index],
                                          scan_types[scan_types_index], row_counts[join_modes_index][scan_types_index]);
    }
  }
}

// This is what InnerJoinTest would look like without cached join result size:
// TEST_F(TableStatisticsJoinTest, InnerJoinRealDataTest) {
//   // test selectivity calculations for join_modes which do not produce null values in the result, scan types and
//   // column combinations of int_equal_distribution.tbl
//   std::vector<JoinMode> join_modes{JoinMode::Inner, JoinMode::Self};
//   std::vector<ScanType> scan_types{ScanType::Equals,         ScanType::NotEquals,   ScanType::LessThan,
//                                    ScanType::LessThanEquals, ScanType::GreaterThan,
//                                    ScanType::GreaterThanEquals};

//   for (const auto join_mode : join_modes) {
//     for (const auto scan_type : scan_types) {
//       predict_join_row_counts_and_compare(_table_uniform_distribution_with_stats, join_mode, scan_type);
//     }
//   }
// }

TEST_F(TableStatisticsJoinTest, CrossJoinTest) {
  auto table_row_count = _table_uniform_distribution_with_stats.table->row_count();
  auto table_stats = _table_uniform_distribution_with_stats.statistics;
  auto join_stats = table_stats->generate_cross_join_statistics(table_stats);
  EXPECT_FLOAT_EQ(join_stats->row_count(), table_row_count * table_row_count);
}

TEST_F(TableStatisticsJoinTest, OuterJoinsTest) {
  // Test selectivity calculations for all join_modes which can produce null values in the result, scan types and
  // column combinations of int_equal_distribution.tbl

  // Currently, the statistics component produces in some cases for a two column predicate with ScanType::LessThan
  // and ScanType::GreaterThan a column statistics with a too high distinct count. (See comment column_statistics.hpp
  // for details). Null value calculations depend on the calculated distinct counts of the columns. Therefore, tests
  // for the mentioned scan types with null values are skipped.

  std::vector<JoinMode> join_modes{JoinMode::Right, JoinMode::Outer, JoinMode::Left};
  std::vector<ScanType> scan_types{ScanType::Equals, ScanType::NotEquals, ScanType::LessThanEquals,
                                   ScanType::GreaterThanEquals};  // ScanType::LessThan, ScanType::GreaterThan,

  // 3 dimensional table of cached row count results
  // [ join_modes index ][ scan_types index ][ column combination index = 4 * col1_index + col2_index ]
  const std::vector<std::vector<std::vector<uint32_t>>> row_counts{
      {
          {5400, 5400, 5400, 5400, 5490, 10800, 10800, 4428, 5520, 10860, 16200, 6588, 5430, 4380, 6480, 6480},
          {27000, 27000, 27000, 27000, 27000, 21600, 21600, 28080, 27000, 21600, 16200, 25920, 27000, 28080, 25920,
           25920},
          {18900, 10800, 13500, 21600, 27000, 21600, 27000, 30240, 24330, 16260, 24300, 29160, 16230, 6540, 9720,
           19440},
          {18900, 27000, 24300, 16200, 10890, 21600, 16200, 6588, 13590, 27000, 24300, 9828, 21600, 30240, 29160,
           19440},
      },
      {
          {5400, 5490, 5520, 5430, 5490, 10800, 10860, 4488, 5520, 10860, 16200, 6588, 5430, 4488, 6588, 6480},
          {27000, 27000, 27000, 27000, 27000, 21600, 21600, 28080, 27000, 21600, 16200, 25920, 27000, 28080, 25920,
           25920},
          {18900, 10890, 13590, 21600, 27000, 21600, 27000, 30240, 24330, 16260, 24300, 29160, 16230, 6648, 9828,
           19440},
          {18900, 27000, 24330, 16230, 10890, 21600, 16260, 6648, 13590, 27000, 24300, 9828, 21600, 30240, 29160,
           19440},
      },
      {
          {5400, 5490, 5520, 5430, 5400, 10800, 10860, 4380, 5400, 10800, 16200, 6480, 5400, 4428, 6588, 6480},
          {27000, 27000, 27000, 27000, 27000, 21600, 21600, 28080, 27000, 21600, 16200, 25920, 27000, 28080, 25920,
           25920},
          {18900, 10890, 13590, 21600, 27000, 21600, 27000, 30240, 24300, 16200, 24300, 29160, 16200, 6588, 9828,
           19440},
          {18900, 27000, 24330, 16230, 10800, 21600, 16260, 6540, 13500, 27000, 24300, 9720, 21600, 30240, 29160,
           19440},
      }};

  for (auto join_modes_index = 0u; join_modes_index < join_modes.size(); ++join_modes_index) {
    for (auto scan_types_index = 0u; scan_types_index < scan_types.size(); ++scan_types_index) {
      predict_join_row_counts_and_compare(_table_uniform_distribution_with_stats, join_modes[join_modes_index],
                                          scan_types[scan_types_index], row_counts[join_modes_index][scan_types_index]);
    }
  }
}

// This is what OuterJoinsTest would look like without cached join result size:
// TEST_F(TableStatisticsJoinTest, OuterJoinsRealDataTest) {
//   // Test selectivity calculations for all join_modes which can produce null values in the result, scan types and
//   // column combinations of int_equal_distribution.tbl

//   // Currently, the statistics component produces in some cases for a two column predicate with ScanType::LessThan
//   // and ScanType::GreaterThan a column statistics with a too high distinct count. (See comment
//   // column_statistics.hpp for details). Null value calculations depend on the calculated distinct counts of the
//   // columns. Therefore, tests for the mentioned scan types with null values are skipped.

//   std::vector<JoinMode> join_modes{JoinMode::Right, JoinMode::Outer, JoinMode::Left};
//   std::vector<ScanType> scan_types{ScanType::Equals, ScanType::NotEquals, ScanType::LessThanEquals,
//                                    ScanType::GreaterThanEquals};

//   for (const auto join_mode : join_modes) {
//     for (const auto scan_type : scan_types) {
//       predict_join_row_counts_and_compare(_table_uniform_distribution_with_stats, join_mode, scan_type);
//     }
//   }
// }

}  // namespace opossum
