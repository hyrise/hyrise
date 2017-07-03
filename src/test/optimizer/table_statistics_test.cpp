#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "optimizer/table_statistics.hpp"
#include "storage/storage_manager.hpp"

namespace opossum {

class TableStatisticsTest : public BaseTest {
 protected:
  void SetUp() override {
    std::shared_ptr<Table> table_a = load_table("src/test/tables/int_float_double_string.tbl", 0);
    StorageManager::get().add_table("table_a", std::move(table_a));

    table_a_stats = std::make_shared<TableStatistics>("table_a");

    //          std::shared_ptr<Table> table_b = load_table("src/test/tables/int_float2.tbl", 0);
    //          StorageManager::get().add_table("table_b", std::move(table_b));
    //
    //          table_b_stats = std::make_shared<TableStatistics>("table_b");

    //          std::shared_ptr<Table> table_c = load_table("src/test/tables/int_string.tbl", 4);
    //          StorageManager::get().add_table("table_c", std::move(table_c));
    //
    //          std::shared_ptr<Table> table_d = load_table("src/test/tables/string_int.tbl", 3);
    //          StorageManager::get().add_table("table_d", std::move(table_d));
    //
    //          std::shared_ptr<Table> test_table2 = load_table("src/test/tables/int_string2.tbl", 2);
    //          StorageManager::get().add_table("TestTable", test_table2);
  }

  std::shared_ptr<TableStatistics> table_a_stats;
  std::shared_ptr<TableStatistics> table_b_stats;
};

TEST_F(TableStatisticsTest, GetTableTest) { ASSERT_EQ(table_a_stats->row_count(), 6.); }

TEST_F(TableStatisticsTest, NotEqualTest) {
  auto stat = table_a_stats->predicate_statistics("a", "!=", opossum::AllParameterVariant(1));
  ASSERT_EQ(stat->row_count(), 5.);

  stat = table_a_stats->predicate_statistics("a", "!=", opossum::AllParameterVariant(0));
  ASSERT_EQ(stat->row_count(), 6.);

  stat = table_a_stats->predicate_statistics("a", "!=", opossum::AllParameterVariant(7));
  ASSERT_EQ(stat->row_count(), 6.);

  stat = table_a_stats->predicate_statistics("b", "!=", opossum::AllParameterVariant(1.f));
  ASSERT_EQ(stat->row_count(), 5.);

  stat = table_a_stats->predicate_statistics("b", "!=", opossum::AllParameterVariant(0.f));
  ASSERT_EQ(stat->row_count(), 6.);

  stat = table_a_stats->predicate_statistics("b", "!=", opossum::AllParameterVariant(7.f));
  ASSERT_EQ(stat->row_count(), 6.);
}

TEST_F(TableStatisticsTest, EqualTest) {
  auto stat = table_a_stats->predicate_statistics("a", "=", opossum::AllParameterVariant(1));
  ASSERT_EQ(stat->row_count(), 1.);

  stat = table_a_stats->predicate_statistics("a", "=", opossum::AllParameterVariant(0));
  ASSERT_EQ(stat->row_count(), 0.);

  stat = table_a_stats->predicate_statistics("a", "=", opossum::AllParameterVariant(7));
  ASSERT_EQ(stat->row_count(), 0.);

  stat = table_a_stats->predicate_statistics("b", "=", opossum::AllParameterVariant(1.f));
  ASSERT_EQ(stat->row_count(), 1.);

  stat = table_a_stats->predicate_statistics("b", "=", opossum::AllParameterVariant(0.f));
  ASSERT_EQ(stat->row_count(), 0.);

  stat = table_a_stats->predicate_statistics("b", "=", opossum::AllParameterVariant(7.f));
  ASSERT_EQ(stat->row_count(), 0.);
}

TEST_F(TableStatisticsTest, LessThanTest) {
  auto stat = table_a_stats->predicate_statistics("a", "<", opossum::AllParameterVariant(2));
  ASSERT_EQ(stat->row_count(), 1.);

  stat = table_a_stats->predicate_statistics("a", "<", opossum::AllParameterVariant(1));
  ASSERT_EQ(stat->row_count(), 0.);

  stat = table_a_stats->predicate_statistics("a", "<", opossum::AllParameterVariant(6));
  ASSERT_EQ(stat->row_count(), 5.);

  stat = table_a_stats->predicate_statistics("b", "<", opossum::AllParameterVariant(2.f));
  ASSERT_EQ(stat->row_count(), 2.);

  stat = table_a_stats->predicate_statistics("b", "<", opossum::AllParameterVariant(1.f));
  ASSERT_EQ(stat->row_count(), 0.);

  stat = table_a_stats->predicate_statistics("b", "<", opossum::AllParameterVariant(6.f));
  ASSERT_EQ(stat->row_count(), 6.);
}

TEST_F(TableStatisticsTest, LessEqualThanTest) {
  auto stat = table_a_stats->predicate_statistics("a", "<=", opossum::AllParameterVariant(3));
  ASSERT_EQ(stat->row_count(), 3.);

  stat = table_a_stats->predicate_statistics("a", "<=", opossum::AllParameterVariant(0));
  ASSERT_EQ(stat->row_count(), 0.);

  stat = table_a_stats->predicate_statistics("a", "<=", opossum::AllParameterVariant(1));
  ASSERT_EQ(stat->row_count(), 1.);

  stat = table_a_stats->predicate_statistics("a", "<=", opossum::AllParameterVariant(6));
  ASSERT_EQ(stat->row_count(), 6.);

  stat = table_a_stats->predicate_statistics("b", "<=", opossum::AllParameterVariant(3.f));
  ASSERT_EQ(stat->row_count(), 3.);

  stat = table_a_stats->predicate_statistics("b", "<=", opossum::AllParameterVariant(0.f));
  ASSERT_EQ(stat->row_count(), 0.);

  stat = table_a_stats->predicate_statistics("b", "<=", opossum::AllParameterVariant(1.f));
  ASSERT_EQ(stat->row_count(), 1.);

  stat = table_a_stats->predicate_statistics("b", "<=", opossum::AllParameterVariant(6.f));
  ASSERT_EQ(stat->row_count(), 6.);
}

TEST_F(TableStatisticsTest, GreaterThanTest) {
  auto stat = table_a_stats->predicate_statistics("a", ">", opossum::AllParameterVariant(2));
  ASSERT_EQ(stat->row_count(), 4.);

  stat = table_a_stats->predicate_statistics("a", ">", opossum::AllParameterVariant(1));
  ASSERT_EQ(stat->row_count(), 5.);

  stat = table_a_stats->predicate_statistics("a", ">", opossum::AllParameterVariant(6));
  ASSERT_EQ(stat->row_count(), 0.);

  stat = table_a_stats->predicate_statistics("b", ">", opossum::AllParameterVariant(3.f));
  ASSERT_EQ(stat->row_count(), 4.);

  stat = table_a_stats->predicate_statistics("b", ">", opossum::AllParameterVariant(1.f));
  ASSERT_EQ(stat->row_count(), 6.);

  stat = table_a_stats->predicate_statistics("b", ">", opossum::AllParameterVariant(6.f));
  ASSERT_EQ(stat->row_count(), 0.);
}

TEST_F(TableStatisticsTest, GreaterEqualThanTest) {
  auto stat = table_a_stats->predicate_statistics("a", ">=", opossum::AllParameterVariant(3));
  ASSERT_EQ(stat->row_count(), 4.);

  stat = table_a_stats->predicate_statistics("a", ">=", opossum::AllParameterVariant(1));
  ASSERT_EQ(stat->row_count(), 6.);

  stat = table_a_stats->predicate_statistics("a", ">=", opossum::AllParameterVariant(6));
  ASSERT_EQ(stat->row_count(), 1.);

  stat = table_a_stats->predicate_statistics("a", ">=", opossum::AllParameterVariant(7));
  ASSERT_EQ(stat->row_count(), 0.);

  stat = table_a_stats->predicate_statistics("b", ">=", opossum::AllParameterVariant(3.f));
  ASSERT_EQ(stat->row_count(), 4.);

  stat = table_a_stats->predicate_statistics("b", ">=", opossum::AllParameterVariant(1.f));
  ASSERT_EQ(stat->row_count(), 6.);

  stat = table_a_stats->predicate_statistics("b", ">=", opossum::AllParameterVariant(6.f));
  ASSERT_EQ(stat->row_count(), 1.);

  stat = table_a_stats->predicate_statistics("b", ">=", opossum::AllParameterVariant(7.f));
  ASSERT_EQ(stat->row_count(), 0.);
}

//
//    TEST_F(TableStatisticsTest, NotEqualTest) {
//      auto stat1 = table_a_stats->predicate_statistics("a", "!=", opossum::AllParameterVariant(123));
//      ASSERT_EQ(stat1->row_count(), 2.);
//
//      auto stat2 = table_a_stats->predicate_statistics("a", "!=", opossum::AllParameterVariant(458.2f));
//      ASSERT_EQ(stat2->row_count(), 2.);
//
//
//
////      auto stat2 = stat1->predicate_statistics("C_D_ID", "!=", opossum::AllParameterVariant(2));
//      auto stat2 = stat1->predicate_statistics("b", "<", opossum::AllParameterVariant(458.2f));
//      ASSERT_GT(stat2->row_count(), 1.);
//      ASSERT_LT(stat2->row_count(), 2.);
//    }

}  // namespace opossum
