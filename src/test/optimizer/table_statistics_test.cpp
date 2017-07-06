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

    table_a_stats = opossum::StorageManager::get().get_table("table_a")->get_table_statistics();
    table_a_container = TableContainer{table_a_stats, table_a};

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
  TableContainer table_a_container;
  TableContainer check_statistic_with_table_scan(const TableContainer& table_container, const std::string& column_name,
                                                 const ScanType scan_type, const AllParameterVariant value,
                                                 const optional<AllTypeVariant> value2 = nullopt,
                                                 const int offset = 0) {
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

  std::vector<int> int_values{0, 1, 6, 7};
  std::vector<float> float_values{0.f, 1.f, 6.f, 7.f};
  std::vector<double> double_values{0., 1., 6., 7.};
  std::vector<std::string> string_values{"a", "b", "g", "h"};
};

TEST_F(TableStatisticsTest, GetTableTest) { assert_equal_row_count(table_a_container); }

TEST_F(TableStatisticsTest, NotEqualTest) {
  ScanType scan_type = ScanType::OpNotEquals;
  check_column_with_values(table_a_container, "a", scan_type, int_values);
  check_column_with_values(table_a_container, "b", scan_type, float_values);
  check_column_with_values(table_a_container, "c", scan_type, double_values);
  check_column_with_values(table_a_container, "d", scan_type, string_values);
}

TEST_F(TableStatisticsTest, EqualTest) {
  ScanType scan_type = ScanType::OpEquals;
  check_column_with_values(table_a_container, "a", scan_type, int_values);
  check_column_with_values(table_a_container, "b", scan_type, float_values);
  check_column_with_values(table_a_container, "c", scan_type, double_values);
  check_column_with_values(table_a_container, "d", scan_type, string_values);
}

TEST_F(TableStatisticsTest, LessThanTest) {
  ScanType scan_type = ScanType::OpLessThan;
  check_column_with_values(table_a_container, "a", scan_type, int_values);
  std::vector<float> custom_float_values{0.f, 1.f, 5.5f, 7.f};
  check_column_with_values(table_a_container, "b", scan_type, custom_float_values);
  std::vector<double> custom_double_values{0., 1., 5.5, 7.};
  check_column_with_values(table_a_container, "c", scan_type, custom_double_values);
  //  check_column_with_values(table_a_container, "d", scan_type, string_values);
}

TEST_F(TableStatisticsTest, LessEqualThanTest) {
  ScanType scan_type = ScanType::OpLessThanEquals;
  check_column_with_values(table_a_container, "a", scan_type, int_values);
  check_column_with_values(table_a_container, "b", scan_type, float_values);
  check_column_with_values(table_a_container, "c", scan_type, double_values);
  //  check_column_with_values(table_a_container, "d", scan_type, string_values);
}

TEST_F(TableStatisticsTest, GreaterThanTest) {
  ScanType scan_type = ScanType::OpGreaterThan;
  check_column_with_values(table_a_container, "a", scan_type, int_values);
  std::vector<float> custom_float_values{0.f, 1.5f, 6.f, 7.f};
  check_column_with_values(table_a_container, "b", scan_type, custom_float_values);
  std::vector<double> custom_double_values{0., 1.5, 6., 7.};
  check_column_with_values(table_a_container, "c", scan_type, custom_double_values);
  //  check_column_with_values(table_a_container, "d", scan_type, string_values);
}

TEST_F(TableStatisticsTest, GreaterEqualThanTest) {
  ScanType scan_type = ScanType::OpGreaterThanEquals;
  check_column_with_values(table_a_container, "a", scan_type, int_values);
  check_column_with_values(table_a_container, "b", scan_type, float_values);
  check_column_with_values(table_a_container, "c", scan_type, double_values);
  //  check_column_with_values(table_a_container, "d", scan_type, string_values);
}

//
//    TEST_F(TableStatisticsTest, NotEqualTest) {
//      auto stat1 = table_a_stats->predicate_statistics("a", ScanType::OpNotEquals, opossum::AllParameterVariant(123));
//      ASSERT_EQ(stat1->row_count(), 2.);
//
//      auto stat2 = table_a_stats->predicate_statistics("a", ScanType::OpNotEquals,
//      opossum::AllParameterVariant(458.2f)); ASSERT_EQ(stat2->row_count(), 2.);
//
//
//
////      auto stat2 = stat1->predicate_statistics("C_D_ID", ScanType::OpNotEquals, opossum::AllParameterVariant(2));
//      auto stat2 = stat1->predicate_statistics("b", ScanType::OpLessThan, opossum::AllParameterVariant(458.2f));
//      ASSERT_GT(stat2->row_count(), 1.);
//      ASSERT_LT(stat2->row_count(), 2.);
//    }

}  // namespace opossum
