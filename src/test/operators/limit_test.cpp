#include <limits>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "operators/limit.hpp"
#include "operators/table_scan.hpp"
#include "operators/table_wrapper.hpp"
#include "types.hpp"

namespace opossum {

class OperatorsLimitTest : public BaseTest {
 protected:
  void SetUp() override {
    _table_wrapper = std::make_shared<TableWrapper>(load_table("src/test/tables/int_int3.tbl", 3));
    _table_wrapper->execute();
  }

  void test_limit_1() {
    auto limit = std::make_shared<Limit>(_input_operator, 1);
    limit->execute();

    auto expected_result = load_table("src/test/tables/int_int3_limit_1.tbl", 3);
    EXPECT_TABLE_EQ_ORDERED(limit->get_output(), expected_result);
  }

  void test_limit_2() {
    auto limit = std::make_shared<Limit>(_input_operator, 2);
    limit->execute();

    auto expected_result = load_table("src/test/tables/int_int3_limit_2.tbl", 3);
    EXPECT_TABLE_EQ_ORDERED(limit->get_output(), expected_result);
  }

  /**
   * Limit across chunks.
   */
  void test_limit_4() {
    auto limit = std::make_shared<Limit>(_input_operator, 4);
    limit->execute();

    auto expected_result = load_table("src/test/tables/int_int3_limit_4.tbl", 3);
    EXPECT_TABLE_EQ_ORDERED(limit->get_output(), expected_result);
  }

  /**
   * Limit with more elements than exist in table.
   */
  void test_limit_10() {
    auto limit = std::make_shared<Limit>(_input_operator, 10);
    limit->execute();

    auto expected_result = load_table("src/test/tables/int_int3.tbl", 3);
    EXPECT_TABLE_EQ_ORDERED(limit->get_output(), expected_result);
  }

  std::shared_ptr<TableWrapper> _table_wrapper;
  std::shared_ptr<AbstractOperator> _input_operator;
};

TEST_F(OperatorsLimitTest, Limit1ValueColumn) {
  _input_operator = _table_wrapper;
  test_limit_1();
}

TEST_F(OperatorsLimitTest, Limit1ReferenceColumn) {
  // Filter accepts all rows in table.
  auto table_scan = std::make_shared<TableScan>(_table_wrapper, ColumnID{0}, ScanType::GreaterThan, -1);
  table_scan->execute();
  _input_operator = table_scan;
  test_limit_1();
}

TEST_F(OperatorsLimitTest, Limit2ValueColumn) {
  _input_operator = _table_wrapper;
  test_limit_2();
}

TEST_F(OperatorsLimitTest, Limit2ReferenceColumn) {
  // Filter accepts all rows in table.
  auto table_scan = std::make_shared<TableScan>(_table_wrapper, ColumnID{0}, ScanType::GreaterThan, -1);
  table_scan->execute();
  _input_operator = table_scan;
  test_limit_2();
}

TEST_F(OperatorsLimitTest, Limit4ValueColumn) {
  _input_operator = _table_wrapper;
  test_limit_4();
}

TEST_F(OperatorsLimitTest, Limit4ReferenceColumn) {
  // Filter accepts all rows in table.
  auto table_scan = std::make_shared<TableScan>(_table_wrapper, ColumnID{0}, ScanType::GreaterThan, -1);
  table_scan->execute();
  _input_operator = table_scan;
  test_limit_4();
}

TEST_F(OperatorsLimitTest, Limit10ValueColumn) {
  _input_operator = _table_wrapper;
  test_limit_10();
}

TEST_F(OperatorsLimitTest, Limit10ReferenceColumn) {
  // Filter accepts all rows in table.
  auto table_scan = std::make_shared<TableScan>(_table_wrapper, ColumnID{0}, ScanType::GreaterThan, -1);
  table_scan->execute();
  _input_operator = table_scan;
  test_limit_10();
}

}  // namespace opossum
