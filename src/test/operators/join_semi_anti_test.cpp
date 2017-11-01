#include <iostream>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <type_traits>
#include <utility>

#include "../base_test.hpp"
#include "gtest/gtest.h"
#include "join_test.hpp"

#include "operators/get_table.hpp"
#include "operators/join_hash.hpp"
#include "operators/table_scan.hpp"
#include "operators/union_all.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"
#include "types.hpp"

namespace opossum {

/*
This contains the tests for Semi- and Anti-Join implementations.
*/

class JoinSemiAntiTest : public JoinTest {
 protected:
  void SetUp() override { JoinTest::SetUp(); }
};

TEST_F(JoinSemiAntiTest, SemiJoin) {
  test_join_output<JoinHash>(_table_wrapper_k, _table_wrapper_a,
                             std::pair<ColumnID, ColumnID>(ColumnID{0}, ColumnID{0}), ScanType::OpEquals,
                             JoinMode::Semi, "src/test/tables/int.tbl", 1);
}

TEST_F(JoinSemiAntiTest, SemiJoinRefColumns) {
  auto scan_a = std::make_shared<TableScan>(this->_table_wrapper_k, ColumnID{0}, ScanType::OpGreaterThanEquals, 0);
  scan_a->execute();

  auto scan_b = std::make_shared<TableScan>(this->_table_wrapper_a, ColumnID{0}, ScanType::OpGreaterThanEquals, 0);
  scan_b->execute();

  test_join_output<JoinHash>(scan_a, scan_b, std::pair<ColumnID, ColumnID>(ColumnID{0}, ColumnID{0}),
                             ScanType::OpEquals, JoinMode::Semi, "src/test/tables/int.tbl", 1);
}

TEST_F(JoinSemiAntiTest, AntiJoin) {
  test_join_output<JoinHash>(_table_wrapper_k, _table_wrapper_a,
                             std::pair<ColumnID, ColumnID>(ColumnID{0}, ColumnID{0}), ScanType::OpEquals,
                             JoinMode::Anti, "src/test/tables/joinoperators/anti_int4.tbl", 1);
}

TEST_F(JoinSemiAntiTest, AntiJoinRefColumns) {
  auto scan_a = std::make_shared<TableScan>(this->_table_wrapper_k, ColumnID{0}, ScanType::OpGreaterThanEquals, 0);
  scan_a->execute();

  auto scan_b = std::make_shared<TableScan>(this->_table_wrapper_a, ColumnID{0}, ScanType::OpGreaterThanEquals, 0);
  scan_b->execute();

  test_join_output<JoinHash>(scan_a, scan_b, std::pair<ColumnID, ColumnID>(ColumnID{0}, ColumnID{0}),
                             ScanType::OpEquals, JoinMode::Anti, "src/test/tables/joinoperators/anti_int4.tbl", 1);
}

}  // namespace opossum
