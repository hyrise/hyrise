#include <iostream>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <utility>

#include "../base_test.hpp"
#include "gtest/gtest.h"
#include "join_test.hpp"

#include "operators/get_table.hpp"
#include "operators/join_hash.hpp"
#include "operators/join_nested_loop_a.hpp"
#include "operators/join_nested_loop_b.hpp"
#include "operators/table_scan.hpp"
#include "operators/union_all.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"
#include "types.hpp"

namespace opossum {

/*
This contains the tests for the Hash Join implementation.
*/

class JoinHashTest : public JoinTest {
 protected:
  void SetUp() override {
    JoinTest::SetUp();

    _table_wrapper_a_null = std::make_shared<TableWrapper>(load_table("src/test/tables/int_float_with_null.tbl", 2));

    _table_wrapper_a_null->execute();
  }

  std::shared_ptr<TableWrapper> _table_wrapper_a_null;
};

TEST_F(JoinHashTest, InnerJoinWithNull) {
  test_join_output<JoinHash>(_table_wrapper_a, _table_wrapper_a_null,
                             std::pair<ColumnID, ColumnID>(ColumnID{0}, ColumnID{0}), ScanType::OpEquals,
                             JoinMode::Inner, "src/test/tables/joinoperators/int_float_null_inner.tbl", 1);
}

}  // namespace opossum
