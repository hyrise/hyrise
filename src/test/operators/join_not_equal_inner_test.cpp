#include <iostream>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <utility>

#include "../base_test.hpp"
#include "gtest/gtest.h"
#include "join_test.hpp"

#include "../../lib/operators/get_table.hpp"
#include "../../lib/operators/join_hash.hpp"
#include "../../lib/operators/join_nested_loop_a.hpp"
#include "../../lib/operators/join_nested_loop_b.hpp"
#include "../../lib/operators/print.hpp"
#include "../../lib/operators/table_scan.hpp"
#include "../../lib/storage/storage_manager.hpp"
#include "../../lib/storage/table.hpp"
#include "../../lib/types.hpp"

namespace opossum {

template <typename T>
class JoinNotEqualInnerTest : public JoinTest {};

// here we define all Join types
typedef ::testing::Types<JoinNestedLoopA, JoinNestedLoopB> JoinNotEqualInnerTypes;
TYPED_TEST_CASE(JoinNotEqualInnerTest, JoinNotEqualInnerTypes);

// This operator is not supported for the SortMergeJoin
TYPED_TEST(JoinNotEqualInnerTest, NotEqualInnerJoin) {
  // Joining two Integer Columns
  this->template test_join_output<TypeParam>(
      this->_table_wrapper_a, this->_table_wrapper_b, std::pair<std::string, std::string>("a", "a"), "!=", Inner,
      std::string("left."), std::string("right."), "src/test/tables/joinoperators/int_notequal_inner_join.tbl", 1);
  // Joining two Float Columns
  this->template test_join_output<TypeParam>(
      this->_table_wrapper_a, this->_table_wrapper_b, std::pair<std::string, std::string>("b", "b"), "!=", Inner,
      std::string("left."), std::string("right."), "src/test/tables/joinoperators/float_notequal_inner_join.tbl", 1);
}

}  // namespace opossum
