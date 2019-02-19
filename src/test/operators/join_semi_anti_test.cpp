#include <iostream>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <type_traits>
#include <utility>

#include "base_test.hpp"
#include "gtest/gtest.h"
#include "join_test.hpp"

#include "operators/join_hash.hpp"
#include "operators/join_sort_merge.hpp"
#include "operators/table_scan.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"
#include "types.hpp"

namespace opossum {

/*
This contains the tests for Semi- and Anti-Join implementations.
*/
template <typename T>
class JoinSemiAntiTest : public JoinTest {
 protected:
  void SetUp() override {
    JoinTest::SetUp();

    _table_wrapper_semi_a =
        std::make_shared<TableWrapper>(load_table("resources/test_data/tbl/joinoperators/semi_left.tbl", 2));
    _table_wrapper_semi_b =
        std::make_shared<TableWrapper>(load_table("resources/test_data/tbl/joinoperators/semi_right.tbl", 2));

    _table_wrapper_semi_a->execute();
    _table_wrapper_semi_b->execute();
  }

  std::shared_ptr<TableWrapper> _table_wrapper_semi_a, _table_wrapper_semi_b;
};

using SemiAntiJoinTypes = ::testing::Types<JoinHash, JoinSortMerge>;
TYPED_TEST_CASE(JoinSemiAntiTest, SemiAntiJoinTypes, );  // NOLINT(whitespace/parens)

TYPED_TEST(JoinSemiAntiTest, SemiJoin) {
  this->template test_join_output<TypeParam>(this->_table_wrapper_k, this->_table_wrapper_a, {ColumnID{0}, ColumnID{0}},
                                             PredicateCondition::Equals, JoinMode::Semi,
                                             "resources/test_data/tbl/int.tbl", 1);
}

TYPED_TEST(JoinSemiAntiTest, SemiJoinRefSegments) {
  auto scan_a = this->create_table_scan(this->_table_wrapper_k, ColumnID{0}, PredicateCondition::GreaterThanEquals, 0);
  scan_a->execute();

  auto scan_b = this->create_table_scan(this->_table_wrapper_a, ColumnID{0}, PredicateCondition::GreaterThanEquals, 0);
  scan_b->execute();

  this->template test_join_output<TypeParam>(scan_a, scan_b, {ColumnID{0}, ColumnID{0}}, PredicateCondition::Equals,
                                             JoinMode::Semi, "resources/test_data/tbl/int.tbl", 1);
}

TYPED_TEST(JoinSemiAntiTest, SemiJoinBig) {
  this->template test_join_output<TypeParam>(this->_table_wrapper_semi_a, this->_table_wrapper_semi_b,
                                             {ColumnID{0}, ColumnID{0}}, PredicateCondition::Equals, JoinMode::Semi,
                                             "resources/test_data/tbl/joinoperators/semi_result.tbl", 1);
}

TYPED_TEST(JoinSemiAntiTest, AntiJoin) {
  this->template test_join_output<TypeParam>(this->_table_wrapper_k, this->_table_wrapper_a, {ColumnID{0}, ColumnID{0}},
                                             PredicateCondition::Equals, JoinMode::Anti,
                                             "resources/test_data/tbl/joinoperators/anti_int4.tbl", 1);
}

TYPED_TEST(JoinSemiAntiTest, AntiJoinRefSegments) {
  auto scan_a = this->create_table_scan(this->_table_wrapper_k, ColumnID{0}, PredicateCondition::GreaterThanEquals, 0);
  scan_a->execute();

  auto scan_b = this->create_table_scan(this->_table_wrapper_a, ColumnID{0}, PredicateCondition::GreaterThanEquals, 0);
  scan_b->execute();

  this->template test_join_output<TypeParam>(scan_a, scan_b, {ColumnID{0}, ColumnID{0}}, PredicateCondition::Equals,
                                             JoinMode::Anti, "resources/test_data/tbl/joinoperators/anti_int4.tbl", 1);
}

TYPED_TEST(JoinSemiAntiTest, AntiJoinBig) {
  this->template test_join_output<TypeParam>(this->_table_wrapper_semi_a, this->_table_wrapper_semi_b,
                                             {ColumnID{0}, ColumnID{0}}, PredicateCondition::Equals, JoinMode::Anti,
                                             "resources/test_data/tbl/joinoperators/anti_result.tbl", 1);
}

}  // namespace opossum
