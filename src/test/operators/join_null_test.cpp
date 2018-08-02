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
#include "operators/join_mpsm.hpp"
#include "operators/join_nested_loop.hpp"
#include "operators/join_sort_merge.hpp"
#include "operators/projection.hpp"
#include "operators/table_scan.hpp"
#include "operators/union_all.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"
#include "types.hpp"

namespace opossum {

/*
This contains the tests for null value support for join implementations.
*/

template <typename T>
class JoinNullTest : public JoinTest {
 protected:
  void SetUp() override {
    JoinTest::SetUp();

    _table_wrapper_a_null = std::make_shared<TableWrapper>(load_table("src/test/tables/int_float_with_null.tbl", 2));
    _table_wrapper_a_null->execute();

    // load and create DictionaryColumn tables
    auto table = load_table("src/test/tables/int_float_with_null.tbl", 2);
    ChunkEncoder::encode_chunks(table, {ChunkID{0}, ChunkID{1}});

    _table_wrapper_a_null_dict = std::make_shared<TableWrapper>(std::move(table));
    _table_wrapper_a_null_dict->execute();
  }

  std::shared_ptr<TableWrapper> _table_wrapper_a_null;
  std::shared_ptr<TableWrapper> _table_wrapper_a_null_dict;
};

using JoinNullTypes = ::testing::Types<JoinHash, JoinSortMerge, JoinNestedLoop, JoinMPSM>;
TYPED_TEST_CASE(JoinNullTest, JoinNullTypes);

TYPED_TEST(JoinNullTest, InnerJoinWithNull) {
  this->template test_join_output<TypeParam>(
      this->_table_wrapper_a, this->_table_wrapper_a_null, ColumnIDPair(ColumnID{0}, ColumnID{0}),
      PredicateCondition::Equals, JoinMode::Inner, "src/test/tables/joinoperators/int_float_null_inner.tbl", 1);
}

TYPED_TEST(JoinNullTest, InnerJoinWithNullDict) {
  this->template test_join_output<TypeParam>(
      this->_table_wrapper_a_dict, this->_table_wrapper_a_null_dict, ColumnIDPair(ColumnID{0}, ColumnID{0}),
      PredicateCondition::Equals, JoinMode::Inner, "src/test/tables/joinoperators/int_float_null_inner.tbl", 1);
}

TYPED_TEST(JoinNullTest, InnerJoinWithNull2) {
  this->template test_join_output<TypeParam>(
      this->_table_wrapper_m, this->_table_wrapper_n, ColumnIDPair(ColumnID{0}, ColumnID{0}),
      PredicateCondition::Equals, JoinMode::Inner, "src/test/tables/joinoperators/int_inner_join_null.tbl", 1);
}

TYPED_TEST(JoinNullTest, InnerJoinWithNullDict2) {
  this->template test_join_output<TypeParam>(
      this->_table_wrapper_m_dict, this->_table_wrapper_n_dict, ColumnIDPair(ColumnID{0}, ColumnID{0}),
      PredicateCondition::Equals, JoinMode::Inner, "src/test/tables/joinoperators/int_inner_join_null.tbl", 1);
}

TYPED_TEST(JoinNullTest, InnerJoinWithNullRef2) {
  auto scan_a =
      std::make_shared<TableScan>(this->_table_wrapper_m, ColumnID{1}, PredicateCondition::GreaterThanEquals, 0);
  scan_a->execute();
  auto scan_b =
      std::make_shared<TableScan>(this->_table_wrapper_n, ColumnID{1}, PredicateCondition::GreaterThanEquals, 0);
  scan_b->execute();

  this->template test_join_output<TypeParam>(scan_a, scan_b, ColumnIDPair(ColumnID{0}, ColumnID{0}),
                                             PredicateCondition::Equals, JoinMode::Inner,
                                             "src/test/tables/joinoperators/int_inner_join_null_ref.tbl", 1);
}

TYPED_TEST(JoinNullTest, LeftJoinWithNullAsOuter) {
  this->template test_join_output<TypeParam>(this->_table_wrapper_a_null, this->_table_wrapper_b,
                                             ColumnIDPair(ColumnID{0}, ColumnID{0}), PredicateCondition::Equals,
                                             JoinMode::Left, "src/test/tables/joinoperators/int_left_join_null.tbl", 1);
}

TYPED_TEST(JoinNullTest, LeftJoinWithNullAsOuterDict) {
  this->template test_join_output<TypeParam>(this->_table_wrapper_a_null_dict, this->_table_wrapper_b_dict,
                                             ColumnIDPair(ColumnID{0}, ColumnID{0}), PredicateCondition::Equals,
                                             JoinMode::Left, "src/test/tables/joinoperators/int_left_join_null.tbl", 1);
}

TYPED_TEST(JoinNullTest, LeftJoinWithNullAsInner) {
  this->template test_join_output<TypeParam>(
      this->_table_wrapper_b, this->_table_wrapper_a_null, ColumnIDPair(ColumnID{0}, ColumnID{0}),
      PredicateCondition::Equals, JoinMode::Left, "src/test/tables/joinoperators/int_left_join_null_inner.tbl", 1);
}

TYPED_TEST(JoinNullTest, LeftJoinWithNullAsInnerDict) {
  this->template test_join_output<TypeParam>(
      this->_table_wrapper_b_dict, this->_table_wrapper_a_null_dict, ColumnIDPair(ColumnID{0}, ColumnID{0}),
      PredicateCondition::Equals, JoinMode::Left, "src/test/tables/joinoperators/int_left_join_null_inner.tbl", 1);
}

TYPED_TEST(JoinNullTest, RightJoinWithNullAsOuter) {
  this->template test_join_output<TypeParam>(
      this->_table_wrapper_b, this->_table_wrapper_a_null, ColumnIDPair(ColumnID{0}, ColumnID{0}),
      PredicateCondition::Equals, JoinMode::Right, "src/test/tables/joinoperators/int_right_join_null.tbl", 1);
}

TYPED_TEST(JoinNullTest, RightJoinWithNullAsOuterDict) {
  this->template test_join_output<TypeParam>(
      this->_table_wrapper_b_dict, this->_table_wrapper_a_null_dict, ColumnIDPair(ColumnID{0}, ColumnID{0}),
      PredicateCondition::Equals, JoinMode::Right, "src/test/tables/joinoperators/int_right_join_null.tbl", 1);
}

TYPED_TEST(JoinNullTest, RightJoinWithNullAsInner) {
  this->template test_join_output<TypeParam>(
      this->_table_wrapper_a_null, this->_table_wrapper_b, ColumnIDPair(ColumnID{0}, ColumnID{0}),
      PredicateCondition::Equals, JoinMode::Right, "src/test/tables/joinoperators/int_right_join_null_inner.tbl", 1);
}

TYPED_TEST(JoinNullTest, RightJoinWithNullAsInnerDict) {
  this->template test_join_output<TypeParam>(
      this->_table_wrapper_a_null_dict, this->_table_wrapper_b_dict, ColumnIDPair(ColumnID{0}, ColumnID{0}),
      PredicateCondition::Equals, JoinMode::Right, "src/test/tables/joinoperators/int_right_join_null_inner.tbl", 1);
}

}  // namespace opossum
