#include "../base_test.hpp"

#include "operators/join_sort_merge.hpp"
#include "operators/table_wrapper.hpp"
#include "types.hpp"

namespace opossum {

class JoinSortMergeTest : public BaseTest {
 protected:
  static void SetUpTestCase() {
    _table_wrapper =
        std::make_shared<TableWrapper>(load_table("resources/test_data/tbl/joinoperators/anti_int4.tbl", 2));
    _table_wrapper->execute();
  }

  void SetUp() override {}

  inline static std::shared_ptr<TableWrapper> _table_wrapper;
};

TEST_F(JoinSortMergeTest, UnsupportedJoinModes) {
  if (!HYRISE_DEBUG) GTEST_SKIP();

  const auto execute_join = [&](const JoinMode mode, const PredicateCondition predicate_condition) {
    std::make_shared<JoinSortMerge>(_table_wrapper, _table_wrapper, mode, OperatorJoinPredicate{{ColumnID{0}, ColumnID{0}}, predicate_condition});
  };

  // The sort merge join supports both anti and semi joins, but only with equal predicates
  EXPECT_NO_THROW(execute_join(JoinMode::AntiNullAsTrue, PredicateCondition::Equals));
  EXPECT_NO_THROW(execute_join(JoinMode::AntiNullAsFalse, PredicateCondition::Equals));
  EXPECT_NO_THROW(execute_join(JoinMode::Semi, PredicateCondition::Equals));
  EXPECT_THROW(execute_join(JoinMode::AntiNullAsTrue, PredicateCondition::LessThan), std::logic_error);
  EXPECT_THROW(execute_join(JoinMode::AntiNullAsFalse, PredicateCondition::LessThan), std::logic_error);
  EXPECT_THROW(execute_join(JoinMode::Semi, PredicateCondition::LessThan), std::logic_error);

  // The sort merge join does not support cross joins.
  EXPECT_THROW(execute_join(JoinMode::Cross, PredicateCondition::Equals), std::logic_error);

  // The sort merge join supports the != join predicate, but not for outer joins
  EXPECT_NO_THROW(execute_join(JoinMode::Inner, PredicateCondition::NotEquals));
  EXPECT_NO_THROW(execute_join(JoinMode::Inner, PredicateCondition::NotEquals));
  EXPECT_THROW(execute_join(JoinMode::Left, PredicateCondition::NotEquals), std::logic_error);
  EXPECT_THROW(execute_join(JoinMode::Right, PredicateCondition::NotEquals), std::logic_error);
}

}  // namespace opossum
