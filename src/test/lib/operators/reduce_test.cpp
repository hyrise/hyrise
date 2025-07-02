#include <gtest/gtest.h>

#include <bitset>
#include <memory>
#include <string>
#include <utility>

#include "base_test.hpp"
#include "operators/reduce.hpp"
#include "operators/table_wrapper.hpp"
#include "types.hpp"

namespace hyrise {

class OperatorsReduceTest : public BaseTest {
 protected:
  void SetUp() override {
    auto int_int_0 = load_table("resources/test_data/tbl/reduce/int_int_shuffled.tbl", ChunkOffset{7});

    _int_int_0 = std::make_shared<TableWrapper>(std::move(int_int_0));
    _int_int_0->never_clear_output();
    _int_int_0->execute();

    auto int_int_1 = load_table("resources/test_data/tbl/reduce/int_int_shuffled_3.tbl", ChunkOffset{7});

    _int_int_1 = std::make_shared<TableWrapper>(std::move(int_int_1));
    _int_int_1->never_clear_output();
    _int_int_1->execute();
  }

 protected:
  std::shared_ptr<TableWrapper> _int_int_0;
  std::shared_ptr<TableWrapper> _int_int_1;
};

TEST_F(OperatorsReduceTest, ReduceTableWithFilterCreatedOnItself) {
  auto reduce = std::make_shared<Reduce<std::hash, 20>>(
      _int_int_0, _int_int_0, OperatorJoinPredicate{ColumnIDPair(ColumnID{0}, ColumnID{0}), PredicateCondition::Equals},
      false);
  reduce->execute();

  const auto expected = load_table("resources/test_data/tbl/reduce/int_int_shuffled.tbl", ChunkOffset{7});
  EXPECT_TABLE_EQ_UNORDERED(reduce->get_output(), expected);
}

TEST_F(OperatorsReduceTest, SimpleFilter) {
  auto reduce = std::make_shared<Reduce<std::hash, 20>>(
      _int_int_1, _int_int_0, OperatorJoinPredicate{ColumnIDPair(ColumnID{0}, ColumnID{0}), PredicateCondition::Equals},
      false);
  reduce->execute();

  const auto expected = load_table("resources/test_data/tbl/reduce/int_int_shuffled_3_res.tbl", ChunkOffset{7});
  EXPECT_TABLE_EQ_UNORDERED(reduce->get_output(), expected);
}

TEST_F(OperatorsReduceTest, TwoReduces) {
  auto reduce_0 = std::make_shared<Reduce<std::hash, 20>>(
      _int_int_1, _int_int_0, OperatorJoinPredicate{ColumnIDPair(ColumnID{0}, ColumnID{0}), PredicateCondition::Equals},
      true);
  auto reduce_1 = std::make_shared<Reduce<std::hash, 20>>(
      _int_int_0, reduce_0, OperatorJoinPredicate{ColumnIDPair(ColumnID{0}, ColumnID{0}), PredicateCondition::Equals},
      false);

  reduce_0->execute();
  reduce_1->execute();

  const auto expected = load_table("resources/test_data/tbl/reduce/two_reduces_result.tbl", ChunkOffset{7});
  EXPECT_TABLE_EQ_UNORDERED(reduce_1->get_output(), expected);
}

TEST_F(OperatorsReduceTest, TwoReducesWithoutFilterUpdate) {
  auto reduce_0 = std::make_shared<Reduce<std::hash, 20>>(
      _int_int_1, _int_int_0, OperatorJoinPredicate{ColumnIDPair(ColumnID{0}, ColumnID{0}), PredicateCondition::Equals},
      false);
  auto reduce_1 = std::make_shared<Reduce<std::hash, 20>>(
      _int_int_0, reduce_0, OperatorJoinPredicate{ColumnIDPair(ColumnID{0}, ColumnID{0}), PredicateCondition::Equals},
      false);

  reduce_0->execute();
  reduce_1->execute();

  const auto expected = load_table("resources/test_data/tbl/reduce/int_int_shuffled.tbl", ChunkOffset{7});
  EXPECT_TABLE_EQ_UNORDERED(reduce_1->get_output(), expected);
}

}  // namespace hyrise
