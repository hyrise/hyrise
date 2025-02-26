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
    auto int_int_7 = load_table("resources/test_data/tbl/int_int_shuffled.tbl", ChunkOffset{7});

    _int_int = std::make_shared<TableWrapper>(std::move(int_int_7));
    _int_int->never_clear_output();
    _int_int->execute();

    auto int_int_extended = load_table("resources/test_data/tbl/int_int_shuffled_3.tbl", ChunkOffset{7});

    _int_int_extended = std::make_shared<TableWrapper>(std::move(int_int_extended));
    _int_int_extended->never_clear_output();
    _int_int_extended->execute();
  }

 protected:
  std::shared_ptr<TableWrapper> _int_int;
  std::shared_ptr<TableWrapper> _int_int_extended;
};

TEST_F(OperatorsReduceTest, ScanTableWithFilterCreatedOnItself) {
  const auto expected = load_table("resources/test_data/tbl/int_int_shuffled.tbl", ChunkOffset{7});
  auto reduce = std::make_shared<Reduce>(_int_int, _int_int, OperatorJoinPredicate{ColumnIDPair(ColumnID{0}, ColumnID{0}), PredicateCondition::Equals}, false);
  reduce->execute();
  EXPECT_TABLE_EQ_UNORDERED(reduce->get_output(), expected);
}

TEST_F(OperatorsReduceTest, SimpleFilter) {
  auto reduce = std::make_shared<Reduce>(_int_int_extended, _int_int, OperatorJoinPredicate{ColumnIDPair(ColumnID{0}, ColumnID{0}), PredicateCondition::Equals}, false);

  const auto expected = load_table("resources/test_data/tbl/int_int_shuffled_3_res.tbl", ChunkOffset{7});
  reduce->execute();
  EXPECT_TABLE_EQ_UNORDERED(reduce->get_output(), expected);
}

TEST_F(OperatorsReduceTest, TwoReduces) {
  auto reduce0 = std::make_shared<Reduce>(_int_int_extended, _int_int, OperatorJoinPredicate{ColumnIDPair(ColumnID{0}, ColumnID{0}), PredicateCondition::Equals}, true);
  auto reduce1 = std::make_shared<Reduce>(_int_int, reduce0, OperatorJoinPredicate{ColumnIDPair(ColumnID{0}, ColumnID{0}), PredicateCondition::Equals}, false);

  reduce0->execute();
  reduce1->execute();

  const auto expected = load_table("resources/test_data/tbl/int_int_shuffled_3_res.tbl", ChunkOffset{7});
  EXPECT_TABLE_EQ_UNORDERED(reduce1->get_output(), expected);
}

TEST_F(OperatorsReduceTest, TwoReducesWithoutFilterUpdate) {
  auto reduce0 = std::make_shared<Reduce>(_int_int_extended, _int_int, OperatorJoinPredicate{ColumnIDPair(ColumnID{0}, ColumnID{0}), PredicateCondition::Equals}, false);
  auto reduce1 = std::make_shared<Reduce>(_int_int, reduce0, OperatorJoinPredicate{ColumnIDPair(ColumnID{0}, ColumnID{0}), PredicateCondition::Equals}, false);

  reduce0->execute();
  reduce1->execute();

  const auto expected = load_table("resources/test_data/tbl/int_int_shuffled.tbl", ChunkOffset{7});
  EXPECT_TABLE_EQ_UNORDERED(reduce1->get_output(), expected);
}

}  // namespace hyrise
