#include "base_test.hpp"

#include "operators/join_mpsm.hpp"
#include "operators/projection.hpp"
#include "operators/table_wrapper.hpp"

namespace opossum {

class OperatorsJoinMPSMTest : public ::testing::Test {
 public:
  void SetUp() override {
    const auto dummy_table =
        std::make_shared<Table>(TableColumnDefinitions{{"a", DataType::Int, false}}, TableType::Data);
    dummy_input = std::make_shared<TableWrapper>(dummy_table);
  }

  std::shared_ptr<AbstractOperator> dummy_input;
};

TEST_F(OperatorsJoinMPSMTest, DescriptionAndName) {
  const auto join_operator =
      std::make_shared<JoinMPSM>(dummy_input, dummy_input, JoinMode::Inner,
                                 OperatorJoinPredicate{{ColumnID{0}, ColumnID{0}}, PredicateCondition::Equals});

  EXPECT_EQ(join_operator->description(DescriptionMode::SingleLine),
            "Join MPSM (Inner Join where Column #0 = Column #0)");
  EXPECT_EQ(join_operator->description(DescriptionMode::MultiLine),
            "Join MPSM\n(Inner Join where Column #0 = Column #0)");

  dummy_input->execute();
  EXPECT_EQ(join_operator->description(DescriptionMode::SingleLine), "Join MPSM (Inner Join where a = a)");
  EXPECT_EQ(join_operator->description(DescriptionMode::MultiLine), "Join MPSM\n(Inner Join where a = a)");

  EXPECT_EQ(join_operator->name(), "Join MPSM");
}

TEST_F(OperatorsJoinMPSMTest, DeepCopy) {
  const auto primary_predicate = OperatorJoinPredicate{{ColumnID{0}, ColumnID{0}}, PredicateCondition::Equals};
  const auto join_operator = std::make_shared<JoinMPSM>(dummy_input, dummy_input, JoinMode::Left, primary_predicate);
  const auto abstract_join_operator_copy = join_operator->deep_copy();
  const auto join_operator_copy = std::dynamic_pointer_cast<JoinMPSM>(join_operator);

  ASSERT_TRUE(join_operator_copy);

  EXPECT_EQ(join_operator_copy->mode(), JoinMode::Left);
  EXPECT_EQ(join_operator_copy->primary_predicate(), primary_predicate);
  EXPECT_NE(join_operator_copy->input_left(), nullptr);
  EXPECT_NE(join_operator_copy->input_right(), nullptr);
}

}  // namespace opossum
