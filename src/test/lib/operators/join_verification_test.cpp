#include "base_test.hpp"

#include "operators/join_verification.hpp"
#include "operators/projection.hpp"
#include "operators/table_wrapper.hpp"

namespace hyrise {

class OperatorsJoinVerificationTest : public BaseTest {
 public:
  void SetUp() override {
    const auto dummy_table =
        std::make_shared<Table>(TableColumnDefinitions{{"a", DataType::Int, false}}, TableType::Data);
    dummy_input = std::make_shared<TableWrapper>(dummy_table);
    dummy_input->never_clear_output();
  }

  std::shared_ptr<AbstractOperator> dummy_input;
};

TEST_F(OperatorsJoinVerificationTest, DescriptionAndName) {
  const auto primary_predicate = OperatorJoinPredicate{{ColumnID{0}, ColumnID{0}}, PredicateCondition::Equals};
  const auto secondary_predicate = OperatorJoinPredicate{{ColumnID{0}, ColumnID{0}}, PredicateCondition::NotEquals};

  const auto join_operator =
      std::make_shared<JoinVerification>(dummy_input, dummy_input, JoinMode::Inner, primary_predicate,
                                         std::vector<OperatorJoinPredicate>{secondary_predicate});

  EXPECT_EQ(join_operator->description(DescriptionMode::SingleLine),
            "JoinVerification (Inner) Column #0 = Column #0 AND Column #0 != Column #0");
  EXPECT_EQ(join_operator->description(DescriptionMode::MultiLine),
            "JoinVerification (Inner)\nColumn #0 = Column #0\nAND Column #0 != Column #0");

  dummy_input->execute();
  EXPECT_EQ(join_operator->description(DescriptionMode::SingleLine), "JoinVerification (Inner) a = a AND a != a");
  EXPECT_EQ(join_operator->description(DescriptionMode::MultiLine), "JoinVerification (Inner)\na = a\nAND a != a");

  EXPECT_EQ(join_operator->name(), "JoinVerification");
}

TEST_F(OperatorsJoinVerificationTest, DeepCopy) {
  const auto primary_predicate = OperatorJoinPredicate{{ColumnID{0}, ColumnID{0}}, PredicateCondition::Equals};
  const auto join_operator =
      std::make_shared<JoinVerification>(dummy_input, dummy_input, JoinMode::Left, primary_predicate);
  const auto abstract_join_operator_copy = join_operator->deep_copy();
  const auto join_operator_copy = std::dynamic_pointer_cast<JoinVerification>(join_operator);

  ASSERT_TRUE(join_operator_copy);

  EXPECT_EQ(join_operator_copy->mode(), JoinMode::Left);
  EXPECT_EQ(join_operator_copy->primary_predicate(), primary_predicate);
  EXPECT_NE(join_operator_copy->left_input(), nullptr);
  EXPECT_NE(join_operator_copy->right_input(), nullptr);
}

}  // namespace hyrise
