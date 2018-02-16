#include <memory>

#include "../../base_test.hpp"
#include "tuning/index/index_choice.hpp"
#include "tuning/index/index_operation.hpp"

namespace opossum {

class IndexChoiceTest : public BaseTest {
 protected:
  void SetUp() override {}

  ColumnRef column_ref{"table_name", ColumnID{0}};
};

TEST_F(IndexChoiceTest, GetDesirabilityTest) {
  IndexChoice choice{column_ref};

  // Constructor default value
  EXPECT_FLOAT_EQ(choice.desirability(), 0.0f);

  // Manual variable assignment
  choice.saved_work = 1.0f;
  EXPECT_FLOAT_EQ(choice.desirability(), 1.0f);
}

TEST_F(IndexChoiceTest, GetCostTest) {
  IndexChoice choice{column_ref};

  EXPECT_FLOAT_EQ(choice.cost(), 0.0f);

  choice.memory_cost = 1.0f;
  EXPECT_FLOAT_EQ(choice.cost(), 1.0f);
}

TEST_F(IndexChoiceTest, GetConfidenceTest) {
  IndexChoice choice{column_ref};

  EXPECT_FLOAT_EQ(choice.confidence(), 1.0f);
}

TEST_F(IndexChoiceTest, GetCurrentlyChosen) {
  IndexChoice choice_nonexisting{column_ref, false};
  EXPECT_EQ(choice_nonexisting.is_currently_chosen(), false);

  IndexChoice choice_existing{column_ref, true};
  EXPECT_EQ(choice_existing.is_currently_chosen(), true);

  choice_nonexisting.exists = true;
  EXPECT_EQ(choice_nonexisting.is_currently_chosen(), true);
}

TEST_F(IndexChoiceTest, AcceptTest) {
  // Expect an IndexOperation that is configured to create a non-existing index
  IndexChoice choice_nonexisting{column_ref, false};
  auto accept_operation = std::dynamic_pointer_cast<IndexOperation>(choice_nonexisting.accept());

  EXPECT_EQ(accept_operation->column(), column_ref);
  EXPECT_EQ(accept_operation->type(), choice_nonexisting.type);
  EXPECT_EQ(accept_operation->create(), true);

  // If the index is already existing, the result is a null-operation, realized as a (not-subclassed) TuningOperation
  // (that then cannot be cast to IndexOp.)
  IndexChoice choice_existing{column_ref, true};
  auto accept_noop = std::dynamic_pointer_cast<IndexOperation>(choice_existing.accept());
  EXPECT_EQ(accept_noop, nullptr);
}

TEST_F(IndexChoiceTest, RejectTest) {
  // Analogous to accept(), but the other way around
  IndexChoice choice_nonexisting{column_ref, false};
  auto reject_noop = std::dynamic_pointer_cast<IndexOperation>(choice_nonexisting.reject());
  EXPECT_EQ(reject_noop, nullptr);

  IndexChoice choice_existing{column_ref, true};
  auto reject_operation = std::dynamic_pointer_cast<IndexOperation>(choice_existing.reject());
  EXPECT_EQ(reject_operation->column(), column_ref);
  EXPECT_EQ(reject_operation->type(), choice_nonexisting.type);
  EXPECT_EQ(reject_operation->create(), false);
}
}  // namespace opossum
