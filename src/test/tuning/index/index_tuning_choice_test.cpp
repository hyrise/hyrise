#include <memory>

#include "../../base_test.hpp"
#include "tuning/index/index_tuning_choice.hpp"
#include "tuning/index/index_tuning_operation.hpp"

namespace opossum {

class IndexTuningChoiceTest : public BaseTest {
 protected:
  void SetUp() override {}

  ColumnRef column_ref{"table_name", ColumnID{0}};
};

TEST_F(IndexTuningChoiceTest, GetDesirability) {
  IndexTuningChoice choice{column_ref};

  // Constructor default value
  EXPECT_FLOAT_EQ(choice.desirability(), 0.0f);

  // Manual variable assignment
  choice.saved_work = 1.0f;
  EXPECT_FLOAT_EQ(choice.desirability(), 1.0f);
}

TEST_F(IndexTuningChoiceTest, GetCost) {
  IndexTuningChoice choice{column_ref};

  EXPECT_FLOAT_EQ(choice.cost(), 0.0f);

  choice.memory_cost = 1.0f;
  EXPECT_FLOAT_EQ(choice.cost(), 1.0f);
}

TEST_F(IndexTuningChoiceTest, GetConfidence) {
  IndexTuningChoice choice{column_ref};

  EXPECT_FLOAT_EQ(choice.confidence(), 1.0f);
}

TEST_F(IndexTuningChoiceTest, GetCurrentlyChosen) {
  IndexTuningChoice choice_nonexisting{column_ref, false};
  EXPECT_EQ(choice_nonexisting.is_currently_chosen(), false);

  IndexTuningChoice choice_existing{column_ref, true};
  EXPECT_EQ(choice_existing.is_currently_chosen(), true);

  choice_nonexisting.index_exists = true;
  EXPECT_EQ(choice_nonexisting.is_currently_chosen(), true);
}

TEST_F(IndexTuningChoiceTest, GetInvalidates) {
  auto choice1 = std::make_shared<IndexTuningChoice>(column_ref, false);
  auto choice2 = std::make_shared<IndexTuningChoice>(column_ref, true);
  EXPECT_TRUE(choice1->invalidates().empty());
  EXPECT_TRUE(choice2->invalidates().empty());
  choice1->add_invalidate(choice2);
  EXPECT_EQ(choice1->invalidates().size(), 1u);
  EXPECT_TRUE(choice2->invalidates().empty());
  auto invalidated_choice = *(choice1->invalidates().cbegin());
  EXPECT_EQ(invalidated_choice, choice2);
}

TEST_F(IndexTuningChoiceTest, Accept) {
  // Expect an IndexTuningOperation that is configured to create a non-existing index
  IndexTuningChoice choice_nonexisting{column_ref, false};
  auto accept_operation = std::dynamic_pointer_cast<IndexTuningOperation>(choice_nonexisting.accept());

  EXPECT_EQ(accept_operation->column(), column_ref);
  EXPECT_EQ(accept_operation->type(), choice_nonexisting.type);
  EXPECT_EQ(accept_operation->will_create_else_delete(), true);

  // If the index is already existing, the result is a null-operation, realized as a (not-subclassed) TuningOperation
  // (that then cannot be cast to IndexOp.)
  IndexTuningChoice choice_existing{column_ref, true};
  auto accept_noop = std::dynamic_pointer_cast<IndexTuningOperation>(choice_existing.accept());
  EXPECT_EQ(accept_noop, nullptr);
}

TEST_F(IndexTuningChoiceTest, Reject) {
  // Analogous to accept(), but the other way around
  IndexTuningChoice choice_nonexisting{column_ref, false};
  auto reject_noop = std::dynamic_pointer_cast<IndexTuningOperation>(choice_nonexisting.reject());
  EXPECT_EQ(reject_noop, nullptr);

  IndexTuningChoice choice_existing{column_ref, true};
  auto reject_operation = std::dynamic_pointer_cast<IndexTuningOperation>(choice_existing.reject());
  EXPECT_EQ(reject_operation->column(), column_ref);
  EXPECT_EQ(reject_operation->type(), choice_nonexisting.type);
  EXPECT_EQ(reject_operation->will_create_else_delete(), false);
}
}  // namespace opossum
