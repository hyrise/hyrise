#include <memory>

#include "../../base_test.hpp"
#include "tuning/index/index_tuning_operation.hpp"
#include "tuning/index/index_tuning_option.hpp"

namespace opossum {

class IndexTuningOptionTest : public BaseTest {
 protected:
  void SetUp() override {}

  IndexableColumnSet indexable_column_set{"table_name", ColumnID{0}};
};

TEST_F(IndexTuningOptionTest, GetDesirability) {
  IndexTuningOption choice{indexable_column_set};

  // Constructor default value
  EXPECT_FLOAT_EQ(choice.desirability(), 0.0f);

  // Manual variable assignment
  choice.saved_work = 1.0f;
  EXPECT_FLOAT_EQ(choice.desirability(), 1.0f);
}

TEST_F(IndexTuningOptionTest, GetCost) {
  IndexTuningOption choice{indexable_column_set};

  EXPECT_FLOAT_EQ(choice.cost(), 0.0f);

  choice.memory_cost = 1.0f;
  EXPECT_FLOAT_EQ(choice.cost(), 1.0f);
}

TEST_F(IndexTuningOptionTest, GetConfidence) {
  IndexTuningOption choice{indexable_column_set};

  EXPECT_FLOAT_EQ(choice.confidence(), 1.0f);
}

TEST_F(IndexTuningOptionTest, GetCurrentlyChosen) {
  IndexTuningOption choice_nonexisting{indexable_column_set, false};
  EXPECT_EQ(choice_nonexisting.is_currently_chosen(), false);

  IndexTuningOption choice_existing{indexable_column_set, true};
  EXPECT_EQ(choice_existing.is_currently_chosen(), true);

  choice_nonexisting.index_exists = true;
  EXPECT_EQ(choice_nonexisting.is_currently_chosen(), true);
}

TEST_F(IndexTuningOptionTest, GetInvalidates) {
  auto choice1 = std::make_shared<IndexTuningOption>(indexable_column_set, false);
  auto choice2 = std::make_shared<IndexTuningOption>(indexable_column_set, true);
  EXPECT_TRUE(choice1->invalidates().empty());
  EXPECT_TRUE(choice2->invalidates().empty());
  choice1->add_invalidate(choice2);
  EXPECT_EQ(choice1->invalidates().size(), 1u);
  EXPECT_TRUE(choice2->invalidates().empty());
  auto invalidated_choice = (choice1->invalidates().cbegin())->lock();
  EXPECT_EQ(invalidated_choice, choice2);
}

TEST_F(IndexTuningOptionTest, Accept) {
  // Expect an IndexTuningOperation that is configured to create a non-existing index
  IndexTuningOption choice_nonexisting{indexable_column_set, false};
  auto accept_operation = std::dynamic_pointer_cast<IndexTuningOperation>(choice_nonexisting.accept());

  EXPECT_EQ(accept_operation->column(), indexable_column_set);
  EXPECT_EQ(accept_operation->type(), choice_nonexisting.type);
  EXPECT_EQ(accept_operation->will_create_else_delete(), true);

  // If the index is already existing, the result is a null-operation, realized as a (not-subclassed) TuningOperation
  // (that then cannot be cast to IndexOp.)
  IndexTuningOption choice_existing{indexable_column_set, true};
  auto accept_noop = std::dynamic_pointer_cast<IndexTuningOperation>(choice_existing.accept());
  EXPECT_EQ(accept_noop, nullptr);
}

TEST_F(IndexTuningOptionTest, Reject) {
  // Analogous to accept(), but the other way around
  IndexTuningOption choice_nonexisting{indexable_column_set, false};
  auto reject_noop = std::dynamic_pointer_cast<IndexTuningOperation>(choice_nonexisting.reject());
  EXPECT_EQ(reject_noop, nullptr);

  IndexTuningOption choice_existing{indexable_column_set, true};
  auto reject_operation = std::dynamic_pointer_cast<IndexTuningOperation>(choice_existing.reject());
  EXPECT_EQ(reject_operation->column(), indexable_column_set);
  EXPECT_EQ(reject_operation->type(), choice_nonexisting.type);
  EXPECT_EQ(reject_operation->will_create_else_delete(), false);
}
}  // namespace opossum
