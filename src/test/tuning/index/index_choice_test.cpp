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

TEST_F(IndexChoiceTest, GetDesirability) {
  IndexChoice choice{column_ref};

  // Constructor default value
  EXPECT_FLOAT_EQ(choice.desirability(), 0.0f);

  // Manual variable assignment
  choice.saved_work = 1.0f;
  EXPECT_FLOAT_EQ(choice.desirability(), 1.0f);
}

TEST_F(IndexChoiceTest, GetCost) {
  IndexChoice choice{column_ref};

  EXPECT_FLOAT_EQ(choice.cost(), 0.0f);

  choice.memory_cost = 1.0f;
  EXPECT_FLOAT_EQ(choice.cost(), 1.0f);
}

TEST_F(IndexChoiceTest, GetConfidence) {
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

TEST_F(IndexChoiceTest, GetInvalidates) {
  auto choice1 = std::make_shared<IndexChoice>(column_ref, false);
  auto choice2 = std::make_shared<IndexChoice>(column_ref, true);
  EXPECT_TRUE(choice1->invalidates().empty());
  EXPECT_TRUE(choice2->invalidates().empty());
  choice1->add_invalidate(choice2);
  EXPECT_EQ(choice1->invalidates().size(), 1u);
  EXPECT_TRUE(choice2->invalidates().empty());
  auto invalidated_choice = *(choice1->invalidates().cbegin());
  EXPECT_EQ(invalidated_choice, choice2);
}

TEST_F(IndexChoiceTest, Accept) {
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

TEST_F(IndexChoiceTest, Reject) {
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
