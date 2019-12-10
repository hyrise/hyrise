#include <memory>

#include "base_test.hpp"
#include "gtest/gtest.h"

#include "hyrise.hpp"
#include "logical_query_plan/create_prepared_plan_node.hpp"
#include "logical_query_plan/dummy_table_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "operators/maintenance/create_prepared_plan.hpp"
#include "storage/prepared_plan.hpp"

namespace opossum {

class CreatePreparedPlanTest : public BaseTest {
 protected:
  void SetUp() override {
    prepared_plan = std::make_shared<PreparedPlan>(DummyTableNode::make(), std::vector<ParameterID>{});
    create_prepared_plan = std::make_shared<CreatePreparedPlan>("prepared_plan_a", prepared_plan);
  }

  std::shared_ptr<PreparedPlan> prepared_plan;
  std::shared_ptr<CreatePreparedPlan> create_prepared_plan;
};

TEST_F(CreatePreparedPlanTest, OperatorName) { EXPECT_EQ(create_prepared_plan->name(), "CreatePreparedPlan"); }

TEST_F(CreatePreparedPlanTest, OperatorDescription) {
  EXPECT_EQ(replace_addresses(create_prepared_plan->description(DescriptionMode::SingleLine)),
            R"(CreatePreparedPlan 'prepared_plan_a' {
ParameterIDs: []
[0] [DummyTable] @ 0x00000000
})");
}

TEST_F(CreatePreparedPlanTest, DeepCopy) {
  const auto copy = std::dynamic_pointer_cast<CreatePreparedPlan>(create_prepared_plan->deep_copy());
  EXPECT_EQ(copy->get_output(), nullptr);
}

TEST_F(CreatePreparedPlanTest, Execute) {
  EXPECT_FALSE(Hyrise::get().storage_manager.has_prepared_plan("prepared_plan_a"));
  create_prepared_plan->execute();
  const auto& sm = Hyrise::get().storage_manager;
  EXPECT_TRUE(sm.has_prepared_plan("prepared_plan_a"));
  EXPECT_EQ(sm.get_prepared_plan("prepared_plan_a"), prepared_plan);

  const auto copy = create_prepared_plan->deep_copy();
  EXPECT_ANY_THROW(copy->execute());
}

}  // namespace opossum
