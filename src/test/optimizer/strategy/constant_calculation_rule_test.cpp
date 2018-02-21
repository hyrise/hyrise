#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "../../base_test.hpp"
#include "gtest/gtest.h"

#include "abstract_expression.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/projection_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "optimizer/strategy/constant_calculation_rule.hpp"
#include "optimizer/strategy/strategy_base_test.hpp"
#include "sql/sql_pipeline.hpp"
#include "storage/storage_manager.hpp"

namespace {
std::shared_ptr<opossum::AbstractLQPNode> compile_query(const std::string& query) {
  return opossum::SQLPipeline{query, opossum::UseMvcc::No}.get_unoptimized_logical_plans().at(0);
}
}  // namespace

namespace opossum {

class ConstantCalculationRuleTest : public StrategyBaseTest {
 protected:
  void SetUp() override {
    StorageManager::get().add_table("table_a", load_table("src/test/tables/int_float.tbl", Chunk::MAX_SIZE));
    _rule = std::make_shared<ConstantCalculationRule>();
  }

  std::shared_ptr<ConstantCalculationRule> _rule;
};

TEST_F(ConstantCalculationRuleTest, ResolveExpressionTest) {
  const auto query = "SELECT * FROM table_a WHERE a = 1232 + 1 + 1";
  const auto result_node = compile_query(query);

  const auto resolved = StrategyBaseTest::apply_rule(_rule, result_node);

  EXPECT_EQ(resolved->type(), LQPNodeType::Projection);
  EXPECT_EQ(resolved->left_child()->type(), LQPNodeType::Projection);
  EXPECT_FALSE(resolved->right_child());

  ASSERT_EQ(resolved->left_child()->left_child()->type(), LQPNodeType::Predicate);
  const auto predicate_node = std::dynamic_pointer_cast<PredicateNode>(resolved->left_child()->left_child());
  EXPECT_FALSE(predicate_node->right_child());
  EXPECT_EQ(predicate_node->predicate_condition(), PredicateCondition::Equals);

  ASSERT_EQ(predicate_node->left_child()->type(), LQPNodeType::Projection);
  const auto projection_node = std::dynamic_pointer_cast<ProjectionNode>(predicate_node->left_child());
  EXPECT_EQ(projection_node->column_expressions().size(), 2u);

  const auto original_node = projection_node->left_child();

  ASSERT_TRUE(is_variant(predicate_node->value()));
  EXPECT_EQ(predicate_node->column_reference(), LQPColumnReference(original_node, ColumnID{0}));
  EXPECT_EQ(boost::get<AllTypeVariant>(predicate_node->value()), AllTypeVariant{1234});
}

}  // namespace opossum
