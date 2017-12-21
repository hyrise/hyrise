#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "../../base_test.hpp"
#include "gtest/gtest.h"

#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/projection_node.hpp"
#include "logical_query_plan/sort_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "logical_query_plan/union_node.hpp"
#include "optimizer/column_statistics.hpp"
#include "optimizer/expression.hpp"
#include "optimizer/strategy/predicate_reordering_rule.hpp"
#include "optimizer/strategy/strategy_base_test.hpp"
#include "optimizer/table_statistics.hpp"
#include "storage/storage_manager.hpp"

#include "utils/assert.hpp"

#include "logical_query_plan/mock_node.hpp"

namespace opossum {

class TableStatisticsMock : public TableStatistics {
 public:
  // we don't need a shared_ptr<Table> for this mock, so just set a nullptr
  TableStatisticsMock() : TableStatistics(std::make_shared<Table>()) { _row_count = 0; }

  explicit TableStatisticsMock(float row_count) : TableStatistics(std::make_shared<Table>()) { _row_count = row_count; }

  std::shared_ptr<TableStatistics> predicate_statistics(const ColumnID column_id, const ScanType scan_type,
                                                        const AllParameterVariant& value,
                                                        const std::optional<AllTypeVariant>& value2) override {
    if (column_id == ColumnID{0}) {
      return std::make_shared<TableStatisticsMock>(500);
    }
    if (column_id == ColumnID{1}) {
      return std::make_shared<TableStatisticsMock>(200);
    }
    if (column_id == ColumnID{2}) {
      return std::make_shared<TableStatisticsMock>(950);
    }

    Fail("Tried to access TableStatisticsMock with unexpected column");
    return nullptr;
  }
};

class PredicateReorderingTest : public StrategyBaseTest {
 protected:
  void SetUp() override {
    StorageManager::get().add_table("a", load_table("src/test/tables/int_float.tbl", Chunk::MAX_SIZE));
    _rule = std::make_shared<PredicateReorderingRule>();
  }

  std::shared_ptr<PredicateReorderingRule> _rule;
};

TEST_F(PredicateReorderingTest, SimpleReorderingTest) {
  auto stored_table_node = std::make_shared<StoredTableNode>("a");

  auto statistics_mock = std::make_shared<TableStatisticsMock>();
  stored_table_node->set_statistics(statistics_mock);

  auto predicate_node_0 = std::make_shared<PredicateNode>(ColumnID{0}, ScanType::GreaterThan, 10);
  predicate_node_0->set_left_child(stored_table_node);

  auto predicate_node_1 = std::make_shared<PredicateNode>(ColumnID{1}, ScanType::GreaterThan, 50);
  predicate_node_1->set_left_child(predicate_node_0);

  predicate_node_1->get_statistics();

  auto reordered = StrategyBaseTest::apply_rule(_rule, predicate_node_1);

  EXPECT_EQ(reordered, predicate_node_0);
  EXPECT_EQ(reordered->left_child(), predicate_node_1);
  EXPECT_EQ(reordered->left_child()->left_child(), stored_table_node);
}

TEST_F(PredicateReorderingTest, MoreComplexReorderingTest) {
  auto stored_table_node = std::make_shared<StoredTableNode>("a");

  auto statistics_mock = std::make_shared<TableStatisticsMock>();
  stored_table_node->set_statistics(statistics_mock);

  auto predicate_node_0 = std::make_shared<PredicateNode>(ColumnID{0}, ScanType::GreaterThan, 10);
  predicate_node_0->set_left_child(stored_table_node);

  auto predicate_node_1 = std::make_shared<PredicateNode>(ColumnID{1}, ScanType::GreaterThan, 50);
  predicate_node_1->set_left_child(predicate_node_0);

  auto predicate_node_2 = std::make_shared<PredicateNode>(ColumnID{2}, ScanType::GreaterThan, 90);
  predicate_node_2->set_left_child(predicate_node_1);

  predicate_node_2->get_statistics();

  auto reordered = StrategyBaseTest::apply_rule(_rule, predicate_node_2);

  EXPECT_EQ(reordered, predicate_node_2);
  EXPECT_EQ(reordered->left_child(), predicate_node_0);
  EXPECT_EQ(reordered->left_child()->left_child(), predicate_node_1);
  EXPECT_EQ(reordered->left_child()->left_child()->left_child(), stored_table_node);
}

TEST_F(PredicateReorderingTest, ComplexReorderingTest) {
  auto stored_table_node = std::make_shared<StoredTableNode>("a");

  auto statistics_mock = std::make_shared<TableStatisticsMock>();
  stored_table_node->set_statistics(statistics_mock);

  auto predicate_node_0 = std::make_shared<PredicateNode>(ColumnID{0}, ScanType::GreaterThan, 10);
  predicate_node_0->set_left_child(stored_table_node);

  auto predicate_node_1 = std::make_shared<PredicateNode>(ColumnID{1}, ScanType::GreaterThan, 50);
  predicate_node_1->set_left_child(predicate_node_0);

  auto predicate_node_2 = std::make_shared<PredicateNode>(ColumnID{2}, ScanType::GreaterThan, 90);
  predicate_node_2->set_left_child(predicate_node_1);

  const std::vector<ColumnID> column_ids = {ColumnID{0}, ColumnID{1}};
  const auto& expressions = Expression::create_columns(column_ids);
  const auto projection_node = std::make_shared<ProjectionNode>(expressions);
  projection_node->set_left_child(predicate_node_2);

  auto predicate_node_3 = std::make_shared<PredicateNode>(ColumnID{0}, ScanType::GreaterThan, 10);
  predicate_node_3->set_left_child(projection_node);

  auto predicate_node_4 = std::make_shared<PredicateNode>(ColumnID{1}, ScanType::GreaterThan, 50);
  predicate_node_4->set_left_child(predicate_node_3);

  predicate_node_4->get_statistics();

  auto reordered = StrategyBaseTest::apply_rule(_rule, predicate_node_4);

  EXPECT_EQ(reordered, predicate_node_3);
  EXPECT_EQ(reordered->left_child(), predicate_node_4);
  EXPECT_EQ(reordered->left_child()->left_child(), projection_node);
  EXPECT_EQ(reordered->left_child()->left_child()->left_child(), predicate_node_2);
  EXPECT_EQ(reordered->left_child()->left_child()->left_child()->left_child(), predicate_node_0);
  EXPECT_EQ(reordered->left_child()->left_child()->left_child()->left_child()->left_child(), predicate_node_1);
  EXPECT_EQ(reordered->left_child()->left_child()->left_child()->left_child()->left_child()->left_child(),
            stored_table_node);
}

TEST_F(PredicateReorderingTest, TwoReorderings) {
  auto stored_table_node = std::make_shared<StoredTableNode>("a");

  auto statistics_mock = std::make_shared<TableStatisticsMock>();
  stored_table_node->set_statistics(statistics_mock);

  auto predicate_node_0 = std::make_shared<PredicateNode>(ColumnID{0}, ScanType::GreaterThan, 10);
  predicate_node_0->set_left_child(stored_table_node);

  auto predicate_node_1 = std::make_shared<PredicateNode>(ColumnID{1}, ScanType::GreaterThan, 50);
  predicate_node_1->set_left_child(predicate_node_0);

  auto sort_node = std::make_shared<SortNode>(std::vector<OrderByDefinition>{{ColumnID{0}, OrderByMode::Ascending}});
  sort_node->set_left_child(predicate_node_1);

  auto predicate_node_2 = std::make_shared<PredicateNode>(ColumnID{2}, ScanType::GreaterThan, 90);
  predicate_node_2->set_left_child(sort_node);

  auto predicate_node_3 = std::make_shared<PredicateNode>(ColumnID{1}, ScanType::GreaterThan, 50);
  predicate_node_3->set_left_child(predicate_node_2);

  const std::vector<ColumnID> column_ids = {ColumnID{0}, ColumnID{1}};
  const auto& expressions = Expression::create_columns(column_ids);
  const auto projection_node = std::make_shared<ProjectionNode>(expressions);
  projection_node->set_left_child(predicate_node_3);

  projection_node->get_statistics();

  auto reordered = StrategyBaseTest::apply_rule(_rule, projection_node);

  EXPECT_EQ(reordered, projection_node);
  EXPECT_EQ(reordered->left_child(), predicate_node_2);
  EXPECT_EQ(reordered->left_child()->left_child(), predicate_node_3);
  EXPECT_EQ(reordered->left_child()->left_child()->left_child(), sort_node);
  EXPECT_EQ(reordered->left_child()->left_child()->left_child()->left_child(), predicate_node_0);
  EXPECT_EQ(reordered->left_child()->left_child()->left_child()->left_child()->left_child(), predicate_node_1);
  EXPECT_EQ(reordered->left_child()->left_child()->left_child()->left_child()->left_child()->left_child(),
            stored_table_node);
}

TEST_F(PredicateReorderingTest, SameOrderingForStoredTable) {
  std::shared_ptr<Table> table_a = load_table("src/test/tables/int_float4.tbl", 2);
  StorageManager::get().add_table("table_a", std::move(table_a));

  auto stored_table_node = std::make_shared<StoredTableNode>("table_a");

  // Setup first LQP
  // predicate_node_1 -> predicate_node_0 -> stored_table_node
  auto predicate_node_0 = std::make_shared<PredicateNode>(ColumnID{0}, ScanType::LessThan, 20);
  predicate_node_0->set_left_child(stored_table_node);

  auto predicate_node_1 = std::make_shared<PredicateNode>(ColumnID{1}, ScanType::GreaterThan, 458.5);
  predicate_node_1->set_left_child(predicate_node_0);

  predicate_node_1->get_statistics();

  auto reordered = StrategyBaseTest::apply_rule(_rule, predicate_node_1);

  // Setup second LQP
  // predicate_node_3 -> predicate_node_2 -> stored_table_node
  auto predicate_node_2 = std::make_shared<PredicateNode>(ColumnID{1}, ScanType::GreaterThan, 458.5);
  predicate_node_2->set_left_child(stored_table_node);

  auto predicate_node_3 = std::make_shared<PredicateNode>(ColumnID{0}, ScanType::LessThan, 20);
  predicate_node_3->set_left_child(predicate_node_2);

  predicate_node_3->get_statistics();

  auto reordered_1 = StrategyBaseTest::apply_rule(_rule, predicate_node_3);

  // Compare members in PredicateNodes to make sure both are ordered the same way.
  auto first_predicate_0 = std::dynamic_pointer_cast<PredicateNode>(reordered);
  auto first_predicate_1 = std::dynamic_pointer_cast<PredicateNode>(reordered_1);
  EXPECT_EQ(first_predicate_0->column_id(), first_predicate_1->column_id());
  EXPECT_EQ(first_predicate_0->scan_type(), first_predicate_1->scan_type());
  EXPECT_EQ(first_predicate_0->value(), first_predicate_1->value());

  auto second_predicate_0 = std::dynamic_pointer_cast<PredicateNode>(first_predicate_0->left_child());
  auto second_predicate_1 = std::dynamic_pointer_cast<PredicateNode>(first_predicate_1->left_child());
  EXPECT_EQ(second_predicate_0->column_id(), second_predicate_1->column_id());
  EXPECT_EQ(second_predicate_0->scan_type(), second_predicate_1->scan_type());
  EXPECT_EQ(second_predicate_0->value(), second_predicate_1->value());
}

TEST_F(PredicateReorderingTest, PredicatesAsRightChild) {
  /**
   * Check that Reordering predicates works if a predicate chain is both on the left and right side of a node.
   * This is particularly interesting because the PredicateReorderingRule needs to re-attach the ordered chain of
   * predicates to the parent (the cross node in this case). This test checks whether the attachment happens as the
   * correct child.
   *
   *             _______Cross________
   *            /                    \
   *  Predicate_0(a > 80)     Predicate_2(b > 90)
   *           |                     |
   *  Predicate_1(a > 60)     Predicate_3(a > 50)
   *           |                     |
   *        Table_0           Predicate_4(c > 30)
   *                                 |
   *                               Table_1
   */

  /**
   * The mocked table has one column of int32_ts with the value range 0..100
   */
  auto column_statistics = std::make_shared<ColumnStatistics<int32_t>>(ColumnID{0}, 100.0f, 0.0f, 100.0f);
  auto table_statistics =
      std::make_shared<TableStatistics>(100, std::vector<std::shared_ptr<BaseColumnStatistics>>{column_statistics});

  auto cross_node = std::make_shared<JoinNode>(JoinMode::Cross);
  auto predicate_0 = std::make_shared<PredicateNode>(ColumnID{0}, ScanType::GreaterThan, 80);
  auto predicate_1 = std::make_shared<PredicateNode>(ColumnID{0}, ScanType::GreaterThan, 60);
  auto predicate_2 = std::make_shared<PredicateNode>(ColumnID{0}, ScanType::GreaterThan, 90);
  auto predicate_3 = std::make_shared<PredicateNode>(ColumnID{0}, ScanType::GreaterThan, 50);
  auto predicate_4 = std::make_shared<PredicateNode>(ColumnID{0}, ScanType::GreaterThan, 30);
  auto table_0 = std::make_shared<MockNode>(table_statistics);
  auto table_1 = std::make_shared<MockNode>(table_statistics);

  predicate_1->set_left_child(table_0);
  predicate_0->set_left_child(predicate_1);
  predicate_4->set_left_child(table_1);
  predicate_3->set_left_child(predicate_4);
  predicate_2->set_left_child(predicate_3);
  cross_node->set_left_child(predicate_0);
  cross_node->set_right_child(predicate_2);

  const auto reordered = StrategyBaseTest::apply_rule(_rule, cross_node);

  EXPECT_EQ(reordered, cross_node);
  EXPECT_EQ(reordered->left_child(), predicate_1);
  EXPECT_EQ(reordered->left_child()->left_child(), predicate_0);
  EXPECT_EQ(reordered->left_child()->left_child()->left_child(), table_0);
  EXPECT_EQ(reordered->right_child(), predicate_4);
  EXPECT_EQ(reordered->right_child()->left_child(), predicate_3);
  EXPECT_EQ(reordered->right_child()->left_child()->left_child(), predicate_2);
}

TEST_F(PredicateReorderingTest, PredicatesWithMultipleParents) {
  /**
   * If a PredicateNode has multiple parents, it should not be considered for reordering
   */
  /**
   *      _____Union___
   *    /             /
   * Predicate_a     /
   *    \           /
   *     Predicate_b
   *         |
   *       Table
   *
   * predicate_a should come before predicate_b - but since Predicate_b has two parents, it can't be reordered
   */

  /**
   * The mocked table has one column of int32_ts with the value range 0..100
   */
  auto column_statistics = std::make_shared<ColumnStatistics<int32_t>>(ColumnID{0}, 100.0f, 0.0f, 100.0f);
  auto table_statistics =
      std::make_shared<TableStatistics>(100, std::vector<std::shared_ptr<BaseColumnStatistics>>{column_statistics});

  auto union_node = std::make_shared<UnionNode>(UnionMode::Positions);
  auto predicate_a_node = std::make_shared<PredicateNode>(ColumnID{0}, ScanType::GreaterThan, 90);
  auto predicate_b_node = std::make_shared<PredicateNode>(ColumnID{0}, ScanType::GreaterThan, 10);
  auto table_node = std::make_shared<MockNode>(table_statistics);

  union_node->set_left_child(predicate_a_node);
  union_node->set_right_child(predicate_b_node);
  predicate_a_node->set_left_child(predicate_b_node);
  predicate_b_node->set_left_child(table_node);

  const auto reordered = StrategyBaseTest::apply_rule(_rule, union_node);

  EXPECT_EQ(reordered, union_node);
  EXPECT_EQ(reordered->left_child(), predicate_a_node);
  EXPECT_EQ(reordered->right_child(), predicate_b_node);
  EXPECT_EQ(predicate_a_node->left_child(), predicate_b_node);
  EXPECT_EQ(predicate_b_node->left_child(), table_node);
}

}  // namespace opossum
