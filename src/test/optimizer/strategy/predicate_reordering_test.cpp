#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "../../base_test.hpp"
#include "gtest/gtest.h"

#include "optimizer/abstract_syntax_tree/predicate_node.hpp"
#include "optimizer/abstract_syntax_tree/projection_node.hpp"
#include "optimizer/abstract_syntax_tree/sort_node.hpp"
#include "optimizer/abstract_syntax_tree/stored_table_node.hpp"
#include "optimizer/strategy/predicate_reordering_rule.hpp"
#include "optimizer/table_statistics.hpp"
#include "storage/storage_manager.hpp"

#include "utils/assert.hpp"

namespace opossum {

class TableStatisticsMock : public TableStatistics {
 public:
  // we don't need a shared_ptr<Table> for this mock, so just set a nullptr
  TableStatisticsMock() : TableStatistics(std::make_shared<Table>()) { _row_count = 0; }

  explicit TableStatisticsMock(float row_count) : TableStatistics(std::make_shared<Table>()) { _row_count = row_count; }

  std::shared_ptr<TableStatistics> predicate_statistics(const ColumnID column_id, const ScanType scan_type,
                                                        const AllParameterVariant& value,
                                                        const optional<AllTypeVariant>& value2) override {
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

class PredicateReorderingTest : public BaseTest {
 protected:
  void SetUp() override { StorageManager::get().add_table("a", load_table("src/test/tables/int_float.tbl", 0)); }
};

TEST_F(PredicateReorderingTest, SimpleReorderingTest) {
  PredicateReorderingRule rule;

  auto stored_table_node = std::make_shared<StoredTableNode>("a");

  auto statistics_mock = std::make_shared<TableStatisticsMock>();
  stored_table_node->set_statistics(statistics_mock);

  auto predicate_node_0 = std::make_shared<PredicateNode>(ColumnID{0}, ScanType::OpGreaterThan, 10);
  predicate_node_0->set_left_child(stored_table_node);

  auto predicate_node_1 = std::make_shared<PredicateNode>(ColumnID{1}, ScanType::OpGreaterThan, 50);
  predicate_node_1->set_left_child(predicate_node_0);

  predicate_node_1->get_statistics();

  auto reordered = rule.apply_to(predicate_node_1);

  EXPECT_EQ(reordered, predicate_node_0);
  EXPECT_EQ(reordered->left_child(), predicate_node_1);
  EXPECT_EQ(reordered->left_child()->left_child(), stored_table_node);
}

TEST_F(PredicateReorderingTest, MoreComplexReorderingTest) {
  PredicateReorderingRule rule;

  auto stored_table_node = std::make_shared<StoredTableNode>("a");

  auto statistics_mock = std::make_shared<TableStatisticsMock>();
  stored_table_node->set_statistics(statistics_mock);

  auto predicate_node_0 = std::make_shared<PredicateNode>(ColumnID{0}, ScanType::OpGreaterThan, 10);
  predicate_node_0->set_left_child(stored_table_node);

  auto predicate_node_1 = std::make_shared<PredicateNode>(ColumnID{1}, ScanType::OpGreaterThan, 50);
  predicate_node_1->set_left_child(predicate_node_0);

  auto predicate_node_2 = std::make_shared<PredicateNode>(ColumnID{2}, ScanType::OpGreaterThan, 90);
  predicate_node_2->set_left_child(predicate_node_1);

  predicate_node_2->get_statistics();

  auto reordered = rule.apply_to(predicate_node_2);

  EXPECT_EQ(reordered, predicate_node_2);
  EXPECT_EQ(reordered->left_child(), predicate_node_0);
  EXPECT_EQ(reordered->left_child()->left_child(), predicate_node_1);
  EXPECT_EQ(reordered->left_child()->left_child()->left_child(), stored_table_node);
}

TEST_F(PredicateReorderingTest, ComplexReorderingTest) {
  PredicateReorderingRule rule;

  auto stored_table_node = std::make_shared<StoredTableNode>("a");

  auto statistics_mock = std::make_shared<TableStatisticsMock>();
  stored_table_node->set_statistics(statistics_mock);

  auto predicate_node_0 = std::make_shared<PredicateNode>(ColumnID{0}, ScanType::OpGreaterThan, 10);
  predicate_node_0->set_left_child(stored_table_node);

  auto predicate_node_1 = std::make_shared<PredicateNode>(ColumnID{1}, ScanType::OpGreaterThan, 50);
  predicate_node_1->set_left_child(predicate_node_0);

  auto predicate_node_2 = std::make_shared<PredicateNode>(ColumnID{2}, ScanType::OpGreaterThan, 90);
  predicate_node_2->set_left_child(predicate_node_1);

  const std::vector<ColumnID> column_ids = {ColumnID{0}, ColumnID{1}};
  const auto& expressions = Expression::create_columns(column_ids);
  const auto projection_node = std::make_shared<ProjectionNode>(expressions);
  projection_node->set_left_child(predicate_node_2);

  auto predicate_node_3 = std::make_shared<PredicateNode>(ColumnID{0}, ScanType::OpGreaterThan, 10);
  predicate_node_3->set_left_child(projection_node);

  auto predicate_node_4 = std::make_shared<PredicateNode>(ColumnID{1}, ScanType::OpGreaterThan, 50);
  predicate_node_4->set_left_child(predicate_node_3);

  predicate_node_4->get_statistics();

  auto reordered = rule.apply_to(predicate_node_4);

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
  PredicateReorderingRule rule;

  auto stored_table_node = std::make_shared<StoredTableNode>("a");

  auto statistics_mock = std::make_shared<TableStatisticsMock>();
  stored_table_node->set_statistics(statistics_mock);

  auto predicate_node_0 = std::make_shared<PredicateNode>(ColumnID{0}, ScanType::OpGreaterThan, 10);
  predicate_node_0->set_left_child(stored_table_node);

  auto predicate_node_1 = std::make_shared<PredicateNode>(ColumnID{1}, ScanType::OpGreaterThan, 50);
  predicate_node_1->set_left_child(predicate_node_0);

  auto sort_node = std::make_shared<SortNode>(std::vector<OrderByDefinition>{{ColumnID{0}, OrderByMode::Ascending}});
  sort_node->set_left_child(predicate_node_1);

  auto predicate_node_2 = std::make_shared<PredicateNode>(ColumnID{2}, ScanType::OpGreaterThan, 90);
  predicate_node_2->set_left_child(sort_node);

  auto predicate_node_3 = std::make_shared<PredicateNode>(ColumnID{1}, ScanType::OpGreaterThan, 50);
  predicate_node_3->set_left_child(predicate_node_2);

  const std::vector<ColumnID> column_ids = {ColumnID{0}, ColumnID{1}};
  const auto& expressions = Expression::create_columns(column_ids);
  const auto projection_node = std::make_shared<ProjectionNode>(expressions);
  projection_node->set_left_child(predicate_node_3);

  projection_node->get_statistics();

  auto reordered = rule.apply_to(projection_node);

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

  PredicateReorderingRule rule;

  auto stored_table_node = std::make_shared<StoredTableNode>("table_a");

  // Setup first AST
  auto predicate_node_0 = std::make_shared<PredicateNode>(ColumnID{0}, ScanType::OpLessThan, 20);
  predicate_node_0->set_left_child(stored_table_node);

  auto predicate_node_1 = std::make_shared<PredicateNode>(ColumnID{1}, ScanType::OpGreaterThan, 458.5);
  predicate_node_1->set_left_child(predicate_node_0);

  predicate_node_1->get_statistics();

  auto reordered = rule.apply_to(predicate_node_1);

  // Setup second AST
  auto predicate_node_2 = std::make_shared<PredicateNode>(ColumnID{1}, ScanType::OpGreaterThan, 458.5);
  predicate_node_2->set_left_child(stored_table_node);

  auto predicate_node_3 = std::make_shared<PredicateNode>(ColumnID{0}, ScanType::OpLessThan, 20);
  predicate_node_3->set_left_child(predicate_node_2);

  predicate_node_3->get_statistics();

  auto reordered_1 = rule.apply_to(predicate_node_3);

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

}  // namespace opossum
