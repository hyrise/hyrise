#include <memory>
#include <string>
#include <vector>

#include "../../base_test.hpp"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include "optimizer/abstract_syntax_tree/predicate_node.hpp"
#include "optimizer/abstract_syntax_tree/projection_node.hpp"
#include "optimizer/abstract_syntax_tree/sort_node.hpp"
#include "optimizer/abstract_syntax_tree/stored_table_node.hpp"
#include "optimizer/strategy/predicate_reordering_rule.hpp"

#include "utils/assert.hpp"

namespace opossum {

class TableStatisticsMock : public TableStatistics {
 public:
  // we don't need a shared_ptr<Table> for this mock, so just set a nullptr
  TableStatisticsMock() : TableStatistics(std::make_shared<Table>()) { _row_count = 0; }

  explicit TableStatisticsMock(float row_count) : TableStatistics(std::make_shared<Table>()) { _row_count = row_count; }

  std::shared_ptr<TableStatistics> predicate_statistics(const std::string &column_name, const ScanType scan_type,
                                                        const AllParameterVariant &value,
                                                        const optional<AllTypeVariant> &value2) override {
    if (column_name == "c1") {
      return std::make_shared<TableStatisticsMock>(500);
    }
    if (column_name == "c2") {
      return std::make_shared<TableStatisticsMock>(200);
    }
    if (column_name == "c3") {
      return std::make_shared<TableStatisticsMock>(950);
    }

    Fail("Tried to access TableStatisticsMock with unexpected column");
    return nullptr;
  }
};

class PredicateReorderingTest : public BaseTest {
 protected:
  void SetUp() override {}
};

TEST_F(PredicateReorderingTest, SimpleReorderingTest) {
  PredicateReorderingRule rule;

  auto stored_table_node = std::make_shared<StoredTableNode>("a");

  stored_table_node->set_statistics(std::make_shared<TableStatisticsMock>());

  auto predicate_node_0 = std::make_shared<PredicateNode>("c1", ScanType::OpGreaterThan, 10);
  predicate_node_0->set_left_child(stored_table_node);

  auto predicate_node_1 = std::make_shared<PredicateNode>("c2", ScanType::OpGreaterThan, 50);
  predicate_node_1->set_left_child(predicate_node_0);

  predicate_node_1->get_or_create_statistics();

  auto reordered = rule.apply_rule(predicate_node_1);

  EXPECT_EQ(reordered, predicate_node_0);
  EXPECT_EQ(reordered->left_child(), predicate_node_1);
  EXPECT_EQ(reordered->left_child()->left_child(), stored_table_node);
}

TEST_F(PredicateReorderingTest, MoreComplexReorderingTest) {
  PredicateReorderingRule rule;

  auto stored_table_node = std::make_shared<StoredTableNode>("a");

  stored_table_node->set_statistics(std::make_shared<TableStatisticsMock>());

  auto predicate_node_0 = std::make_shared<PredicateNode>("c1", ScanType::OpGreaterThan, 10);
  predicate_node_0->set_left_child(stored_table_node);

  auto predicate_node_1 = std::make_shared<PredicateNode>("c2", ScanType::OpGreaterThan, 50);
  predicate_node_1->set_left_child(predicate_node_0);

  auto predicate_node_2 = std::make_shared<PredicateNode>("c3", ScanType::OpGreaterThan, 90);
  predicate_node_2->set_left_child(predicate_node_1);

  predicate_node_2->get_or_create_statistics();

  auto reordered = rule.apply_rule(predicate_node_2);

  EXPECT_EQ(reordered, predicate_node_2);
  EXPECT_EQ(reordered->left_child(), predicate_node_0);
  EXPECT_EQ(reordered->left_child()->left_child(), predicate_node_1);
  EXPECT_EQ(reordered->left_child()->left_child()->left_child(), stored_table_node);
}

TEST_F(PredicateReorderingTest, ComplexReorderingTest) {
  PredicateReorderingRule rule;

  auto stored_table_node = std::make_shared<StoredTableNode>("a");

  stored_table_node->set_statistics(std::make_shared<TableStatisticsMock>());

  auto predicate_node_0 = std::make_shared<PredicateNode>("c1", ScanType::OpGreaterThan, 10);
  predicate_node_0->set_left_child(stored_table_node);

  auto predicate_node_1 = std::make_shared<PredicateNode>("c2", ScanType::OpGreaterThan, 50);
  predicate_node_1->set_left_child(predicate_node_0);

  auto predicate_node_2 = std::make_shared<PredicateNode>("c3", ScanType::OpGreaterThan, 90);
  predicate_node_2->set_left_child(predicate_node_1);

  std::vector<std::string> columns({"c1", "c2"});
  auto projection_node = std::make_shared<ProjectionNode>(columns);
  projection_node->set_left_child(predicate_node_2);

  auto predicate_node_3 = std::make_shared<PredicateNode>("c1", ScanType::OpGreaterThan, 10);
  predicate_node_3->set_left_child(projection_node);

  auto predicate_node_4 = std::make_shared<PredicateNode>("c2", ScanType::OpGreaterThan, 50);
  predicate_node_4->set_left_child(predicate_node_3);

  predicate_node_4->get_or_create_statistics();

  auto reordered = rule.apply_rule(predicate_node_4);

  EXPECT_EQ(reordered, predicate_node_3);
  EXPECT_EQ(reordered->left_child(), predicate_node_4);
  EXPECT_EQ(reordered->left_child()->left_child(), projection_node);
  EXPECT_EQ(reordered->left_child()->left_child()->left_child(), predicate_node_2);
  EXPECT_EQ(reordered->left_child()->left_child()->left_child()->left_child(), predicate_node_0);
  EXPECT_EQ(reordered->left_child()->left_child()->left_child()->left_child()->left_child(), predicate_node_1);
  EXPECT_EQ(reordered->left_child()->left_child()->left_child()->left_child()->left_child()->left_child(), stored_table_node);
}

TEST_F(PredicateReorderingTest, TwoReorderings) {
  PredicateReorderingRule rule;

  auto stored_table_node = std::make_shared<StoredTableNode>("a");

  stored_table_node->set_statistics(std::make_shared<TableStatisticsMock>());

  auto predicate_node_0 = std::make_shared<PredicateNode>("c1", ScanType::OpGreaterThan, 10);
  predicate_node_0->set_left_child(stored_table_node);

  auto predicate_node_1 = std::make_shared<PredicateNode>("c2", ScanType::OpGreaterThan, 50);
  predicate_node_1->set_left_child(predicate_node_0);

  auto sort_node = std::make_shared<SortNode>("c1", true);
  sort_node->set_left_child(predicate_node_1);

  auto predicate_node_2 = std::make_shared<PredicateNode>("c3", ScanType::OpGreaterThan, 90);
  predicate_node_2->set_left_child(sort_node);

  auto predicate_node_3 = std::make_shared<PredicateNode>("c2", ScanType::OpGreaterThan, 50);
  predicate_node_3->set_left_child(predicate_node_2);

  std::vector<std::string> columns({"c1", "c2"});
  auto projection_node = std::make_shared<ProjectionNode>(columns);
  projection_node->set_left_child(predicate_node_3);


  projection_node->get_or_create_statistics();

  auto reordered = rule.apply_rule(projection_node);

  reordered->print();

  EXPECT_EQ(reordered, projection_node);
  EXPECT_EQ(reordered->left_child(), predicate_node_2);
  EXPECT_EQ(reordered->left_child()->left_child(), predicate_node_3);
  EXPECT_EQ(reordered->left_child()->left_child()->left_child(), sort_node);
  EXPECT_EQ(reordered->left_child()->left_child()->left_child()->left_child(), predicate_node_0);
  EXPECT_EQ(reordered->left_child()->left_child()->left_child()->left_child()->left_child(), predicate_node_1);
  EXPECT_EQ(reordered->left_child()->left_child()->left_child()->left_child()->left_child()->left_child(), stored_table_node);
}

}  // namespace opossum
