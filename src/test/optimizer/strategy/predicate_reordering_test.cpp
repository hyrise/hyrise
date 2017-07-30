#include <memory>
#include <string>
#include <vector>

#include "../../base_test.hpp"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include "optimizer/abstract_syntax_tree/predicate_node.hpp"
#include "optimizer/abstract_syntax_tree/projection_node.hpp"
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

  auto t_n = std::make_shared<StoredTableNode>("a");

  t_n->set_statistics(std::make_shared<TableStatisticsMock>());

  auto ts_n_0 = std::make_shared<PredicateNode>("c1", ScanType::OpGreaterThan, 10);
  ts_n_0->set_left_child(t_n);

  auto ts_n_1 = std::make_shared<PredicateNode>("c2", ScanType::OpGreaterThan, 50);
  ts_n_1->set_left_child(ts_n_0);

  ts_n_1->get_or_create_statistics();

  auto reordered = rule.apply_rule(ts_n_1);

  ASSERT_EQ(reordered, ts_n_0);
  ASSERT_EQ(reordered->left_child(), ts_n_1);
  ASSERT_EQ(reordered->left_child()->left_child(), t_n);
}

TEST_F(PredicateReorderingTest, MoreComplexReorderingTest) {
  PredicateReorderingRule rule;

  auto t_n = std::make_shared<StoredTableNode>("a");

  t_n->set_statistics(std::make_shared<TableStatisticsMock>());

  auto ts_n_0 = std::make_shared<PredicateNode>("c1", ScanType::OpGreaterThan, 10);
  ts_n_0->set_left_child(t_n);

  auto ts_n_1 = std::make_shared<PredicateNode>("c2", ScanType::OpGreaterThan, 50);
  ts_n_1->set_left_child(ts_n_0);

  auto ts_n_2 = std::make_shared<PredicateNode>("c3", ScanType::OpGreaterThan, 90);
  ts_n_2->set_left_child(ts_n_1);

  ts_n_2->get_or_create_statistics();

  auto reordered = rule.apply_rule(ts_n_2);

  ASSERT_EQ(reordered, ts_n_2);
  ASSERT_EQ(reordered->left_child(), ts_n_0);
  ASSERT_EQ(reordered->left_child()->left_child(), ts_n_1);
  ASSERT_EQ(reordered->left_child()->left_child()->left_child(), t_n);
}

TEST_F(PredicateReorderingTest, ComplexReorderingTest) {
  PredicateReorderingRule rule;

  auto t_n = std::make_shared<StoredTableNode>("a");

  t_n->set_statistics(std::make_shared<TableStatisticsMock>());

  auto ts_n_0 = std::make_shared<PredicateNode>("c1", ScanType::OpGreaterThan, 10);
  ts_n_0->set_left_child(t_n);

  auto ts_n_1 = std::make_shared<PredicateNode>("c2", ScanType::OpGreaterThan, 50);
  ts_n_1->set_left_child(ts_n_0);

  auto ts_n_2 = std::make_shared<PredicateNode>("c3", ScanType::OpGreaterThan, 90);
  ts_n_2->set_left_child(ts_n_1);

  std::vector<std::string> columns({"c1", "c2"});
  auto p_n = std::make_shared<ProjectionNode>(columns);
  p_n->set_left_child(ts_n_2);

  auto ts_n_3 = std::make_shared<PredicateNode>("c1", ScanType::OpGreaterThan, 10);
  ts_n_3->set_left_child(p_n);

  auto ts_n_4 = std::make_shared<PredicateNode>("c2", ScanType::OpGreaterThan, 50);
  ts_n_4->set_left_child(ts_n_3);

  ts_n_4->get_or_create_statistics();

  auto reordered = rule.apply_rule(ts_n_4);

  ASSERT_EQ(reordered, ts_n_3);
  ASSERT_EQ(reordered->left_child(), ts_n_4);
  ASSERT_EQ(reordered->left_child()->left_child(), p_n);
  ASSERT_EQ(reordered->left_child()->left_child()->left_child(), ts_n_2);
  ASSERT_EQ(reordered->left_child()->left_child()->left_child()->left_child(), ts_n_0);
  ASSERT_EQ(reordered->left_child()->left_child()->left_child()->left_child()->left_child(), ts_n_1);
  ASSERT_EQ(reordered->left_child()->left_child()->left_child()->left_child()->left_child()->left_child(), t_n);
}

}  // namespace opossum
