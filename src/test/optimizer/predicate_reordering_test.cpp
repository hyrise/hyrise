#include <memory>
#include <string>
#include <vector>

#include "../base_test.hpp"
#include "gtest/gtest.h"
#include "gmock/gmock.h"

#include "optimizer/abstract_syntax_tree/projection_node.hpp"
#include "optimizer/abstract_syntax_tree/table_node.hpp"
#include "optimizer/abstract_syntax_tree/table_scan_node.hpp"
#include "optimizer/strategies/predicate_reordering_rule.hpp"

namespace opossum {

class TableStatisticsMock : public TableStatistics {
 public:
  TableStatisticsMock() {
    _row_count = 0;
  }

  TableStatisticsMock(double row_count) {
    _row_count = row_count;
  }

  std::shared_ptr<TableStatistics> predicate_statistics(const std::string &column_name, const std::string &op,
                                                                const AllParameterVariant value,
                                                                const optional<AllTypeVariant> value2) override {
    if (column_name == "c1") {
      return std::make_shared<TableStatisticsMock>(500);
    }
    if (column_name == "c2") {
      return std::make_shared<TableStatisticsMock>(200);
    }
    if (column_name == "c3") {
      return std::make_shared<TableStatisticsMock>(950);
    }

    Fail("Shouldn't happen");
    return {};
  }
};

class PredicateReorderingTest : public BaseTest {
protected:
  void SetUp() override {}
};

TEST_F(PredicateReorderingTest, SimpleReorderingTest) {
  PredicateReorderingRule rule;

  auto t_n = std::make_shared<TableNode>("a");

  t_n->set_statistics(std::make_shared<TableStatisticsMock>());

  auto ts_n_0 = std::make_shared<TableScanNode>("c1", ScanType::OpGreaterThan, 10);
  ts_n_0->set_left(t_n);

  auto ts_n_1 = std::make_shared<TableScanNode>("c2", ScanType::OpGreaterThan, 50);
  ts_n_1->set_left(ts_n_0);

  ts_n_1->get_or_create_statistics();


  auto reordered = rule.apply_rule(ts_n_1);

  std::cout << " Printing result " << std::endl;
  reordered->print();

  ASSERT_EQ(reordered, ts_n_1);
  ASSERT_EQ(reordered->left(), ts_n_0);
  ASSERT_EQ(reordered->left()->left(), t_n);
}

TEST_F(PredicateReorderingTest, MoreComplexReorderingTest) {
  PredicateReorderingRule rule;

  auto t_n = std::make_shared<TableNode>("a");

  t_n->set_statistics(std::make_shared<TableStatisticsMock>());

  auto ts_n_0 = std::make_shared<TableScanNode>("c1", ScanType::OpGreaterThan, 10);
  ts_n_0->set_left(t_n);

  auto ts_n_1 = std::make_shared<TableScanNode>("c2", ScanType::OpGreaterThan, 50);
  ts_n_1->set_left(ts_n_0);

  auto ts_n_2 = std::make_shared<TableScanNode>("c3", ScanType::OpGreaterThan, 90);
  ts_n_2->set_left(ts_n_1);

  ts_n_2->get_or_create_statistics();


  auto reordered = rule.apply_rule(ts_n_2);

  std::cout << " Printing result " << std::endl;
  reordered->print();

  ASSERT_EQ(reordered, ts_n_2);
  ASSERT_EQ(reordered->left(), ts_n_0);
  ASSERT_EQ(reordered->left()->left(), ts_n_1);
  ASSERT_EQ(reordered->left()->left()->left(), t_n);
}

TEST_F(PredicateReorderingTest, ComplexReorderingTest) {
  PredicateReorderingRule rule;

  auto t_n = std::make_shared<TableNode>("a");

  t_n->set_statistics(std::make_shared<TableStatisticsMock>());

  auto ts_n_0 = std::make_shared<TableScanNode>("c1", ScanType::OpGreaterThan, 10);
  ts_n_0->set_left(t_n);

  auto ts_n_1 = std::make_shared<TableScanNode>("c2", ScanType::OpGreaterThan, 50);
  ts_n_1->set_left(ts_n_0);

  auto ts_n_2 = std::make_shared<TableScanNode>("c3", ScanType::OpGreaterThan, 90);
  ts_n_2->set_left(ts_n_1);

  std::vector<std::string> columns ({"c1", "c2"});
  auto p_n = std::make_shared<ProjectionNode>(columns);
  p_n->set_left(ts_n_2);

  auto ts_n_3 = std::make_shared<TableScanNode>("c1", ScanType::OpGreaterThan, 10);
  ts_n_3->set_left(p_n);

  auto ts_n_4 = std::make_shared<TableScanNode>("c2", ScanType::OpGreaterThan, 50);
  ts_n_4->set_left(ts_n_3);

  ts_n_4->get_or_create_statistics();

  auto reordered = rule.apply_rule(ts_n_4);

  std::cout << " Printing result " << std::endl;
  reordered->print();

  ASSERT_EQ(reordered, ts_n_3);
  ASSERT_EQ(reordered->left(), ts_n_4);
  ASSERT_EQ(reordered->left()->left(), p_n);
  ASSERT_EQ(reordered->left()->left()->left(), ts_n_2);
  ASSERT_EQ(reordered->left()->left()->left()->left(), ts_n_0);
  ASSERT_EQ(reordered->left()->left()->left()->left()->left(), ts_n_1);
  ASSERT_EQ(reordered->left()->left()->left()->left()->left()->left(), t_n);
}

}  // namespace opossum
