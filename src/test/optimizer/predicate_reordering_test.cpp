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
  void SetUp() override { query_plan = setupAst(); }

  std::shared_ptr<AbstractNode> query_plan, t_n, ts_n_0, ts_n_1, ts_n_2;

  std::shared_ptr<AbstractNode> setupAst() {
    t_n = std::make_shared<TableNode>("a");

    t_n->set_statistics(std::make_shared<TableStatisticsMock>());

    ts_n_0 = std::make_shared<TableScanNode>("c1", ScanType::OpGreaterThan, 10);
    ts_n_0->set_left(t_n);

    ts_n_1 = std::make_shared<TableScanNode>("c2", ScanType::OpGreaterThan, 50);
    ts_n_1->set_left(ts_n_0);

    ts_n_2 = std::make_shared<TableScanNode>("c3", ScanType::OpGreaterThan, 90);
    ts_n_2->set_left(ts_n_1);

    ts_n_2->get_or_create_statistics();

    return ts_n_2;
  }
};

TEST_F(PredicateReorderingTest, SimpleReorderingTest) {
  PredicateReorderingRule rule;

  auto reordered = rule.apply_rule(query_plan);

  std::cout << " Printing result " << std::endl;
  reordered->print();

  ASSERT_EQ(reordered, ts_n_2);
  ASSERT_EQ(reordered->left(), ts_n_0);
  ASSERT_EQ(reordered->left()->left(), ts_n_1);
}

}  // namespace opossum
