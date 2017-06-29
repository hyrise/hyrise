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

//class TableStatisticsMock : public TableStatistics {
//public:
//  MOCK_METHOD0(row_count, double());
////  MOCK_METHOD1(predicate_statistics, std::shared_ptr<TableStatistics>(const std::string &column_name, ScanType op,
////    const AllParameterVariant value,
////    const optional<AllTypeVariant> value2));
//};

class PredicateReorderingTest : public BaseTest {
protected:
  void SetUp() override { queryPlan = setupAst(); }

  std::shared_ptr<AbstractNode> queryPlan, t_n, ts_n_0, ts_n_1, ts_n_2;


  std::shared_ptr<AbstractNode> setupAst() {
    t_n = std::make_shared<TableNode>("a");
//    auto statisticsMock = std::make_shared<TableStatisticsMock>();
//    t_n->set_statistics(statisticsMock);

//    EXPECT_CALL(*statisticsMock, row_count())
//                .WillOnce(Return(5));

    ts_n_0 = std::make_shared<TableScanNode>("c1", ScanType::OpGreaterThan, 10);
    ts_n_0->set_left(t_n);

    ts_n_1 = std::make_shared<TableScanNode>("c2", ScanType::OpGreaterThan, 50);
    ts_n_1->set_left(ts_n_0);

    ts_n_2 = std::make_shared<TableScanNode>("c3", ScanType::OpGreaterThan, 90);
    ts_n_2->set_left(ts_n_1);

    return ts_n_2;
  }
};

TEST_F(PredicateReorderingTest, SimpleReorderingTest) {
  PredicateReorderingRule rule;

  auto reordered = rule.apply_rule(queryPlan);

  std::cout << " Printing result " << std::endl;
  reordered->print();
  ASSERT_EQ(reordered, ts_n_0);
  ASSERT_EQ(reordered->left(), ts_n_1);
  ASSERT_EQ(reordered->left()->left(), ts_n_2);
}

}  // namespace opossum
