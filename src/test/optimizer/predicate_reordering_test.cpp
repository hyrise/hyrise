#include <memory>
#include <string>
#include <vector>

#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "optimizer/abstract_syntax_tree/projection_node.hpp"
#include "optimizer/abstract_syntax_tree/table_node.hpp"
#include "optimizer/abstract_syntax_tree/table_scan_node.hpp"
#include "optimizer/strategies/predicate_reordering_rule.hpp"

namespace opossum {

class PredicateReorderingTest : public BaseTest {
 protected:
  void SetUp() override { ast = setupAst(); }

  std::shared_ptr<AbstractNode> ast;

  std::shared_ptr<AbstractNode> setupAst() {
    const auto t_n = std::make_shared<TableNode>("a");

    const auto ts_n = std::make_shared<TableScanNode>("c1", nullptr, ScanType::OpEquals, "a");
    ts_n->set_left(t_n);

    const auto ts_n_2 = std::make_shared<TableScanNode>("c2", nullptr, ScanType::OpEquals, "a");
    ts_n_2->set_left(ts_n);

    return ts_n_2;
  }
};

TEST_F(PredicateReorderingTest, SimpleReorderingTest) {
  PredicateReorderingRule rule;

  auto reordered = rule.apply_rule(ast);

  std::cout << " Printing result " << std::endl;
  reordered->print();

  //    ASSERT_TRUE(false);
}

}  // namespace opossum
