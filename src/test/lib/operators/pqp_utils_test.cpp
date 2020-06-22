#include "base_test.hpp"

#include "expression/expression_functional.hpp"
#include "operators/get_table.hpp"
#include "operators/join_hash.hpp"
#include "operators/limit.hpp"
#include "operators/pqp_utils.hpp"

namespace opossum {

class PQPUtilsTest : public BaseTest {
 public:
  void SetUp() override { node_a = std::make_shared<GetTable>("foo"); }
  std::shared_ptr<AbstractOperator> node_a;
};

TEST_F(PQPUtilsTest, VisitPQPStreamlinePQP) {
  auto node_b = std::make_shared<GetTable>("bar");
  auto node_c = std::make_shared<JoinHash>(
      node_b, node_a, JoinMode::Inner,
      OperatorJoinPredicate{ColumnIDPair{ColumnID{0}, ColumnID{0}}, PredicateCondition::Equals});
  auto node_d = std::make_shared<Limit>(node_c, to_expression(int64_t{1}));
  const auto expected_nodes = std::vector<std::shared_ptr<AbstractOperator>>{node_d, node_c, node_b, node_a};

  auto actual_nodes = std::vector<std::shared_ptr<AbstractOperator>>{};
  visit_pqp(expected_nodes[0], [&](const auto& node) {
    actual_nodes.emplace_back(node);
    return PQPVisitation::VisitInputs;
  });

  EXPECT_EQ(actual_nodes, expected_nodes);
}

TEST_F(PQPUtilsTest, VisitPQPDiamondStructure) {
  auto node_b = std::make_shared<Limit>(node_a, to_expression(int64_t{1}));
  auto node_c = std::make_shared<JoinHash>(
      node_b, node_b, JoinMode::Inner,
      OperatorJoinPredicate{ColumnIDPair{ColumnID{0}, ColumnID{0}}, PredicateCondition::Equals});
  auto node_d = std::make_shared<Limit>(node_c, to_expression(int64_t{1}));

  const auto expected_nodes = std::vector<std::shared_ptr<AbstractOperator>>{node_d, node_c, node_b, node_a};

  auto actual_nodes = std::vector<std::shared_ptr<AbstractOperator>>{};
  visit_pqp(expected_nodes[0], [&](const auto& node) {
    actual_nodes.emplace_back(node);
    return PQPVisitation::VisitInputs;
  });

  EXPECT_EQ(actual_nodes, expected_nodes);
}

TEST_F(PQPUtilsTest, VisitPQPNonConstOperators) {
  auto node_b = std::make_shared<Limit>(node_a, to_expression(int64_t{1}));
  const auto expected_nodes = std::vector<std::shared_ptr<AbstractOperator>>{node_b, node_a};

  auto actual_nodes = std::vector<std::shared_ptr<AbstractOperator>>{};
  visit_pqp(expected_nodes[0], [&](const auto& node) {
    actual_nodes.emplace_back(node);
    node->clear_output();
    return PQPVisitation::VisitInputs;
  });

  EXPECT_EQ(actual_nodes, expected_nodes);
}

TEST_F(PQPUtilsTest, VisitPQPConstOperators) {
  auto node_b = std::make_shared<const GetTable>("bar");
  auto node_c = std::make_shared<const Limit>(node_b, to_expression(int64_t{1}));
  const auto expected_nodes = std::vector<std::shared_ptr<const AbstractOperator>>{node_c, node_b};

  auto actual_nodes = std::vector<std::shared_ptr<const AbstractOperator>>{};
  visit_pqp(expected_nodes[0], [&](const auto& node) {
    actual_nodes.emplace_back(node);
    return PQPVisitation::VisitInputs;
  });

  EXPECT_EQ(actual_nodes, expected_nodes);
}

}  // namespace opossum
