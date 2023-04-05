#include "base_test.hpp"

#include "expression/expression_functional.hpp"
#include "operators/get_table.hpp"
#include "operators/join_hash.hpp"
#include "operators/limit.hpp"
#include "operators/pqp_utils.hpp"
#include "operators/projection.hpp"
#include "operators/table_wrapper.hpp"

namespace hyrise {

using namespace expression_functional;  // NOLIT(build/namespaces)

class PQPUtilsTest : public BaseTest {
 public:
  void SetUp() override {
    node_a = std::make_shared<GetTable>("foo");
  }

  std::shared_ptr<AbstractOperator> node_a;
};

TEST_F(PQPUtilsTest, VisitPQPStreamlinePQP) {
  const auto node_b = std::make_shared<GetTable>("bar");
  const auto node_c = std::make_shared<JoinHash>(
      node_b, node_a, JoinMode::Inner,
      OperatorJoinPredicate{ColumnIDPair{ColumnID{0}, ColumnID{0}}, PredicateCondition::Equals});
  const auto node_d = std::make_shared<Limit>(node_c, value_(1));

  const auto expected_nodes = std::vector<std::shared_ptr<AbstractOperator>>{node_d, node_c, node_b, node_a};
  auto actual_nodes = std::vector<std::shared_ptr<AbstractOperator>>{};

  visit_pqp(node_d, [&](const auto& node) {
    actual_nodes.emplace_back(node);
    return PQPVisitation::VisitInputs;
  });

  EXPECT_EQ(actual_nodes, expected_nodes);
}

TEST_F(PQPUtilsTest, VisitPQPDiamondStructure) {
  const auto node_b = std::make_shared<Limit>(node_a, value_(1));
  const auto node_c = std::make_shared<JoinHash>(
      node_b, node_b, JoinMode::Inner,
      OperatorJoinPredicate{ColumnIDPair{ColumnID{0}, ColumnID{0}}, PredicateCondition::Equals});
  const auto node_d = std::make_shared<Limit>(node_c, value_(1));

  const auto expected_nodes = std::vector<std::shared_ptr<AbstractOperator>>{node_d, node_c, node_b, node_a};
  auto actual_nodes = std::vector<std::shared_ptr<AbstractOperator>>{};

  visit_pqp(node_d, [&](const auto& node) {
    actual_nodes.emplace_back(node);
    return PQPVisitation::VisitInputs;
  });

  EXPECT_EQ(actual_nodes, expected_nodes);
}

TEST_F(PQPUtilsTest, VisitPQPNonConstOperators) {
  const auto node_b = std::make_shared<Limit>(node_a, value_(1));

  const auto expected_nodes = std::vector<std::shared_ptr<AbstractOperator>>{node_b, node_a};
  auto actual_nodes = std::vector<std::shared_ptr<AbstractOperator>>{};

  visit_pqp(node_b, [&](const auto& node) {
    actual_nodes.emplace_back(node);
    return PQPVisitation::VisitInputs;
  });

  EXPECT_EQ(actual_nodes, expected_nodes);
}

TEST_F(PQPUtilsTest, VisitPQPConstOperators) {
  const auto node_b = std::make_shared<const GetTable>("bar");
  const auto node_c = std::make_shared<const Limit>(node_b, value_(1));

  const auto expected_nodes = std::vector<std::shared_ptr<const AbstractOperator>>{node_c, node_b};
  auto actual_nodes = std::vector<std::shared_ptr<const AbstractOperator>>{};

  visit_pqp(node_c, [&](const auto& node) {
    actual_nodes.emplace_back(node);
    return PQPVisitation::VisitInputs;
  });

  EXPECT_EQ(actual_nodes, expected_nodes);
}

TEST_F(PQPUtilsTest, ResolveUncorrelatedSubquery) {
  const auto table_wrapper = std::make_shared<TableWrapper>(Projection::dummy_table());
  table_wrapper->execute();

  EXPECT_EQ(resolve_uncorrelated_subquery(table_wrapper), AllTypeVariant(0));
}

TEST_F(PQPUtilsTest, ResolveUncorrelatedSubqueryWrongOperatorState) {
  const auto table_wrapper = std::make_shared<TableWrapper>(Projection::dummy_table());

  // Operator is not executed yet.
  EXPECT_THROW(resolve_uncorrelated_subquery(table_wrapper), std::logic_error);

  table_wrapper->execute();
  table_wrapper->clear_output();

  // Operator is cleared.
  EXPECT_THROW(resolve_uncorrelated_subquery(table_wrapper), std::logic_error);
}

TEST_F(PQPUtilsTest, ResolveUncorrelatedSubqueryEmptyResult) {
  const auto dummy_table = Table::create_dummy_table({{"a", DataType::Int, false}});
  const auto table_wrapper = std::make_shared<TableWrapper>(dummy_table);
  table_wrapper->execute();

  EXPECT_TRUE(variant_is_null(resolve_uncorrelated_subquery(table_wrapper)));
}

TEST_F(PQPUtilsTest, ResolveUncorrelatedSubqueryNullValue) {
  const auto dummy_table = Table::create_dummy_table({{"a", DataType::Int, true}});
  dummy_table->append({NULL_VALUE});
  const auto table_wrapper = std::make_shared<TableWrapper>(dummy_table);
  table_wrapper->execute();

  EXPECT_TRUE(variant_is_null(resolve_uncorrelated_subquery(table_wrapper)));
}

TEST_F(PQPUtilsTest, ResolveUncorrelatedSubqueryTooManyTuples) {
  const auto table_too_wide = Table::create_dummy_table({{"a", DataType::Int, false}, {"a", DataType::Int, false}});
  table_too_wide->append({1, 2});
  const auto table_wrapper_too_wide = std::make_shared<TableWrapper>(table_too_wide);
  table_wrapper_too_wide->execute();

  EXPECT_THROW(resolve_uncorrelated_subquery(table_wrapper_too_wide), std::logic_error);

  const auto table_too_long = Table::create_dummy_table({{"a", DataType::Int, false}});
  table_too_long->append({1});
  table_too_long->append({2});
  const auto table_wrapper_too_long = std::make_shared<TableWrapper>(table_too_long);
  table_wrapper_too_long->execute();

  EXPECT_THROW(resolve_uncorrelated_subquery(table_wrapper_too_long), std::logic_error);
}

}  // namespace hyrise
