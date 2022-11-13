#include <memory>
#include <vector>

#include "base_test.hpp"

#include "expression/abstract_expression.hpp"
#include "expression/aggregate_expression.hpp"
#include "expression/pqp_column_expression.hpp"
#include "operators/aggregate_hash.hpp"
#include "operators/get_table.hpp"
#include "operators/join_hash.hpp"
#include "operators/projection.hpp"
#include "operators/table_scan.hpp"
#include "operators/table_wrapper.hpp"
#include "operators/validate.hpp"

using namespace hyrise::expression_functional;  // NOLINT

namespace hyrise {

class OperatorClearOutputTest : public BaseTest {
 protected:
  void SetUp() override {
    _table = load_table("resources/test_data/tbl/int_int_int.tbl", ChunkOffset{2});
    Hyrise::get().storage_manager.add_table(_table_name, _table);
    _a = PQPColumnExpression::from_table(*_table, ColumnID{0});
    _b = PQPColumnExpression::from_table(*_table, ColumnID{1});
    _c = PQPColumnExpression::from_table(*_table, ColumnID{2});

    _gt = std::make_shared<GetTable>(_table_name);
    _gt->execute();

    _ta_context = Hyrise::get().transaction_manager.new_transaction_context(AutoCommit::No);
  }

  const std::string _table_name = "int_int_int";
  std::shared_ptr<Table> _table;
  std::shared_ptr<PQPColumnExpression> _a, _b, _c;

  std::shared_ptr<GetTable> _gt;
  std::shared_ptr<TransactionContext> _ta_context;
};

TEST_F(OperatorClearOutputTest, ConsumerTracking) {
  // Prerequisite
  ASSERT_EQ(_gt->consumer_count(), 0);
  ASSERT_EQ(_gt->state(), OperatorState::ExecutedAndAvailable);
  ASSERT_NE(_gt->get_output(), nullptr);

  // Add consumers
  auto validate1 = std::make_shared<Validate>(_gt);
  validate1->set_transaction_context(_ta_context);
  EXPECT_EQ(_gt->consumer_count(), 1);

  auto validate2 = std::make_shared<Validate>(_gt);
  validate2->set_transaction_context(_ta_context);
  EXPECT_EQ(_gt->consumer_count(), 2);

  // Remove consumer 1
  validate1->execute();
  ASSERT_EQ(validate1->state(), OperatorState::ExecutedAndAvailable);
  ASSERT_NE(validate1->get_output(), nullptr);

  EXPECT_EQ(_gt->consumer_count(), 1);
  EXPECT_NE(_gt->get_output(), nullptr);

  // Remove consumer 2
  validate2->execute();
  ASSERT_EQ(validate2->state(), OperatorState::ExecutedAndAvailable);
  ASSERT_NE(validate2->get_output(), nullptr);

  EXPECT_EQ(_gt->consumer_count(), 0);
  // Output should have been cleared by now.
  EXPECT_EQ(_gt->state(), OperatorState::ExecutedAndCleared);
}

TEST_F(OperatorClearOutputTest, NeverClearOutput) {
  // Check whether we can prevent auto-clearing of operators.
  _gt->never_clear_output();

  // Prerequisite
  ASSERT_EQ(_gt->consumer_count(), 0);
  ASSERT_EQ(_gt->state(), OperatorState::ExecutedAndAvailable);

  // Prepare
  auto validate = std::make_shared<Validate>(_gt);
  validate->set_transaction_context(_ta_context);
  EXPECT_EQ(_gt->consumer_count(), 1);

  // Check whether output of GetTable is retained
  validate->execute();
  EXPECT_EQ(_gt->consumer_count(), 0);
  EXPECT_NE(_gt->get_output(), nullptr);
}

TEST_F(OperatorClearOutputTest, ConsumerTrackingTableScanUncorrelatedSubquery) {
  /**
   * Models a PQP for the following SQL query:
   *  SELECT a, b
   *  FROM int_int_int
   *  WHERE a IN (SELECT 9 + 2) AND b > (SELECT 9 + 2)
   */

  // Subquery: (SELECT 9 + 2)
  auto dummy_table_wrapper = std::make_shared<TableWrapper>(Projection::dummy_table());
  auto projection_literals =
      std::make_shared<Projection>(dummy_table_wrapper, expression_vector(add_(value_(9), value_(2))));

  // b > (SELECT 9 + 2)
  auto validate = std::make_shared<Validate>(_gt);
  validate->set_transaction_context(_ta_context);
  auto pqp_subquery_expression = pqp_subquery_(projection_literals, DataType::Int, false);
  auto table_scan = std::make_shared<TableScan>(
      validate, greater_than_(pqp_column_(ColumnID{1}, DataType::Int, false, "b"), pqp_subquery_expression));

  // a IN (SELECT 9 + 2)
  const auto semi_join_predicate = OperatorJoinPredicate{{ColumnID{0}, ColumnID{0}}, PredicateCondition::Equals};
  const auto semi_join =
      std::make_shared<JoinHash>(table_scan, projection_literals, JoinMode::Semi, semi_join_predicate);

  // SELECT a, b
  const auto projection = std::make_shared<Projection>(semi_join, expression_vector(_a, _b));

  // Check for consumer registration
  EXPECT_EQ(_gt->consumer_count(), 1);
  EXPECT_EQ(validate->consumer_count(), 1);
  EXPECT_EQ(table_scan->consumer_count(), 1);

  EXPECT_EQ(dummy_table_wrapper->consumer_count(), 1);
  EXPECT_EQ(projection_literals->consumer_count(), 2);

  EXPECT_EQ(semi_join->consumer_count(), 1);
  EXPECT_EQ(projection->consumer_count(), 0);

  // Check for consumer deregistration / output clearing
  validate->execute();
  table_scan->execute();
  EXPECT_EQ(projection_literals->consumer_count(), 1);
  semi_join->execute();
  EXPECT_EQ(projection_literals->consumer_count(), 0);
  EXPECT_EQ(projection_literals->state(), OperatorState::ExecutedAndCleared);
  EXPECT_EQ(dummy_table_wrapper->consumer_count(), 0);
  EXPECT_EQ(dummy_table_wrapper->state(), OperatorState::ExecutedAndCleared);
  projection->execute();
  EXPECT_EQ(semi_join->consumer_count(), 0);
  EXPECT_EQ(semi_join->state(), OperatorState::ExecutedAndCleared);
}

TEST_F(OperatorClearOutputTest, ConsumerTrackingTableScanUncorrelatedSubqueryNested) {
  /**
   * Based on ConsumerTrackingTableScanUncorrelatedSubquery, but with additional nesting of the subquery.
   * Models a PQP for the following SQL query:
   *  SELECT a, b
   *  FROM int_int_int
   *  WHERE a IN (SELECT 9 + 2) AND b IN (9, 10, (SELECT 9 + 2))
   */

  // Subquery: (SELECT 9 + 2)
  auto dummy_table_wrapper = std::make_shared<TableWrapper>(Projection::dummy_table());
  auto projection_literals =
      std::make_shared<Projection>(dummy_table_wrapper, expression_vector(add_(value_(9), value_(2))));

  // b IN (9, 10, (SELECT 9 + 2))
  auto validate = std::make_shared<Validate>(_gt);
  validate->set_transaction_context(_ta_context);
  auto pqp_subquery_expression = pqp_subquery_(projection_literals, DataType::Int, false);
  auto table_scan = std::make_shared<TableScan>(
      validate, in_(pqp_column_(ColumnID{1}, DataType::Int, false, "b"), list_(9, 10, pqp_subquery_expression)));

  // a IN (SELECT 9 + 2)
  const auto semi_join_predicate = OperatorJoinPredicate{{ColumnID{0}, ColumnID{0}}, PredicateCondition::Equals};
  const auto semi_join =
      std::make_shared<JoinHash>(table_scan, projection_literals, JoinMode::Semi, semi_join_predicate);

  // SELECT a, b
  const auto projection = std::make_shared<Projection>(semi_join, expression_vector(_a, _b));

  // Check for consumer registration
  EXPECT_EQ(_gt->consumer_count(), 1);
  EXPECT_EQ(validate->consumer_count(), 1);
  EXPECT_EQ(table_scan->consumer_count(), 1);

  EXPECT_EQ(dummy_table_wrapper->consumer_count(), 1);
  EXPECT_EQ(projection_literals->consumer_count(), 2);

  EXPECT_EQ(semi_join->consumer_count(), 1);
  EXPECT_EQ(projection->consumer_count(), 0);

  // Check for consumer deregistration / output clearing
  validate->execute();
  table_scan->execute();
  EXPECT_EQ(projection_literals->consumer_count(), 1);
  semi_join->execute();
  EXPECT_EQ(projection_literals->consumer_count(), 0);
  EXPECT_EQ(projection_literals->state(), OperatorState::ExecutedAndCleared);
  EXPECT_EQ(dummy_table_wrapper->consumer_count(), 0);
  EXPECT_EQ(dummy_table_wrapper->state(), OperatorState::ExecutedAndCleared);
  projection->execute();
  EXPECT_EQ(semi_join->consumer_count(), 0);
  EXPECT_EQ(semi_join->state(), OperatorState::ExecutedAndCleared);
}

TEST_F(OperatorClearOutputTest, ConsumerTrackingProjectionUncorrelatedSubquery) {
  /**
   * Models a PQP for the following SQL query:
   *  SELECT COUNT(*), (SELECT MAX(a) FROM int_int_int)
   *  FROM int_int_int
   */

  auto validate = std::make_shared<Validate>(_gt);
  validate->set_transaction_context(_ta_context);
  validate->execute();

  // Left input node: COUNT(*)
  const auto star = std::make_shared<PQPColumnExpression>(INVALID_COLUMN_ID, DataType::Long, false, "*");
  auto aggregate_count_star = std::make_shared<AggregateExpression>(AggregateFunction::Count, star);
  auto aggregate_hash_count_star = std::make_shared<AggregateHash>(
      validate, std::vector<std::shared_ptr<AggregateExpression>>{aggregate_count_star}, std::vector<ColumnID>{});
  // Subquery expression: (SELECT MAX(a) FROM int_int_int)
  auto aggregate_max_a = std::make_shared<AggregateExpression>(AggregateFunction::Max, _a);
  auto aggregate_hash_max_a = std::make_shared<AggregateHash>(
      validate, std::vector<std::shared_ptr<AggregateExpression>>{aggregate_max_a}, std::vector<ColumnID>{});

  // Projection: COUNT(*), (SELECT MAX(a) FROM int_int_int)
  auto pqp_count_star =
      pqp_column_(ColumnID{0}, aggregate_count_star->data_type(), false, aggregate_count_star->as_column_name());
  auto pqp_subquery_max_id = pqp_subquery_(aggregate_hash_max_a, DataType::Int, false);
  auto projection =
      std::make_shared<Projection>(aggregate_hash_count_star, expression_vector(pqp_count_star, pqp_subquery_max_id));
  // Check for consumer registration
  EXPECT_NE(validate->get_output(), nullptr);
  EXPECT_EQ(validate->consumer_count(), 2);
  EXPECT_EQ(aggregate_hash_count_star->consumer_count(), 1);
  EXPECT_EQ(aggregate_hash_max_a->consumer_count(), 1);

  // Check for consumer deregistration & output clearing
  aggregate_hash_max_a->execute();
  EXPECT_EQ(validate->consumer_count(), 1);
  aggregate_hash_count_star->execute();
  EXPECT_EQ(validate->consumer_count(), 0);
  EXPECT_EQ(validate->state(), OperatorState::ExecutedAndCleared);
  projection->execute();
  EXPECT_EQ(aggregate_hash_count_star->consumer_count(), 0);
  EXPECT_EQ(aggregate_hash_max_a->consumer_count(), 0);
  EXPECT_EQ(aggregate_hash_count_star->state(), OperatorState::ExecutedAndCleared);
  EXPECT_EQ(aggregate_hash_max_a->state(), OperatorState::ExecutedAndCleared);
}

TEST_F(OperatorClearOutputTest, ConsumerTrackingProjectionUncorrelatedSubqueryNested) {
  /**
   * Based on ConsumerTrackingProjectionUncorrelatedSubquery, but with additional nesting of the subquery.
   * Models a PQP for the following SQL query:
   *  SELECT (COUNT(*) + (SELECT MAX(a) FROM int_int_int))
   *  FROM int_int_int
   */

  auto validate = std::make_shared<Validate>(_gt);
  validate->set_transaction_context(_ta_context);
  validate->execute();

  // Left input node: COUNT(*)
  const auto star = std::make_shared<PQPColumnExpression>(INVALID_COLUMN_ID, DataType::Long, false, "*");
  auto aggregate_count_star = std::make_shared<AggregateExpression>(AggregateFunction::Count, star);
  auto aggregate_hash_count_star = std::make_shared<AggregateHash>(
      validate, std::vector<std::shared_ptr<AggregateExpression>>{aggregate_count_star}, std::vector<ColumnID>{});
  // Subquery expression: (SELECT MAX(a) FROM int_int_int)
  auto aggregate_max_a = std::make_shared<AggregateExpression>(AggregateFunction::Max, _a);
  auto aggregate_hash_max_a = std::make_shared<AggregateHash>(
      validate, std::vector<std::shared_ptr<AggregateExpression>>{aggregate_max_a}, std::vector<ColumnID>{});

  // Projection: COUNT(*) + (SELECT MAX(a) FROM int_int_int)
  auto pqp_count_star =
      pqp_column_(ColumnID{0}, aggregate_count_star->data_type(), false, aggregate_count_star->as_column_name());
  auto pqp_subquery_max_id = pqp_subquery_(aggregate_hash_max_a, DataType::Int, false);
  auto projection = std::make_shared<Projection>(aggregate_hash_count_star,
                                                 expression_vector(add_(pqp_count_star, pqp_subquery_max_id)));
  // Check for consumer registration
  EXPECT_NE(validate->get_output(), nullptr);
  EXPECT_EQ(validate->consumer_count(), 2);
  EXPECT_EQ(aggregate_hash_count_star->consumer_count(), 1);
  EXPECT_EQ(aggregate_hash_max_a->consumer_count(), 1);

  // Check for consumer deregistration & output clearing
  aggregate_hash_max_a->execute();
  EXPECT_EQ(validate->consumer_count(), 1);
  aggregate_hash_count_star->execute();
  EXPECT_EQ(validate->consumer_count(), 0);
  EXPECT_EQ(validate->state(), OperatorState::ExecutedAndCleared);
  projection->execute();
  EXPECT_EQ(aggregate_hash_count_star->consumer_count(), 0);
  EXPECT_EQ(aggregate_hash_max_a->consumer_count(), 0);
  EXPECT_EQ(aggregate_hash_count_star->state(), OperatorState::ExecutedAndCleared);
  EXPECT_EQ(aggregate_hash_max_a->state(), OperatorState::ExecutedAndCleared);
}

}  // namespace hyrise
