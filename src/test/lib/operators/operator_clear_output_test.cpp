#include "base_test.hpp"

#include <memory>
#include <vector>

#include "expression/abstract_expression.hpp"
#include "expression/aggregate_expression.hpp"
#include "expression/pqp_column_expression.hpp"
#include "operators/aggregate_hash.hpp"
#include "operators/projection.hpp"
#include "operators/get_table.hpp"
#include "operators/join_hash.hpp"
#include "operators/table_wrapper.hpp"
#include "operators/table_scan.hpp"
#include "operators/validate.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

class OperatorClearOutputTest : public BaseTest {
 protected:
  void SetUp() override {
    _table = load_table("resources/test_data/tbl/int_int_int.tbl", 2);
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
  EXPECT_EQ(_gt->consumer_count(), 0);
  EXPECT_NE(_gt->get_output(), nullptr);

  // Add consumers
  auto validate1 = std::make_shared<Validate>(_gt);
  validate1->set_transaction_context(_ta_context);
  EXPECT_EQ(_gt->consumer_count(), 1);

  auto validate2 = std::make_shared<Validate>(_gt);
  validate2->set_transaction_context(_ta_context);
  EXPECT_EQ(_gt->consumer_count(), 2);

  // Remove consumers
  validate1->execute();
  EXPECT_EQ(_gt->consumer_count(), 1);
  EXPECT_NE(_gt->get_output(), nullptr);

  validate2->execute();
  EXPECT_EQ(_gt->consumer_count(), 0);

  // Output should have been cleared by now.
  EXPECT_EQ(_gt->get_output(), nullptr);
}

TEST_F(OperatorClearOutputTest, ConsumerTrackingTableScanUncorrelatedSubquery) {
  /**
   * Models the PQP of the following SQL query
   *  SELECT a
   *  FROM int_int_int
   *  WHERE a IN (SELECT 9 + 2) AND b > (SELECT 9 + 2)
   */

  // (SELECT 9 + 2)
  auto dummy_table_wrapper = std::make_shared<TableWrapper>(Projection::dummy_table());
  auto literal_projection = std::make_shared<Projection>(dummy_table_wrapper, expression_vector(add_(value_(9),value_(2))));

  // b > (SELECT 9 + 2)
  auto validate = std::make_shared<Validate>(_gt);
  validate->set_transaction_context(_ta_context);
  auto pqp_subquery_expression = pqp_subquery_(literal_projection, DataType::Int, false);
  auto table_scan = std::make_shared<TableScan>(validate, greater_than_(pqp_column_(ColumnID{1}, DataType::Int,false, "b"), pqp_subquery_expression));

  // a IN (SELECT 9 + 2)
  const auto semi_join_predicate = OperatorJoinPredicate{{ColumnID{0}, ColumnID{0}}, PredicateCondition::Equals};
  const auto semi_join = std::make_shared<JoinHash>(table_scan, literal_projection, JoinMode::Semi, semi_join_predicate);

  // Check for consumer registration
  EXPECT_EQ(_gt->consumer_count(), 1);
  EXPECT_EQ(validate->consumer_count(), 1);
  EXPECT_EQ(table_scan->consumer_count(), 1);
  EXPECT_EQ(semi_join->consumer_count(), 0);

  EXPECT_EQ(dummy_table_wrapper->consumer_count(), 1);
  EXPECT_EQ(literal_projection->consumer_count(), 2);

  // Check for consumer deregistration / output clearing
  validate->execute();
  table_scan->execute();
  EXPECT_EQ(literal_projection->consumer_count(), 1);
  semi_join->execute();
  EXPECT_EQ(literal_projection->consumer_count(), 0);
}

TEST_F(OperatorClearOutputTest, ConsumerTrackingProjectionNestedUncorrelatedSubquery) {
  // SELECT COUNT(*) - (SELECT MAX(a) FROM int_int_int) FROM int_int_int

  auto validate = std::make_shared<Validate>(_gt);
  validate->set_transaction_context(_ta_context);
  validate->execute();

  // Subquery: (SELECT a FROM int_int_int LIMIT 1)
  auto max_id = std::make_shared<AggregateExpression>(AggregateFunction::Max, _a);
  auto groupby = std::vector<ColumnID>{_b->column_id, _c->column_id};
  auto aggregates1 = std::vector<std::shared_ptr<AggregateExpression>>{max_id};
  auto aggregate_max_id = std::make_shared<AggregateHash>(validate, aggregates1, groupby);
  auto pqp_subquery_expression = pqp_subquery_(aggregate_max_id, DataType::Int, false);

  // COUNT(*)
  const auto star = std::make_shared<PQPColumnExpression>(INVALID_COLUMN_ID, DataType::Long, false, "*");
  auto count_star = std::make_shared<AggregateExpression>(AggregateFunction::Count, star);
  auto aggregates2 = std::vector<std::shared_ptr<AggregateExpression>>{count_star};
  auto aggregate_count_star = std::make_shared<AggregateHash>(validate, aggregates2, groupby);

  // Projection
  auto projection = std::make_shared<Projection>(aggregate_count_star, expression_vector(sub_(count_star, pqp_subquery_expression)));

  // Check for consumer registration
  EXPECT_NE(validate->get_output(), nullptr);
  EXPECT_EQ(validate->consumer_count(), 2);
  EXPECT_EQ(aggregate_count_star->consumer_count(), 1);
  EXPECT_EQ(aggregate_max_id->consumer_count(), 1);

  // Check for consumer deregistration & output clearing
  aggregate_max_id->execute();
  EXPECT_EQ(validate->consumer_count(), 1);
  aggregate_count_star->execute();
  EXPECT_EQ(validate->consumer_count(), 0);
  EXPECT_EQ(validate->get_output(), nullptr);
  projection->execute();
  EXPECT_EQ(aggregate_count_star->consumer_count(), 0);
  EXPECT_EQ(aggregate_max_id->consumer_count(), 0);
  EXPECT_EQ(aggregate_count_star->get_output(), nullptr);
  EXPECT_EQ(aggregate_max_id->get_output(), nullptr);
}

TEST_F(OperatorClearOutputTest, NeverClearOutput) {
  // Prerequisite
  EXPECT_EQ(_gt->consumer_count(), 0);
  EXPECT_NE(_gt->get_output(), nullptr);

  // Prepare
  _gt->never_clear_output();
  auto validate = std::make_shared<Validate>(_gt);
  validate->set_transaction_context(_ta_context);
  EXPECT_EQ(_gt->consumer_count(), 1);

  // Check whether output of GetTable is retained
  validate->execute();
  EXPECT_EQ(_gt->consumer_count(), 0);
  EXPECT_NE(_gt->get_output(), nullptr);
}

}
