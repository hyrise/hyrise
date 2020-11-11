#include "base_test.hpp"

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
    Hyrise::get().storage_manager.add_table("int_int_int", load_table("resources/test_data/tbl/int_int_int.tbl", 2));
    _gt = std::make_shared<GetTable>("int_int_int");
    _gt->execute();
  }

  std::shared_ptr<GetTable> _gt;
//  std::shared_ptr<TableWrapper> _table_wrapper_a, _table_wrapper_b, _table_wrapper_c, _table_wrapper_d;
};

TEST_F(OperatorClearOutputTest, ConsumerTracking) {
  // Prerequisite
  EXPECT_EQ(_gt->consumer_count(), 0);
  EXPECT_NE(_gt->get_output(), nullptr);

  // Add consumers
  auto validate1 = std::make_shared<Validate>(_gt);
  EXPECT_EQ(_gt->consumer_count(), 1);
  auto validate2 = std::make_shared<Validate>(_gt);
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

TEST_F(OperatorClearOutputTest, ConsumerTrackingUncorrelatedSubqueryProjection) {

}

TEST_F(OperatorClearOutputTest, ConsumerTrackingUncorrelatedSubqueryTableScan) {
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
  auto pqp_subquery_expression = pqp_subquery_(literal_projection, DataType::Int, false);
  auto table_scan = std::make_shared<TableScan>(validate, greater_than_(pqp_column_(ColumnID{1}, DataType::Int,false, "b"), pqp_subquery_expression));

  // a IN (SELECT 9 + 2)
  const auto semi_join_predicate = OperatorJoinPredicate{{ColumnID{0}, ColumnID{0}}, PredicateCondition::Equals};
  const auto semi_join = std::make_shared<JoinHash>(table_scan, literal_projection, JoinMode::Semi, semi_join_predicate);

  // Check consumer registering
  EXPECT_EQ(_gt->consumer_count(), 1);
  EXPECT_EQ(validate->consumer_count(), 1);
  EXPECT_EQ(table_scan->consumer_count(), 1);
  EXPECT_EQ(semi_join->consumer_count(), 0);

  EXPECT_EQ(dummy_table_wrapper->consumer_count(), 1);
  EXPECT_EQ(literal_projection->consumer_count(), 2);

  // Check consumer deregistering
  validate->execute();
  table_scan->execute();
  EXPECT_EQ(literal_projection->consumer_count(), 1);
  semi_join->execute();
  EXPECT_EQ(literal_projection->consumer_count(), 0);
}

TEST_F(OperatorClearOutputTest, ConsumerTrackingTableScanSubqueryCorrelated) {

}

}
