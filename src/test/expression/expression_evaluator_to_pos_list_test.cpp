#include <optional>

#include "gtest/gtest.h"

#include "expression/arithmetic_expression.hpp"
#include "expression/binary_predicate_expression.hpp"
#include "expression/case_expression.hpp"
#include "expression/evaluation/expression_evaluator.hpp"
#include "expression/evaluation/expression_result.hpp"
#include "expression/exists_expression.hpp"
#include "expression/expression_functional.hpp"
#include "expression/expression_utils.hpp"
#include "expression/extract_expression.hpp"
#include "expression/function_expression.hpp"
#include "expression/in_expression.hpp"
#include "expression/pqp_column_expression.hpp"
#include "expression/pqp_subquery_expression.hpp"
#include "expression/value_expression.hpp"
#include "operators/projection.hpp"
#include "operators/table_scan.hpp"
#include "operators/table_wrapper.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"
#include "testing_assert.hpp"
#include "utils/load_table.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

class ExpressionEvaluatorToPosListTest : public ::testing::Test {
 public:
  void SetUp() override {
    table_a = load_table("resources/test_data/tbl/expression_evaluator/input_a.tbl", 4);
    table_b = load_table("resources/test_data/tbl/expression_evaluator/input_b.tbl", 4);
    c = PQPColumnExpression::from_table(*table_a, "c");
    d = PQPColumnExpression::from_table(*table_a, "d");
    s1 = PQPColumnExpression::from_table(*table_a, "s1");
    s3 = PQPColumnExpression::from_table(*table_a, "s3");
    x = PQPColumnExpression::from_table(*table_b, "x");
  }

  bool test_expression(const std::shared_ptr<Table>& table, const ChunkID chunk_id,
                       const AbstractExpression& expression, const std::vector<ChunkOffset>& matching_chunk_offsets) {
    const auto actual_pos_list = ExpressionEvaluator{table, chunk_id}.evaluate_expression_to_pos_list(expression);

    auto expected_pos_list = PosList{};
    expected_pos_list.resize(matching_chunk_offsets.size());
    for (auto chunk_offset = ChunkOffset{0}; chunk_offset < matching_chunk_offsets.size(); ++chunk_offset) {
      expected_pos_list[chunk_offset] = RowID{chunk_id, matching_chunk_offsets[chunk_offset]};
    }

    return actual_pos_list == expected_pos_list;
  }

  std::shared_ptr<Table> table_a, table_b;

  std::shared_ptr<PQPColumnExpression> c, d, s1, s3, x;
};

TEST_F(ExpressionEvaluatorToPosListTest, PredicateWithoutNulls) {
  EXPECT_TRUE(test_expression(table_b, ChunkID{0}, *less_than_(x, 9), {3}));
  EXPECT_TRUE(test_expression(table_b, ChunkID{1}, *less_than_(x, 8), {1}));

  EXPECT_TRUE(test_expression(table_b, ChunkID{0}, *less_than_equals_(x, 9), {1, 3}));
  EXPECT_TRUE(test_expression(table_b, ChunkID{1}, *less_than_equals_(x, 7), {1}));

  EXPECT_TRUE(test_expression(table_b, ChunkID{0}, *equals_(x, 10), {0, 2}));
  EXPECT_TRUE(test_expression(table_b, ChunkID{1}, *equals_(x, 8), {0, 2}));

  EXPECT_TRUE(test_expression(table_b, ChunkID{0}, *not_equals_(x, 10), {1, 3}));
  EXPECT_TRUE(test_expression(table_b, ChunkID{1}, *not_equals_(x, 8), {1}));

  EXPECT_TRUE(test_expression(table_b, ChunkID{0}, *greater_than_(x, 9), {0, 2}));
  EXPECT_TRUE(test_expression(table_b, ChunkID{1}, *greater_than_(x, 9), {}));

  EXPECT_TRUE(test_expression(table_b, ChunkID{0}, *greater_than_equals_(x, 9), {0, 1, 2}));
  EXPECT_TRUE(test_expression(table_b, ChunkID{1}, *greater_than_equals_(x, 8), {0, 2}));

  EXPECT_TRUE(test_expression(table_b, ChunkID{0}, *between_(x, 8, 9), {1, 3}));
  EXPECT_TRUE(test_expression(table_b, ChunkID{1}, *between_(x, 7, 8), {0, 1, 2}));

  EXPECT_TRUE(test_expression(table_b, ChunkID{0}, *in_(x, list_(9, "hello", 10)), {0, 1, 2}));
  EXPECT_TRUE(test_expression(table_b, ChunkID{1}, *in_(x, list_(1, 2, 7)), {1}));
  EXPECT_TRUE(test_expression(table_b, ChunkID{0}, *not_in_(x, list_(9, "hello", 10)), {3}));
  EXPECT_TRUE(test_expression(table_b, ChunkID{1}, *not_in_(x, list_(1, 2, 7)), {0, 2}));

  EXPECT_TRUE(test_expression(table_a, ChunkID{0}, *like_(s1, "%a%"), {0, 2, 3}));

  EXPECT_TRUE(test_expression(table_a, ChunkID{0}, *not_like_(s1, "%a%"), {1}));
}

TEST_F(ExpressionEvaluatorToPosListTest, PredicatesWithOnlyLiterals) {
  EXPECT_TRUE(test_expression(table_a, ChunkID{0}, *like_("hello", "%ll%"), {0, 1, 2, 3}));
  EXPECT_TRUE(test_expression(table_a, ChunkID{0}, *like_("hello", "%lol%"), {}));
  EXPECT_TRUE(test_expression(table_b, ChunkID{0}, *in_(5, list_(1, 2)), {}));
  EXPECT_TRUE(test_expression(table_b, ChunkID{0}, *in_(5, list_(1, 2, 5)), {0, 1, 2, 3}));
  EXPECT_TRUE(test_expression(table_b, ChunkID{0}, *greater_than_(5, 1), {0, 1, 2, 3}));
  EXPECT_TRUE(test_expression(table_b, ChunkID{1}, *greater_than_(5, 1), {0, 1, 2}));
  EXPECT_TRUE(test_expression(table_b, ChunkID{1}, *between_(2, 5, 6), {}));
  EXPECT_TRUE(test_expression(table_b, ChunkID{0}, *between_(2, 1, 6), {0, 1, 2, 3}));
  EXPECT_TRUE(test_expression(table_b, ChunkID{0}, *value_(1), {0, 1, 2, 3}));
  EXPECT_TRUE(test_expression(table_b, ChunkID{0}, *value_(0), {}));
  EXPECT_TRUE(test_expression(table_b, ChunkID{0}, *is_null_(0), {}));
  EXPECT_TRUE(test_expression(table_b, ChunkID{0}, *is_null_(null_()), {0, 1, 2, 3}));
  EXPECT_TRUE(test_expression(table_b, ChunkID{0}, *or_(0, 1), {0, 1, 2, 3}));
  EXPECT_TRUE(test_expression(table_b, ChunkID{0}, *or_(0, 0), {}));
  EXPECT_TRUE(test_expression(table_b, ChunkID{0}, *and_(0, 1), {}));
  EXPECT_TRUE(test_expression(table_b, ChunkID{0}, *and_(1, 1), {0, 1, 2, 3}));
}

TEST_F(ExpressionEvaluatorToPosListTest, PredicateWithNulls) {
  EXPECT_TRUE(test_expression(table_a, ChunkID{0}, *equals_(c, 33), {0}));
  EXPECT_TRUE(test_expression(table_a, ChunkID{0}, *not_equals_(c, 33), {2}));
  EXPECT_TRUE(test_expression(table_a, ChunkID{0}, *less_than_(c, 35), {0, 2}));
  EXPECT_TRUE(test_expression(table_a, ChunkID{0}, *less_than_equals_(c, 35), {0, 2}));
  EXPECT_TRUE(test_expression(table_a, ChunkID{0}, *greater_than_(c, 33), {2}));
  EXPECT_TRUE(test_expression(table_a, ChunkID{0}, *greater_than_equals_(c, 0), {0, 2}));
  EXPECT_TRUE(test_expression(table_a, ChunkID{0}, *between_(c, 0, 100), {0, 2}));
  EXPECT_TRUE(test_expression(table_a, ChunkID{0}, *is_null_(c), {1, 3}));
  EXPECT_TRUE(test_expression(table_a, ChunkID{0}, *is_not_null_(c), {0, 2}));
  EXPECT_TRUE(test_expression(table_a, ChunkID{0}, *in_(c, list_(0, null_(), 33)), {0}));
  EXPECT_TRUE(test_expression(table_a, ChunkID{0}, *in_(c, list_(0, null_(), 33)), {0}));
}

TEST_F(ExpressionEvaluatorToPosListTest, LogicalWithoutNulls) {
  EXPECT_TRUE(test_expression(table_b, ChunkID{0}, *and_(greater_than_equals_(x, 8), less_than_(x, 10)), {1, 3}));
  EXPECT_TRUE(test_expression(table_b, ChunkID{1}, *and_(less_than_(x, 9), less_than_(x, 8)), {1}));

  EXPECT_TRUE(test_expression(table_b, ChunkID{0}, *or_(equals_(x, 10), less_than_(x, 2)), {0, 2}));
  EXPECT_TRUE(test_expression(table_b, ChunkID{0}, *or_(equals_(x, 10), not_equals_(x, 8)), {0, 1, 2}));
}

TEST_F(ExpressionEvaluatorToPosListTest, LogicalWithNulls) {
  EXPECT_TRUE(test_expression(table_a, ChunkID{0}, *and_(is_not_null_(c), equals_(c, 33)), {0}));
  EXPECT_TRUE(test_expression(table_a, ChunkID{0}, *or_(is_null_(c), equals_(c, 33)), {0, 1, 3}));
}

TEST_F(ExpressionEvaluatorToPosListTest, ExistsCorrelated) {
  const auto table_wrapper = std::make_shared<TableWrapper>(table_a);
  const auto table_scan =
      std::make_shared<TableScan>(table_wrapper, equals_(d, correlated_parameter_(ParameterID{0}, x)));
  const auto subquery = pqp_subquery_(table_scan, DataType::Int, false, std::make_pair(ParameterID{0}, ColumnID{0}));

  EXPECT_TRUE(test_expression(table_b, ChunkID{0}, *exists_(subquery), {}));
  EXPECT_TRUE(test_expression(table_b, ChunkID{1}, *exists_(subquery), {1}));

  EXPECT_TRUE(test_expression(table_b, ChunkID{0}, *not_exists_(subquery), {0, 1, 2, 3}));
  EXPECT_TRUE(test_expression(table_b, ChunkID{1}, *not_exists_(subquery), {0, 2}));
}

TEST_F(ExpressionEvaluatorToPosListTest, ExistsUncorrelated) {
  const auto table_wrapper_all = std::make_shared<TableWrapper>(Projection::dummy_table());
  const auto subquery_returning_all = pqp_subquery_(table_wrapper_all, DataType::Int, false);

  const auto empty_table = std::make_shared<Table>(TableColumnDefinitions{{"a", DataType::Int}}, TableType::Data);
  const auto table_wrapper_empty = std::make_shared<TableWrapper>(empty_table);
  const auto subquery_returning_none = pqp_subquery_(table_wrapper_empty, DataType::Int, false);

  EXPECT_TRUE(test_expression(table_b, ChunkID{0}, *exists_(subquery_returning_all), {0, 1, 2, 3}));
  EXPECT_TRUE(test_expression(table_b, ChunkID{0}, *exists_(subquery_returning_none), {}));

  EXPECT_TRUE(test_expression(table_b, ChunkID{0}, *not_exists_(subquery_returning_all), {}));
  EXPECT_TRUE(test_expression(table_b, ChunkID{0}, *not_exists_(subquery_returning_none), {0, 1, 2, 3}));
}

}  // namespace opossum
