#include "gtest/gtest.h"

#include <optional>

#include "expression/expression_evaluator.hpp"
#include "expression/arithmetic_expression.hpp"
#include "expression/external_expression.hpp"
#include "expression/pqp_column_expression.hpp"
#include "expression/pqp_select_expression.hpp"
#include "expression/value_placeholder_expression.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"
#include "operators/get_table.hpp"
#include "operators/projection.hpp"
#include "operators/aggregate.hpp"
#include "operators/table_wrapper.hpp"
#include "utils/load_table.hpp"
#include "testing_assert.hpp"

namespace opossum {

class ExpressionEvaluatorTest : public ::testing::Test {
 public:
  void SetUp() override {
    table_a = load_table("src/test/tables/expression_evaluator/input_a.tbl");
    chunk_a = table_a->get_chunk(ChunkID{0});
    evaluator.emplace(chunk_a);

    a = std::make_shared<PQPColumnExpression>(ColumnID{0}, table_a->column_data_type(ColumnID{0}));
    b = std::make_shared<PQPColumnExpression>(ColumnID{1}, table_a->column_data_type(ColumnID{1}));
    a_plus_b = std::make_shared<ArithmeticExpression>(ArithmeticOperator::Addition, a, b);

    table_b = load_table("src/test/tables/expression_evaluator/input_b.tbl");
  }

  std::shared_ptr<Table> table_a, table_b;
  std::shared_ptr<Chunk> chunk_a;
  std::optional<ExpressionEvaluator> evaluator;

  std::shared_ptr<PQPColumnExpression> a, b;
  std::shared_ptr<ArithmeticExpression> a_plus_b, a_minus_b, a_times_b, a_divided_by_b, a_modulo_b, a_to_the_b;
};

TEST_F(ExpressionEvaluatorTest, ArithmeticExpression) {
  const auto a_plus_b_result = std::vector<int32_t>({3, 5, 7, 9});
  EXPECT_EQ(boost::get<std::vector<int32_t>>(evaluator->evaluate_expression<int32_t>(*a_plus_b)), a_plus_b_result);
}

TEST_F(ExpressionEvaluatorTest, PQPSelectExpression) {
  const auto table_wrapper_b = std::make_shared<TableWrapper>(table_b);
  const auto x = std::make_shared<PQPColumnExpression>(ColumnID{0}, table_b->column_data_type(ColumnID{0}));
  const auto external_b = std::make_shared<ValuePlaceholderExpression>(ValuePlaceholder{0});
  const auto b_plus_x = std::make_shared<ArithmeticExpression>(ArithmeticOperator::Addition, external_b, x);
  const auto inner_expressions = std::vector<std::shared_ptr<AbstractExpression>>({b_plus_x});
  const auto inner_projection = std::make_shared<Projection>(table_wrapper_b, inner_expressions);
  const auto aggregates = std::vector<AggregateColumnDefinition>({{AggregateFunction::Sum, ColumnID{0}}});
  const auto aggregate = std::make_shared<Aggregate>(inner_projection, aggregates, std::vector<ColumnID>{});

  const auto pqp_select_expression = std::make_shared<PQPSelectExpression>(aggregate, );

  const auto expected_result = std::vector<int32_t>({20, 9, 27, 7});
  EXPECT_EQ(boost::get<std::vector<int32_t>>(evaluator->evaluate_expression<int32_t>(*a_plus_b)), a_plus_b_result);
}

}  // namespace opossum