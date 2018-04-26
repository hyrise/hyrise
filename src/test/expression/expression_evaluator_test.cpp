#include "gtest/gtest.h"

#include <optional>

#include "expression/expression_evaluator.hpp"
#include "expression/arithmetic_expression.hpp"
#include "expression/pqp_column_expression.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"
#include "utils/load_table.hpp"

namespace opossum {

class ExpressionEvaluatorTest : public ::testing::Test {
 public:
  void SetUp() override {
    table = load_table("src/test/tables/expression_evaluator/input.tbl");
    chunk = table->get_chunk(ChunkID{0});
    evaluator.emplace(chunk);

    a = std::make_shared<PQPColumnExpression>(ColumnID{0});
    b = std::make_shared<PQPColumnExpression>(ColumnID{1});
    a_plus_b = std::make_shared<ArithmeticExpression>(ArithmeticOperator::Addition, a, b);
  }

  std::shared_ptr<Table> table;
  std::shared_ptr<Chunk> chunk;
  std::optional<ExpressionEvaluator> evaluator;

  std::shared_ptr<PQPColumnExpression> a, b;
  std::shared_ptr<ArithmeticExpression> a_plus_b, a_minus_b, a_times_b, a_divided_by_b, a_modulo_b, a_to_the_b;
};

TEST_F(ExpressionEvaluatorTest, ArithmeticExpression) {
  const auto a_plus_b_result = std::vector<int32_t>({3, 5, 7, 9});
  EXPECT_EQ(boost::get<std::vector<int32_t>>(evaluator->evaluate_expression<int32_t>(*a_plus_b)), a_plus_b_result);
}

}  // namespace opossum