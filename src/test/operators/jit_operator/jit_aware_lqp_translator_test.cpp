#include "operators/jit_operator/jit_aware_lqp_translator.hpp"
#include "../../base_test.hpp"
#include "operators/jit_operator/operators/jit_compute.hpp"
#include "operators/jit_operator/operators/jit_filter.hpp"
#include "sql/sql_pipeline_builder.hpp"

namespace opossum {

class JitAwareLQPTranslatorTest : public BaseTest {
  void SetUp() override {
    auto int_int_int_table = load_table("src/test/tables/int_int_int.tbl");
    StorageManager::get().add_table("table_a", int_int_int_table);
  }

  void TearDown() override { StorageManager::get().reset(); }
};

TEST_F(JitAwareLQPTranslatorTest, SomeTest) {
  auto lqp = SQLPipelineBuilder("SELECT a, b, c FROM table_a WHERE a > 1 AND b > 2 AND c > 3")
                 .create_pipeline_statement(nullptr)
                 .get_unoptimized_logical_plan();
  JitAwareLQPTranslator lqp_translator;
  auto jit_operator_wrapper = std::dynamic_pointer_cast<JitOperatorWrapper>(lqp_translator.translate_node(lqp));

  ASSERT_NE(jit_operator_wrapper, nullptr);

  auto jit_operators = jit_operator_wrapper->jit_operators();
  ASSERT_EQ(jit_operator_wrapper->jit_operators().size(), 4u);

  auto jit_compute = std::dynamic_pointer_cast<JitCompute>(jit_operators[1]);
  auto jit_filter = std::dynamic_pointer_cast<JitFilter>(jit_operators[2]);

  ASSERT_NE(jit_compute, nullptr);
  ASSERT_NE(jit_filter, nullptr);

  auto expression_a = std::make_shared<JitExpression>(JitTupleValue(DataType::Int, false, 0));
  auto expression_1 = std::make_shared<JitExpression>(JitTupleValue(DataType::Int, false, 1));
  auto expression_b = std::make_shared<JitExpression>(JitTupleValue(DataType::Int, false, 2));
  auto expression_2 = std::make_shared<JitExpression>(JitTupleValue(DataType::Int, false, 3));
  auto expression_c = std::make_shared<JitExpression>(JitTupleValue(DataType::Int, false, 4));
  auto expression_3 = std::make_shared<JitExpression>(JitTupleValue(DataType::Int, false, 5));
  auto expression_a_gt_1 = std::make_shared<JitExpression>(expression_a, ExpressionType::GreaterThan, expression_1, 6);
  auto expression_b_gt_2 = std::make_shared<JitExpression>(expression_b, ExpressionType::GreaterThan, expression_2, 7);
  auto expression_c_gt_3 = std::make_shared<JitExpression>(expression_c, ExpressionType::GreaterThan, expression_3, 8);
  auto expression = std::make_shared<JitExpression>(expression_a_gt_1, ExpressionType::GreaterThan, std::make_shared<JitExpression>(expression_b_gt_2, ExpressionType::And, expression_c_gt_3, 9), 10);

  ASSERT_TRUE(*expression == *jit_compute->expression());
  //ASSERT_EQ(jit_filter->condition(), jit_compute->expression()->result());
}

TEST_F(JitAwareLQPTranslatorTest, SomeOtherTest) { ASSERT_FALSE(true); }

}  // namespace opossum
