#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "operators/abstract_read_only_operator.hpp"
#include "operators/pqp_expression.hpp"
#include "operators/print.hpp"
#include "operators/projection.hpp"
#include "operators/table_scan.hpp"
#include "operators/table_wrapper.hpp"
#include "storage/chunk_encoder.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"
#include "types.hpp"

namespace opossum {

class OperatorsProjectionTest : public BaseTest {
 protected:
  void SetUp() override {
    _table_wrapper = std::make_shared<TableWrapper>(load_table("src/test/tables/int_float.tbl", 2));
    _table_wrapper->execute();

    _table_wrapper_int = std::make_shared<TableWrapper>(load_table("src/test/tables/int_int_int.tbl", 2));
    _table_wrapper_int->execute();

    _table_wrapper_int_null = std::make_shared<TableWrapper>(load_table("src/test/tables/int_int_int_null.tbl", 2));
    _table_wrapper_int_null->execute();

    _table_wrapper_int_zero = std::make_shared<TableWrapper>(load_table("src/test/tables/int_int4.tbl", 2));
    _table_wrapper_int_zero->execute();

    _table_wrapper_string = std::make_shared<TableWrapper>(load_table("src/test/tables/string.tbl", 2));
    _table_wrapper_string->execute();

    _table_wrapper_float = std::make_shared<TableWrapper>(load_table("src/test/tables/float_float_float.tbl", 2));
    _table_wrapper_float->execute();

    std::shared_ptr<Table> test_table_dict = load_table("src/test/tables/int_int_int.tbl", 2);
    ChunkEncoder::encode_all_chunks(test_table_dict);
    _table_wrapper_int_dict = std::make_shared<TableWrapper>(std::move(test_table_dict));
    _table_wrapper_int_dict->execute();

    std::shared_ptr<Table> test_table_dict_null = load_table("src/test/tables/int_int_int.tbl", 2);
    ChunkEncoder::encode_all_chunks(test_table_dict_null);
    _table_wrapper_int_dict_null = std::make_shared<TableWrapper>(std::move(test_table_dict_null));
    _table_wrapper_int_dict_null->execute();

    _dummy_wrapper = std::make_shared<TableWrapper>(Projection::dummy_table());
    _dummy_wrapper->execute();

    // Projection Expression: a + b + c
    _sum_a_b_c_expr = Projection::ColumnExpressions{PQPExpression::create_binary_operator(
        ExpressionType::Addition, PQPExpression::create_column(ColumnID{0}),
        PQPExpression::create_binary_operator(ExpressionType::Addition, PQPExpression::create_column(ColumnID{1}),
                                              PQPExpression::create_column(ColumnID{2})),
        {"sum"})};

    // Projection Expression: (a + b) * c
    _mul_a_b_c_expr = Projection::ColumnExpressions{PQPExpression::create_binary_operator(
        ExpressionType::Multiplication,
        PQPExpression::create_binary_operator(ExpressionType::Addition, PQPExpression::create_column(ColumnID{0}),
                                              PQPExpression::create_column(ColumnID{1})),
        PQPExpression::create_column(ColumnID{2}), {"mul"})};

    _sum_a_b_expr = Projection::ColumnExpressions{
        PQPExpression::create_binary_operator(ExpressionType::Addition, PQPExpression::create_column(ColumnID{0}),
                                              PQPExpression::create_column(ColumnID{1}), {"sum"})};

    _div_a_b_expr = Projection::ColumnExpressions{
        PQPExpression::create_binary_operator(ExpressionType::Division, PQPExpression::create_column(ColumnID{0}),
                                              PQPExpression::create_column(ColumnID{1}), {"div"})};

    _div_a_zero_expr = Projection::ColumnExpressions{
        PQPExpression::create_binary_operator(ExpressionType::Division, PQPExpression::create_column(ColumnID{0}),
                                              PQPExpression::create_literal(0), {"div"})};

    // Projection Expression: a
    _a_expr = Projection::ColumnExpressions{PQPExpression::create_column(ColumnID{0})};

    // Projection Expression: b
    _b_expr = Projection::ColumnExpressions{PQPExpression::create_column(ColumnID{1})};

    // Projection Expression: b, a
    _b_a_expr = Projection::ColumnExpressions{PQPExpression::create_column(ColumnID{1}),
                                              PQPExpression::create_column(ColumnID{0})};

    // Projection Expression: a, b
    _a_b_expr = Projection::ColumnExpressions{PQPExpression::create_column(ColumnID{0}),
                                              PQPExpression::create_column(ColumnID{1})};

    // Projection Expression: 123 AS a, A AS b
    _literal_expr = Projection::ColumnExpressions{PQPExpression::create_literal(123, std::string("a")),
                                                  PQPExpression::create_literal(std::string("A"), std::string("b"))};

    // Projection Expression: a + 'hallo' AS b
    _concat_expr = Projection::ColumnExpressions{
        PQPExpression::create_binary_operator(ExpressionType::Addition, PQPExpression::create_column(ColumnID{0}),
                                              PQPExpression::create_literal("hallo"), {"b"})};

    // Projection Expression: a + NULL AS b
    _add_null_expr = Projection::ColumnExpressions{
        PQPExpression::create_binary_operator(ExpressionType::Addition, PQPExpression::create_column(ColumnID{0}),
                                              PQPExpression::create_literal(NullValue{}), {"b"})};
  }

  Projection::ColumnExpressions _sum_a_b_expr;
  Projection::ColumnExpressions _div_a_b_expr;
  Projection::ColumnExpressions _div_a_zero_expr;
  Projection::ColumnExpressions _sum_a_b_c_expr;
  Projection::ColumnExpressions _mul_a_b_c_expr;
  Projection::ColumnExpressions _a_expr;
  Projection::ColumnExpressions _b_expr;
  Projection::ColumnExpressions _b_a_expr;
  Projection::ColumnExpressions _a_b_expr;
  Projection::ColumnExpressions _literal_expr;
  Projection::ColumnExpressions _concat_expr;
  Projection::ColumnExpressions _add_null_expr;
  std::shared_ptr<TableWrapper> _table_wrapper, _table_wrapper_int, _table_wrapper_int_null, _table_wrapper_int_zero,
      _table_wrapper_int_dict, _table_wrapper_int_dict_null, _table_wrapper_float, _dummy_wrapper,
      _table_wrapper_string;
};

TEST_F(OperatorsProjectionTest, SingleColumnInt) {
  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int.tbl", 1);

  auto projection = std::make_shared<Projection>(_table_wrapper, _a_expr);
  projection->execute();
  auto out = projection->get_output();
  EXPECT_TABLE_EQ_UNORDERED(projection->get_output(), expected_result);
}

TEST_F(OperatorsProjectionTest, DoubleProjectInt) {
  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int.tbl", 3);

  auto projection1 = std::make_shared<Projection>(_table_wrapper, _a_expr);
  projection1->execute();

  auto projection2 = std::make_shared<Projection>(projection1, _a_expr);
  projection2->execute();

  EXPECT_TABLE_EQ_UNORDERED(projection2->get_output(), expected_result);
}

TEST_F(OperatorsProjectionTest, SingleColumnFloat) {
  std::shared_ptr<Table> expected_result = load_table("src/test/tables/float.tbl", 1);

  auto projection = std::make_shared<Projection>(_table_wrapper_float, _a_expr);
  projection->execute();
  auto out = projection->get_output();
  EXPECT_TABLE_EQ_UNORDERED(projection->get_output(), expected_result);
}

TEST_F(OperatorsProjectionTest, DoubleProjectFloat) {
  std::shared_ptr<Table> expected_result = load_table("src/test/tables/float.tbl", 3);

  auto projection1 = std::make_shared<Projection>(_table_wrapper_float, _a_expr);
  projection1->execute();

  auto projection2 = std::make_shared<Projection>(projection1, _a_expr);
  projection2->execute();

  EXPECT_TABLE_EQ_UNORDERED(projection2->get_output(), expected_result);
}

TEST_F(OperatorsProjectionTest, AllColumns) {
  std::shared_ptr<Table> expected_result = load_table("src/test/tables/float_int.tbl", 2);

  auto projection = std::make_shared<Projection>(_table_wrapper, _b_a_expr);
  projection->execute();

  EXPECT_TABLE_EQ_UNORDERED(projection->get_output(), expected_result);
}

TEST_F(OperatorsProjectionTest, ConstantArithmeticProjection) {
  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_int_int_fix_values.tbl", 2);

  // 2+2
  Projection::ColumnExpressions column_expressions{PQPExpression::create_binary_operator(
      ExpressionType::Addition, PQPExpression::create_literal(2), PQPExpression::create_literal(2), {"fix"})};

  auto projection = std::make_shared<Projection>(_table_wrapper_int, column_expressions);
  projection->execute();

  EXPECT_TABLE_EQ_UNORDERED(projection->get_output(), expected_result);
}

TEST_F(OperatorsProjectionTest, SimpleArithmeticProjection) {
  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_int_addition.tbl", 2);

  auto projection = std::make_shared<Projection>(_table_wrapper_int, _sum_a_b_expr);
  projection->execute();

  EXPECT_TABLE_EQ_UNORDERED(projection->get_output(), expected_result);
}

TEST_F(OperatorsProjectionTest, StringConcat) {
  std::shared_ptr<Table> expected_result = load_table("src/test/tables/string_concatenated.tbl", 2);

  auto projection = std::make_shared<Projection>(_table_wrapper_string, _concat_expr);
  projection->execute();

  EXPECT_TABLE_EQ_UNORDERED(projection->get_output(), expected_result);
}

TEST_F(OperatorsProjectionTest, DivisionByZero) {
  auto projection = std::make_shared<Projection>(_table_wrapper_int_zero, _div_a_b_expr);
  EXPECT_THROW(projection->execute(), std::runtime_error);

  auto projection_literal = std::make_shared<Projection>(_table_wrapper_int_zero, _div_a_zero_expr);
  EXPECT_THROW(projection_literal->execute(), std::runtime_error);
}

TEST_F(OperatorsProjectionTest, AddNull) {
  std::shared_ptr<Table> expected_result = load_table("src/test/tables/string_concatenated_null.tbl", 2);

  auto projection = std::make_shared<Projection>(_table_wrapper_string, _add_null_expr);
  projection->execute();

  EXPECT_TABLE_EQ_UNORDERED(projection->get_output(), expected_result);
}

TEST_F(OperatorsProjectionTest, NestedArithmeticProjectionA) {
  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_int_int_multiplication.tbl", 2);

  auto projection = std::make_shared<Projection>(_table_wrapper_int, _mul_a_b_c_expr);
  projection->execute();

  EXPECT_TABLE_EQ_UNORDERED(projection->get_output(), expected_result);
}

TEST_F(OperatorsProjectionTest, NestedArithmeticProjectionWithNull) {
  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_int_int_multiplication_null.tbl", 2);

  auto projection = std::make_shared<Projection>(_table_wrapper_int_null, _mul_a_b_c_expr);
  projection->execute();

  EXPECT_TABLE_EQ_UNORDERED(projection->get_output(), expected_result);
}

TEST_F(OperatorsProjectionTest, NestedArithmeticProjectionB) {
  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_int_int_addition.tbl", 2);

  auto projection = std::make_shared<Projection>(_table_wrapper_int, _sum_a_b_c_expr);
  projection->execute();

  EXPECT_TABLE_EQ_UNORDERED(projection->get_output(), expected_result);
}

TEST_F(OperatorsProjectionTest, NestedArithmeticProjectionWithDictA) {
  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_int_int_addition.tbl", 2);

  auto projection = std::make_shared<Projection>(_table_wrapper_int_dict, _sum_a_b_c_expr);
  projection->execute();

  EXPECT_TABLE_EQ_UNORDERED(projection->get_output(), expected_result);
}

TEST_F(OperatorsProjectionTest, NestedArithmeticProjectionWithDictAndNull) {
  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_int_int_addition_null.tbl", 2);

  auto projection = std::make_shared<Projection>(_table_wrapper_int_dict_null, _sum_a_b_c_expr);
  projection->execute();

  EXPECT_TABLE_EQ_UNORDERED(projection->get_output(), expected_result);
}

TEST_F(OperatorsProjectionTest, VariableArithmeticWithRefProjection) {
  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_int_int_addition.tbl", 2);

  // creates ref_columns
  auto table_scan =
      std::make_shared<TableScan>(_table_wrapper_int_dict, ColumnID{0}, PredicateCondition::GreaterThan, "0");
  table_scan->execute();

  auto projection = std::make_shared<Projection>(table_scan, _sum_a_b_c_expr);
  projection->execute();

  EXPECT_TABLE_EQ_UNORDERED(projection->get_output(), expected_result);
}

TEST_F(OperatorsProjectionTest, ValueColumnCount) {
  auto projection_1 = std::make_shared<opossum::Projection>(_table_wrapper, _a_b_expr);
  projection_1->execute();
  EXPECT_EQ(projection_1->get_output()->column_count(), (u_int)2);
  EXPECT_EQ(projection_1->get_output()->row_count(), (u_int)3);

  auto projection_2 = std::make_shared<opossum::Projection>(_table_wrapper, _b_expr);
  projection_2->execute();
  EXPECT_EQ(projection_2->get_output()->column_count(), (u_int)1);
  EXPECT_EQ(projection_2->get_output()->row_count(), (u_int)3);

  auto projection_3 = std::make_shared<opossum::Projection>(_table_wrapper, _a_expr);
  projection_3->execute();
  EXPECT_EQ(projection_3->get_output()->column_count(), (u_int)1);
  EXPECT_EQ(projection_3->get_output()->row_count(), (u_int)3);
}

// TODO(anyone): refactor test
TEST_F(OperatorsProjectionTest, ReferenceColumnCount) {
  auto scan = std::make_shared<opossum::TableScan>(_table_wrapper, ColumnID{0}, PredicateCondition::Equals, 1234);
  scan->execute();

  auto projection_1 = std::make_shared<opossum::Projection>(scan, _a_b_expr);
  projection_1->execute();
  EXPECT_EQ(projection_1->get_output()->column_count(), (u_int)2);
  EXPECT_EQ(projection_1->get_output()->row_count(), (u_int)1);

  auto projection_2 = std::make_shared<opossum::Projection>(scan, _a_expr);
  projection_2->execute();
  EXPECT_EQ(projection_2->get_output()->column_count(), (u_int)1);
  EXPECT_EQ(projection_2->get_output()->row_count(), (u_int)1);

  auto projection_3 = std::make_shared<opossum::Projection>(scan, _b_expr);
  projection_3->execute();
  EXPECT_EQ(projection_3->get_output()->column_count(), (u_int)1);
  EXPECT_EQ(projection_3->get_output()->row_count(), (u_int)1);
}

TEST_F(OperatorsProjectionTest, Literals) {
  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_string_filtered.tbl", 1);

  auto projection = std::make_shared<Projection>(_dummy_wrapper, _literal_expr);
  projection->execute();
  auto out = projection->get_output();
  EXPECT_TABLE_EQ_UNORDERED(projection->get_output(), expected_result);
}

TEST_F(OperatorsProjectionTest, OperatorName) {
  auto projection_1 = std::make_shared<opossum::Projection>(_table_wrapper, _a_b_expr);

  EXPECT_EQ(projection_1->name(), "Projection");
}

}  // namespace opossum
