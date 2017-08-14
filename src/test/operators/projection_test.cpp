#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "../../lib/operators/abstract_read_only_operator.hpp"
#include "../../lib/operators/print.hpp"
#include "../../lib/operators/projection.hpp"
#include "../../lib/operators/table_scan.hpp"
#include "../../lib/operators/table_wrapper.hpp"
#include "../../lib/storage/dictionary_compression.hpp"
#include "../../lib/storage/storage_manager.hpp"
#include "../../lib/storage/table.hpp"
#include "../../lib/types.hpp"

namespace opossum {

class OperatorsProjectionTest : public BaseTest {
 protected:
  void SetUp() override {
    _table_wrapper = std::make_shared<TableWrapper>(load_table("src/test/tables/int_float.tbl", 2));
    _table_wrapper->execute();

    _table_wrapper_int = std::make_shared<TableWrapper>(load_table("src/test/tables/int_int_int.tbl", 2));
    _table_wrapper_int->execute();

    std::shared_ptr<Table> test_table_dict = load_table("src/test/tables/int_int_int.tbl", 2);
    DictionaryCompression::compress_table(*test_table_dict);

    _table_wrapper_int_dict = std::make_shared<TableWrapper>(std::move(test_table_dict));
    _table_wrapper_int_dict->execute();

    // Projection Expression: a + b + c
    _sum_a_b_c_expr = Projection::ColumnExpressions{ExpressionNode::create_binary_operator(
        ExpressionType::Addition, ExpressionNode::create_column_reference("a"),
        ExpressionNode::create_binary_operator(ExpressionType::Addition, ExpressionNode::create_column_reference("b"),
                                               ExpressionNode::create_column_reference("c")),
        "sum")};  // NOLINT

    // Projection Expression: (a + b) * c
    _mul_a_b_c_expr = Projection::ColumnExpressions{ExpressionNode::create_binary_operator(
        ExpressionType::Multiplication,
        ExpressionNode::create_binary_operator(ExpressionType::Addition, ExpressionNode::create_column_reference("a"),
                                               ExpressionNode::create_column_reference("b")),
        ExpressionNode::create_column_reference("c"), "mul")};  // NOLINT

    _sum_a_b_expr = Projection::ColumnExpressions{
        ExpressionNode::create_binary_operator(ExpressionType::Addition, ExpressionNode::create_column_reference("a"),
                                               ExpressionNode::create_column_reference("b"), "sum")};  // NOLINT

    // Projection Expression: a
    _a_expr = Projection::ColumnExpressions{ExpressionNode::create_column_reference("a")};

    // Projection Expression: b
    _b_expr = Projection::ColumnExpressions{ExpressionNode::create_column_reference("b")};

    // Projection Expression: b, a
    _b_a_expr = Projection::ColumnExpressions{ExpressionNode::create_column_reference("b"),
                                              ExpressionNode::create_column_reference("a")};

    // Projection Expression: a, b
    _a_b_expr = Projection::ColumnExpressions{ExpressionNode::create_column_reference("a"),
                                              ExpressionNode::create_column_reference("b")};
  }

  Projection::ColumnExpressions _sum_a_b_expr;
  Projection::ColumnExpressions _sum_a_b_c_expr;
  Projection::ColumnExpressions _mul_a_b_c_expr;
  Projection::ColumnExpressions _a_expr;
  Projection::ColumnExpressions _b_expr;
  Projection::ColumnExpressions _b_a_expr;
  Projection::ColumnExpressions _a_b_expr;
  std::shared_ptr<TableWrapper> _table_wrapper, _table_wrapper_int, _table_wrapper_int_dict;
};

TEST_F(OperatorsProjectionTest, SingleColumn) {
  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int.tbl", 1);

  auto projection = std::make_shared<Projection>(_table_wrapper, _a_expr);
  projection->execute();
  auto out = projection->get_output();
  EXPECT_TABLE_EQ(projection->get_output(), expected_result);
}

TEST_F(OperatorsProjectionTest, DoubleProject) {
  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int.tbl", 3);

  auto projection1 = std::make_shared<Projection>(_table_wrapper, _a_expr);
  projection1->execute();

  auto projection2 = std::make_shared<Projection>(projection1, _a_expr);
  projection2->execute();

  EXPECT_TABLE_EQ(projection2->get_output(), expected_result);
}

TEST_F(OperatorsProjectionTest, AllColumns) {
  std::shared_ptr<Table> expected_result = load_table("src/test/tables/float_int.tbl", 2);

  auto projection = std::make_shared<Projection>(_table_wrapper, _b_a_expr);
  projection->execute();

  EXPECT_TABLE_EQ(projection->get_output(), expected_result);
}

TEST_F(OperatorsProjectionTest, ConstantArithmeticProjection) {
  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_int_int_fix_values.tbl", 2);

  // 2+2
  Projection::ColumnExpressions column_expressions{
      ExpressionNode::create_binary_operator(ExpressionType::Addition, ExpressionNode::create_literal(2),
                                             ExpressionNode::create_literal(2), "fix")};  // NOLINT

  auto projection = std::make_shared<Projection>(_table_wrapper_int, column_expressions);
  projection->execute();

  EXPECT_TABLE_EQ(projection->get_output(), expected_result);
}

TEST_F(OperatorsProjectionTest, SimpleArithmeticProjection) {
  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_int_addition.tbl", 2);

  auto projection = std::make_shared<Projection>(_table_wrapper_int, _sum_a_b_expr);
  projection->execute();

  EXPECT_TABLE_EQ(projection->get_output(), expected_result);
}

TEST_F(OperatorsProjectionTest, NestedArithmeticProjectionA) {
  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_int_int_multiplication.tbl", 2);

  auto projection = std::make_shared<Projection>(_table_wrapper_int, _mul_a_b_c_expr);
  projection->execute();

  EXPECT_TABLE_EQ(projection->get_output(), expected_result);
}

TEST_F(OperatorsProjectionTest, NestedArithmeticProjectionB) {
  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_int_int_addition.tbl", 2);

  auto projection = std::make_shared<Projection>(_table_wrapper_int, _sum_a_b_c_expr);
  projection->execute();

  EXPECT_TABLE_EQ(projection->get_output(), expected_result);
}

TEST_F(OperatorsProjectionTest, NestedArithmeticProjectionWithDictA) {
  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_int_int_addition.tbl", 2);

  auto projection = std::make_shared<Projection>(_table_wrapper_int_dict, _sum_a_b_c_expr);
  projection->execute();

  EXPECT_TABLE_EQ(projection->get_output(), expected_result);
}

TEST_F(OperatorsProjectionTest, VariableArithmeticWithRefProjection) {
  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_int_int_addition.tbl", 2);

  // creates ref_columns
  auto table_scan = std::make_shared<TableScan>(_table_wrapper_int_dict, "a", ScanType::OpGreaterThan, "0");
  table_scan->execute();

  auto projection = std::make_shared<Projection>(table_scan, _sum_a_b_c_expr);
  projection->execute();

  EXPECT_TABLE_EQ(projection->get_output(), expected_result);
}

TEST_F(OperatorsProjectionTest, ValueColumnCount) {
  auto projection_1 = std::make_shared<opossum::Projection>(_table_wrapper, _a_b_expr);
  projection_1->execute();
  EXPECT_EQ(projection_1->get_output()->col_count(), (u_int)2);
  EXPECT_EQ(projection_1->get_output()->row_count(), (u_int)3);

  auto projection_2 = std::make_shared<opossum::Projection>(_table_wrapper, _b_expr);
  projection_2->execute();
  EXPECT_EQ(projection_2->get_output()->col_count(), (u_int)1);
  EXPECT_EQ(projection_2->get_output()->row_count(), (u_int)3);

  auto projection_3 = std::make_shared<opossum::Projection>(_table_wrapper, _a_expr);
  projection_3->execute();
  EXPECT_EQ(projection_3->get_output()->col_count(), (u_int)1);
  EXPECT_EQ(projection_3->get_output()->row_count(), (u_int)3);
}

// TODO(anyone): refactor test
TEST_F(OperatorsProjectionTest, ReferenceColumnCount) {
  auto scan = std::make_shared<opossum::TableScan>(_table_wrapper, "a", ScanType::OpEquals, 1234);
  scan->execute();

  auto projection_1 = std::make_shared<opossum::Projection>(scan, _a_b_expr);
  projection_1->execute();
  EXPECT_EQ(projection_1->get_output()->col_count(), (u_int)2);
  EXPECT_EQ(projection_1->get_output()->row_count(), (u_int)1);

  auto projection_2 = std::make_shared<opossum::Projection>(scan, _a_expr);
  projection_2->execute();
  EXPECT_EQ(projection_2->get_output()->col_count(), (u_int)1);
  EXPECT_EQ(projection_2->get_output()->row_count(), (u_int)1);

  auto projection_3 = std::make_shared<opossum::Projection>(scan, _b_expr);
  projection_3->execute();
  EXPECT_EQ(projection_3->get_output()->col_count(), (u_int)1);
  EXPECT_EQ(projection_3->get_output()->row_count(), (u_int)1);
}

TEST_F(OperatorsProjectionTest, NumInputTables) {
  auto projection_1 = std::make_shared<opossum::Projection>(_table_wrapper, _a_b_expr);

  EXPECT_EQ(projection_1->num_in_tables(), 1);
}

TEST_F(OperatorsProjectionTest, NumOutputTables) {
  auto projection_1 = std::make_shared<opossum::Projection>(_table_wrapper, _a_b_expr);

  EXPECT_EQ(projection_1->num_out_tables(), 1);
}

TEST_F(OperatorsProjectionTest, OperatorName) {
  auto projection_1 = std::make_shared<opossum::Projection>(_table_wrapper, _a_b_expr);

  EXPECT_EQ(projection_1->name(), "Projection");
}

}  // namespace opossum
