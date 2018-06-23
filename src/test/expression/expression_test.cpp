#include "gtest/gtest.h"

#include "expression/case_expression.hpp"
#include "expression/expression_factory.hpp"
#include "expression/expression_utils.hpp"
#include "logical_query_plan/mock_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "utils/load_table.hpp"
#include "storage/storage_manager.hpp"

using namespace std::string_literals;  // NOLINT
using namespace opossum::expression_factory;  // NOLINT

namespace opossum {

class ExpressionTest : public ::testing::Test {
 public:
  void SetUp() {
    StorageManager::get().add_table("int_float", load_table("src/test/tables/int_float.tbl"));

    int_float_node_a = StoredTableNode::make("int_float");
    a = {int_float_node_a, ColumnID{0}};
    b = {int_float_node_a, ColumnID{1}};

    case_a = case_(equals(add(a, 5), b), add(5, b), a);
    case_b = case_(a, 1, 3);
    case_c = case_(equals(a, 123), b, case_(equals(a, 1234), a, null()));
  }

  void TearDown() {
    StorageManager::reset();
  }

  LQPColumnReference a, b;
  std::shared_ptr<AbstractExpression> case_a, case_b, case_c;
  std::shared_ptr<StoredTableNode> int_float_node_a;
};

TEST_F(ExpressionTest, DeepEquals) {
  EXPECT_TRUE(case_a->deep_equals(*case_a));
  const auto case_a_copy = case_a->deep_copy();
  EXPECT_TRUE(case_a->deep_equals(*case_a_copy));
  EXPECT_TRUE(case_c->deep_equals(*case_c));
  EXPECT_FALSE(case_a->deep_equals(*case_b));
  EXPECT_FALSE(case_a->deep_equals(*case_c));
}

TEST_F(ExpressionTest, AsColumnNameParentheses) {
  /** Test that parentheses are placed correctly in nested expressions */

  EXPECT_EQ(add(5, 3)->as_column_name(), "5 + 3");
  EXPECT_EQ(add(5, mul(2, 3))->as_column_name(), "5 + 2 * 3");
  EXPECT_EQ(mul(5, add(2, 3))->as_column_name(), "5 * (2 + 3)");
  EXPECT_EQ(div_(5, mul(2, 3))->as_column_name(), "5 / (2 * 3)");
  EXPECT_EQ(case_(greater_than(3,2), mul(2, 3), 2)->as_column_name(), "CASE WHEN 3 > 2 THEN 2 * 3 ELSE 2 END");
  EXPECT_EQ(case_(1, mul(2, 3), div_(5, mul(2, 3)))->as_column_name(), "CASE WHEN 1 THEN 2 * 3 ELSE 5 / (2 * 3) END");
  EXPECT_EQ(list(1, sum(a))->as_column_name(), "(1, SUM(a))");
  EXPECT_EQ(and_(1, 1)->as_column_name(), "1 AND 1");
  EXPECT_EQ(and_(1, greater_than(add(2, 3), 4))->as_column_name(), "1 AND 2 + 3 > 4");
  EXPECT_EQ(and_(1, or_(greater_than(add(2, 3), 4), 0))->as_column_name(), "1 AND (2 + 3 > 4 OR 0)");
  EXPECT_EQ(or_(1, and_(greater_than(add(2, 3), 4), 0))->as_column_name(), "1 OR (2 + 3 > 4 AND 0)");
  EXPECT_EQ(and_(1, and_(1, or_(0, 1)))->as_column_name(), "1 AND 1 AND (0 OR 1)");
  EXPECT_EQ(is_null(1)->as_column_name(), "1 IS NULL");
  EXPECT_EQ(is_null(and_(1, 1))->as_column_name(), "(1 AND 1) IS NULL");
  EXPECT_EQ(is_null(sum(a))->as_column_name(), "SUM(a) IS NULL");
  EXPECT_EQ(less_than(a, b)->as_column_name(), "a < b");
  EXPECT_EQ(less_than(add(a, 5), b)->as_column_name(), "a + 5 < b");
  EXPECT_EQ(between(a, 2, 3)->as_column_name(), "a BETWEEN 2 AND 3");
  EXPECT_EQ(and_(greater_than_equals(b, 5), between(a, 2, 3))->as_column_name(), "b > 5 AND a BETWEEN 2 AND 3");
  EXPECT_EQ(not_equals(between(a, 2, 3), 0)->as_column_name(), "(a BETWEEN 2 AND 3) != 0");

  EXPECT_EQ(mul(less_than(add(a, 5), b), 3)->as_column_name(), "(a + 5 < b) * 3");
  EXPECT_EQ(add(1, between(a, 2, 3))->as_column_name(), "1 + (a BETWEEN 2 AND 3)");

  // TODO(anybody) Omit redundant parentheses
  EXPECT_EQ(add(5, add(1, 3))->as_column_name(), "5 + (1 + 3)");
  EXPECT_EQ(add(add(2, 5), add(1, 3))->as_column_name(), "(2 + 5) + (1 + 3)");
  EXPECT_EQ(mul(mul(2, 5), mul(1, 3))->as_column_name(), "(2 * 5) * (1 * 3)");
  EXPECT_EQ(and_(and_(1, 0), and_(0, 1))->as_column_name(), "(1 AND 0) AND (0 AND 1)");
}

}  // namespace opossum
