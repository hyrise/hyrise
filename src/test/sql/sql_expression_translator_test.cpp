#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "../base_test.hpp"

#include "optimizer/abstract_syntax_tree/predicate_node.hpp"
#include "optimizer/abstract_syntax_tree/projection_node.hpp"
#include "optimizer/abstract_syntax_tree/stored_table_node.hpp"
#include "optimizer/expression/expression_node.hpp"
#include "sql/SQLStatement.h"
#include "sql/sql_expression_translator.hpp"
#include "storage/storage_manager.hpp"

namespace opossum {

class SQLExpressionTranslatorTest : public BaseTest {
 protected:
  void SetUp() override {}

  /*
   * The following two functions are quite similar and contain lots of code duplication.
   * However, extracting redundant lines of code led to inconsistent pointers in the hsql::Expr classes.
   * For example, nullptr were surprisingly pointing to invalid addresses or enum values changed.
   *
   * Due to time limitations and expected upcoming changes in hsql::SQLParser we decided to stick with the
   * code duplication for now.
   *
   * Hopefully this is fixed once hsql::SQLParser uses Smart Pointers as proposed in #55 in hyrise/sql-parser.
   * TODO(anyone): refactor these two methods
   */
  std::shared_ptr<ExpressionNode> compile_where_expression(const std::string &query) {
    hsql::SQLParserResult parse_result;
    hsql::SQLParser::parseSQLString(query, &parse_result);

    if (!parse_result.isValid()) {
      throw std::runtime_error("Query is not valid.");
    }

    const auto *statement = parse_result.getStatements().at(0);

    switch (statement->type()) {
      case hsql::kStmtSelect: {
        const auto *select = static_cast<const hsql::SelectStatement *>(statement);
        return _translator.translate_expression(*(select->whereClause));
      }
      default:
        throw std::runtime_error("Translating statement failed.");
    }
  }

  std::vector<std::shared_ptr<ExpressionNode>> compile_select_expression(const std::string &query) {
    hsql::SQLParserResult parse_result;
    hsql::SQLParser::parseSQLString(query, &parse_result);

    if (!parse_result.isValid()) {
      throw std::runtime_error("Query is not valid.");
    }

    const auto *statement = parse_result.getStatements().at(0);
    std::vector<std::shared_ptr<ExpressionNode>> expressions;

    switch (statement->type()) {
      case hsql::kStmtSelect: {
        const auto *select = static_cast<const hsql::SelectStatement *>(statement);
        for (auto expr : *(select->selectList)) {
          expressions.emplace_back(_translator.translate_expression(*expr));
        }
        return expressions;
      }
      default:
        throw std::runtime_error("Translating statement failed.");
    }
  }

  SQLExpressionTranslator _translator;
};

TEST_F(SQLExpressionTranslatorTest, ArithmeticExpression) {
  const auto query = "SELECT * FROM table_a WHERE a = 1234 + 1";
  auto predicate = compile_where_expression(query);

  EXPECT_EQ(predicate->type(), ExpressionType::Equals);
  EXPECT_EQ(predicate->left_child()->type(), ExpressionType::ColumnReference);
  EXPECT_EQ(predicate->right_child()->type(), ExpressionType::Plus);

  auto plus_expression = predicate->right_child();
  EXPECT_EQ(plus_expression->left_child()->type(), ExpressionType::Literal);
  EXPECT_EQ(plus_expression->right_child()->type(), ExpressionType::Literal);
}

TEST_F(SQLExpressionTranslatorTest, ExpressionColumnReference) {
  const auto query = "SELECT * FROM table_a WHERE a = \"b\"";
  auto predicate = compile_where_expression(query);

  EXPECT_EQ(predicate->type(), ExpressionType::Equals);
  EXPECT_EQ(predicate->left_child()->type(), ExpressionType::ColumnReference);
  EXPECT_EQ(predicate->right_child()->type(), ExpressionType::ColumnReference);
}

TEST_F(SQLExpressionTranslatorTest, ExpressionString) {
  const auto query = "SELECT * FROM table_a WHERE a = 'b'";
  auto predicate = compile_where_expression(query);

  EXPECT_EQ(predicate->type(), ExpressionType::Equals);
  EXPECT_EQ(predicate->left_child()->type(), ExpressionType::ColumnReference);
  EXPECT_EQ(predicate->right_child()->type(), ExpressionType::Literal);
}

TEST_F(SQLExpressionTranslatorTest, ExpressionGreaterThan) {
  const auto query = "SELECT * FROM table_a WHERE a > 1";
  auto predicate = compile_where_expression(query);

  EXPECT_EQ(predicate->type(), ExpressionType::GreaterThan);
  EXPECT_EQ(predicate->left_child()->type(), ExpressionType::ColumnReference);
  EXPECT_EQ(predicate->right_child()->type(), ExpressionType::Literal);
}

TEST_F(SQLExpressionTranslatorTest, ExpressionLessThan) {
  const auto query = "SELECT * FROM table_a WHERE a < 1";
  auto predicate = compile_where_expression(query);

  EXPECT_EQ(predicate->type(), ExpressionType::LessThan);
  EXPECT_EQ(predicate->left_child()->type(), ExpressionType::ColumnReference);
  EXPECT_EQ(predicate->right_child()->type(), ExpressionType::Literal);
}

TEST_F(SQLExpressionTranslatorTest, ExpressionGreaterEqualsParameter) {
  const auto query = "SELECT * FROM table_a WHERE a >= ?";
  auto predicate = compile_where_expression(query);

  EXPECT_EQ(predicate->type(), ExpressionType::GreaterThanEquals);
  EXPECT_EQ(predicate->left_child()->type(), ExpressionType::ColumnReference);
  EXPECT_EQ(predicate->right_child()->type(), ExpressionType::Placeholder);
}

TEST_F(SQLExpressionTranslatorTest, ExpressionLessEqualsParameter) {
  const auto query = "SELECT * FROM table_a WHERE a <= ?";
  auto predicate = compile_where_expression(query);

  EXPECT_EQ(predicate->type(), ExpressionType::LessThanEquals);
  EXPECT_EQ(predicate->left_child()->type(), ExpressionType::ColumnReference);
  EXPECT_EQ(predicate->right_child()->type(), ExpressionType::Placeholder);
}

TEST_F(SQLExpressionTranslatorTest, ExpressionStar) {
  const auto query = "SELECT * FROM table_a";
  auto expressions = compile_select_expression(query);

  ASSERT_EQ(expressions.size(), 1u);
  auto &first = expressions.at(0);

  EXPECT_EQ(first->type(), ExpressionType::Star);
  EXPECT_EQ(first->left_child(), nullptr);
  EXPECT_EQ(first->right_child(), nullptr);
}

TEST_F(SQLExpressionTranslatorTest, ExpressionFunction) {
  const auto query = "SELECT a, SUM(b) FROM table_a GROUP BY a";
  auto expressions = compile_select_expression(query);

  ASSERT_EQ(expressions.size(), 2u);
  auto &first = expressions.at(0);
  auto &second = expressions.at(1);

  EXPECT_EQ(first->type(), ExpressionType::ColumnReference);
  EXPECT_EQ(first->left_child(), nullptr);
  EXPECT_EQ(first->right_child(), nullptr);

  EXPECT_EQ(second->type(), ExpressionType::FunctionReference);
  ASSERT_EQ(second->expression_list().size(), 1u);
  EXPECT_EQ(second->expression_list().at(0)->type(), ExpressionType::ColumnReference);
}

TEST_F(SQLExpressionTranslatorTest, ExpressionComplexFunction) {
  const auto query = "SELECT SUM(b * c) as d FROM table_a";
  auto expressions = compile_select_expression(query);

  ASSERT_EQ(expressions.size(), 1u);
  auto &first = expressions.at(0);

  EXPECT_EQ(first->type(), ExpressionType::FunctionReference);
  ASSERT_EQ(first->expression_list().size(), 1u);

  auto function_expression = first->expression_list().at(0);
  EXPECT_EQ(function_expression->type(), ExpressionType::Asterisk);
  EXPECT_EQ(function_expression->left_child()->type(), ExpressionType::ColumnReference);
  EXPECT_EQ(function_expression->right_child()->type(), ExpressionType::ColumnReference);
}

TEST_F(SQLExpressionTranslatorTest, ExpressionStringConcatenation) {
  const auto query = "SELECT 'b' + 'c' as d FROM table_a";
  auto expressions = compile_select_expression(query);

  EXPECT_EQ(expressions.size(), 1u);
  auto &first = expressions.at(0);

  EXPECT_EQ(first->type(), ExpressionType::Plus);
  EXPECT_EQ(first->left_child()->type(), ExpressionType::Literal);
  EXPECT_EQ(first->right_child()->type(), ExpressionType::Literal);
}

// Enable as soon as SQLQueryNodeTranslator is merged
TEST_F(SQLExpressionTranslatorTest, DISABLED_ExpressionIn) {
  const auto query = "SELECT * FROM table_a WHERE a in (SELECT a FROM table_b)";
  auto expression = compile_where_expression(query);

  EXPECT_EQ(expression->type(), ExpressionType::In);
  EXPECT_EQ(expression->left_child()->type(), ExpressionType::ColumnReference);
  EXPECT_EQ(expression->right_child()->type(), ExpressionType::Select);
}

// Enable as soon as SQLQueryNodeTranslator is merged
TEST_F(SQLExpressionTranslatorTest, DISABLED_ExpressionExist) {
  const auto query = "SELECT * FROM table_a WHERE EXISTS (SELECT * FROM table_b)";
  auto expression = compile_where_expression(query);

  EXPECT_EQ(expression->type(), ExpressionType::Exists);
  EXPECT_EQ(expression->left_child()->type(), ExpressionType::Select);
}

// TODO(mp): implement, not supported yet
TEST_F(SQLExpressionTranslatorTest, DISABLED_ExpressionCase) {
  const auto query = "SELECT CASE WHEN a = 'something' THEN 'yes' ELSE 'no' END AS a_new FROM table_a";
  auto expressions = compile_select_expression(query);
}

}  // namespace opossum
