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

  std::shared_ptr<ExpressionNode> compile_where_expression(const std::string query) {
    hsql::SQLParserResult parse_result;
    hsql::SQLParser::parseSQLString(query, &parse_result);

    if (!parse_result.isValid()) {
      throw std::runtime_error("Query is not valid.");
    }

    // DAMN RAW POINTERS!
    auto statement = parse_result.getStatements().at(0);

    switch (statement->type()) {
      case hsql::kStmtSelect: {
        const hsql::SelectStatement *select = (const hsql::SelectStatement *)statement;
        return _translator.translate_expression(*(select->whereClause));
      }
      default:
        throw std::runtime_error("Translating statement failed.");
    }
  }

  std::shared_ptr<std::vector<std::shared_ptr<ExpressionNode>>> compile_select_expression(const std::string query) {
    hsql::SQLParserResult parse_result;
    hsql::SQLParser::parseSQLString(query, &parse_result);

    if (!parse_result.isValid()) {
      throw std::runtime_error("Query is not valid.");
    }

    // Caution, statement is a raw pointer!
    auto statement = parse_result.getStatements().at(0);
    auto expressions = std::make_shared<std::vector<std::shared_ptr<ExpressionNode>>>();

    switch (statement->type()) {
      case hsql::kStmtSelect: {
        const hsql::SelectStatement *select = (const hsql::SelectStatement *)statement;
        for (auto expr : *(select->selectList)) {
          expressions->emplace_back(_translator.translate_expression(*expr));
        }
        return expressions;
      }
      default:
        throw std::runtime_error("Translating statement failed.");
    }
  }

  SQLExpressionTranslator _translator;
};

TEST_F(SQLExpressionTranslatorTest, ExpressionTest) {
  const auto query = "SELECT * FROM table_a WHERE a = 1234 + 1";
  auto predicate = compile_where_expression(query);

  EXPECT_EQ(predicate->type(), ExpressionType::Equals);
  EXPECT_EQ(predicate->type(), ExpressionType::Equals);
  EXPECT_EQ(predicate->left_child()->type(), ExpressionType::ColumnReference);
  EXPECT_EQ(predicate->right_child()->type(), ExpressionType::Plus);
  EXPECT_EQ(predicate->right_child()->right_child()->type(), ExpressionType::Literal);
  EXPECT_EQ(predicate->right_child()->right_child()->type(), ExpressionType::Literal);
}

TEST_F(SQLExpressionTranslatorTest, ExpressionColumnReferenceTest) {
  const auto query = "SELECT * FROM table_a WHERE a = \"b\"";
  auto predicate = compile_where_expression(query);

  EXPECT_EQ(predicate->type(), ExpressionType::Equals);
  EXPECT_EQ(predicate->left_child()->type(), ExpressionType::ColumnReference);
  EXPECT_EQ(predicate->right_child()->type(), ExpressionType::ColumnReference);
}

TEST_F(SQLExpressionTranslatorTest, ExpressionStringTest) {
  const auto query = "SELECT * FROM table_a WHERE a = 'b'";
  auto predicate = compile_where_expression(query);

  EXPECT_EQ(predicate->type(), ExpressionType::Equals);
  EXPECT_EQ(predicate->left_child()->type(), ExpressionType::ColumnReference);
  EXPECT_EQ(predicate->right_child()->type(), ExpressionType::Literal);
}

TEST_F(SQLExpressionTranslatorTest, ExpressionLessThanTest) {
  const auto query = "SELECT * FROM table_a WHERE a > 1";
  auto predicate = compile_where_expression(query);

  EXPECT_EQ(predicate->type(), ExpressionType::Greater);
  EXPECT_EQ(predicate->left_child()->type(), ExpressionType::ColumnReference);
}

TEST_F(SQLExpressionTranslatorTest, ExpressionLessEqualsParameterTest) {
  const auto query = "SELECT * FROM table_a WHERE a >= ?";
  auto predicate = compile_where_expression(query);

  EXPECT_EQ(predicate->type(), ExpressionType::GreaterEquals);
  EXPECT_EQ(predicate->left_child()->type(), ExpressionType::ColumnReference);
  EXPECT_EQ(predicate->right_child()->type(), ExpressionType::Parameter);
}

TEST_F(SQLExpressionTranslatorTest, ExpressionFunctionTest) {
  const auto query = "SELECT a, SUM(b) FROM table_a GROUP BY a";
  auto expressions = compile_select_expression(query);

  EXPECT_EQ(expressions->size(), 2u);
  auto &first = expressions->at(0);
  auto &second = expressions->at(1);

  EXPECT_EQ(first->type(), ExpressionType::ColumnReference);
  EXPECT_EQ(second->type(), ExpressionType::FunctionReference);
  EXPECT_EQ(second->expression_list()->size(), 1u);
  EXPECT_EQ(second->expression_list()->at(0)->type(), ExpressionType::ColumnReference);
}

TEST_F(SQLExpressionTranslatorTest, ExpressionComplexFunctionTest) {
  const auto query = "SELECT SUM(b * c) as d FROM table_a";
  auto expressions = compile_select_expression(query);

  EXPECT_EQ(expressions->size(), 1u);
  auto &first = expressions->at(0);

  EXPECT_EQ(first->type(), ExpressionType::FunctionReference);
  EXPECT_EQ(first->expression_list()->size(), 1);
  EXPECT_EQ(first->expression_list()->at(0)->type(), ExpressionType::Asterisk);
  EXPECT_EQ(first->expression_list()->at(0)->left_child()->type(), ExpressionType::ColumnReference);
  EXPECT_EQ(first->expression_list()->at(0)->right_child()->type(), ExpressionType::ColumnReference);
}

TEST_F(SQLExpressionTranslatorTest, ExpressionStringConcatenationTest) {
  const auto query = "SELECT 'b' + 'c' as d FROM table_a";
  auto expressions = compile_select_expression(query);

  EXPECT_EQ(expressions->size(), 1u);
  auto &first = expressions->at(0);

  EXPECT_EQ(first->type(), ExpressionType::Plus);
  EXPECT_EQ(first->left_child()->type(), ExpressionType::Literal);
  EXPECT_EQ(first->right_child()->type(), ExpressionType::Literal);
}

}  // namespace opossum
