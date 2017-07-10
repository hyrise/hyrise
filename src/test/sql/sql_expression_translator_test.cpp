#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "../base_test.hpp"
#include "SQLParser.h"
#include "gtest/gtest.h"

#include "optimizer/abstract_syntax_tree/projection_node.hpp"
#include "optimizer/abstract_syntax_tree/table_node.hpp"
#include "optimizer/abstract_syntax_tree/predicate_node.hpp"
#include "sql/sql_query_node_translator.hpp"
#include "storage/storage_manager.hpp"

namespace opossum {

class SQLExpressionTranslatorTest : public BaseTest {
 protected:
  void SetUp() override {}

  std::shared_ptr<AbstractAstNode> compile_query(const std::string query) {
    hsql::SQLParserResult parse_result;
    hsql::SQLParser::parseSQLString(query, &parse_result);

    if (!parse_result.isValid()) {
      throw std::runtime_error("Query is not valid.");
    }

    return _translator.translate_parse_result(parse_result)[0];
  }

  SQLQueryNodeTranslator _translator;
};

TEST_F(SQLExpressionTranslatorTest, ExpressionTest) {
  const auto query = "SELECT * FROM table_a WHERE a = 1234 + 1";
  std::cout << query << std::endl;
  auto result_node = compile_query(query);

  EXPECT_EQ(result_node->type(), AstNodeType::Projection);
  EXPECT_FALSE(result_node->right());

  auto ts_node_1 = result_node->left();
  EXPECT_EQ(ts_node_1->type(), AstNodeType::Predicate);
  EXPECT_FALSE(ts_node_1->right());

  auto predicate = std::static_pointer_cast<PredicateNode>(ts_node_1)->predicate();
  predicate->print();
  EXPECT_EQ(predicate->expression_type(), ExpressionType::ExpressionEquals);
}

TEST_F(SQLExpressionTranslatorTest, ExpressionStringTest) {
  const auto query = "SELECT * FROM table_a WHERE a = \"b\"";
  std::cout << query << std::endl;
  auto result_node = compile_query(query);

  EXPECT_EQ(result_node->type(), AstNodeType::Projection);
  EXPECT_FALSE(result_node->right());

  auto ts_node_1 = result_node->left();
  EXPECT_EQ(ts_node_1->type(), AstNodeType::Predicate);
  EXPECT_FALSE(ts_node_1->right());

  auto predicate = std::static_pointer_cast<PredicateNode>(ts_node_1)->predicate();
  predicate->print();
  EXPECT_EQ(predicate->expression_type(), ExpressionType::ExpressionEquals);
}

TEST_F(SQLExpressionTranslatorTest, ExpressionStringTest2) {
  const auto query = "SELECT * FROM table_a WHERE a = 'b'";
  std::cout << query << std::endl;
  auto result_node = compile_query(query);

  EXPECT_EQ(result_node->type(), AstNodeType::Projection);
  EXPECT_FALSE(result_node->right());

  auto ts_node_1 = result_node->left();
  EXPECT_EQ(ts_node_1->type(), AstNodeType::Predicate);
  EXPECT_FALSE(ts_node_1->right());

  auto predicate = std::static_pointer_cast<PredicateNode>(ts_node_1)->predicate();
  predicate->print();
  EXPECT_EQ(predicate->expression_type(), ExpressionType::ExpressionEquals);
}

}  // namespace opossum
