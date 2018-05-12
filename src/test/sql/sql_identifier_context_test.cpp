#include "gtest/gtest.h"

#include <memory>

#include "expression/abstract_expression.hpp"
#include "expression/sql_identifier_expression.hpp"
#include "sql/sql_identifier_context.hpp"
#include "sql/sql_identifier_context_proxy.hpp"

using namespace std::string_literals;  // NOLINT

namespace opossum {

class SQLIdentifierContextTest : public ::testing::Test {
 public:
  void SetUp() override {
    expression_a = std::make_shared<SQLIdentifierExpression>(SQLIdentifier{"a", "T1"});
    expression_b = std::make_shared<SQLIdentifierExpression>(SQLIdentifier{"b", "T1"});
    expression_c = std::make_shared<SQLIdentifierExpression>(SQLIdentifier{"c", "T1"});
    expression_unnamed = std::make_shared<SQLIdentifierExpression>(SQLIdentifier{"d", "T1"});

    context.set_column_name(expression_a, {"a"s});
    context.set_column_name(expression_b, {"b"s});
    context.set_column_name(expression_c, {"c"s});
    context.set_table_name(expression_a, {"T1"s});
    context.set_table_name(expression_b, {"T1"s});
    context.set_table_name(expression_c, {"T2"s});
  }

  std::shared_ptr<AbstractExpression> expression_a, expression_b, expression_c, expression_unnamed;
  SQLIdentifierContext context;
};

TEST_F(SQLIdentifierContextTest, ResolveIdentifier) {
  EXPECT_EQ(context.resolve_identifier_relaxed({"a"s}), expression_a);
  EXPECT_EQ(context.resolve_identifier_relaxed({"b"s}), expression_b);
  EXPECT_EQ(context.resolve_identifier_relaxed({"c"s}), expression_c);
  EXPECT_EQ(context.resolve_identifier_relaxed({"a"s, "T1"}), expression_a);
  EXPECT_EQ(context.resolve_identifier_relaxed({"a"s, "T2"}), nullptr);
  EXPECT_EQ(context.resolve_identifier_relaxed({"x"s}), nullptr);
  EXPECT_EQ(context.resolve_identifier_relaxed({"x"s, "T1"}), nullptr);
}

TEST_F(SQLIdentifierContextTest, ColumnNameChanges) {
  context.set_column_name(expression_a, "x");

  EXPECT_EQ(context.resolve_identifier_relaxed({"a"s}), nullptr);
  EXPECT_EQ(context.resolve_identifier_relaxed({"a"s, "T1"}), nullptr);
  EXPECT_EQ(context.resolve_identifier_relaxed({"x"s, "T1"}), expression_a);
  EXPECT_EQ(context.resolve_identifier_relaxed({"x"s}), expression_a);

  EXPECT_EQ(context.resolve_identifier_relaxed({"b"s}), expression_b);
}

TEST_F(SQLIdentifierContextTest, TableNameChanges) {
  context.set_column_name(expression_a, "x");
  context.set_table_name(expression_a, "X");

  EXPECT_EQ(context.resolve_identifier_relaxed({"a"s}), nullptr);
  EXPECT_EQ(context.resolve_identifier_relaxed({"a"s, "T1"}), nullptr);
  EXPECT_EQ(context.resolve_identifier_relaxed({"x"s, "T1"}), nullptr);
  EXPECT_EQ(context.resolve_identifier_relaxed({"x"s}), expression_a);
  EXPECT_EQ(context.resolve_identifier_relaxed({"x"s, "X"}), expression_a);

  EXPECT_EQ(context.resolve_identifier_relaxed({"b"s}), expression_b);
  EXPECT_EQ(context.resolve_identifier_relaxed({"b"s, "T1"}), expression_b);
}

TEST_F(SQLIdentifierContextTest, ColumnNameRedundancy) {
  auto expression_a2 = std::make_shared<SQLIdentifierExpression>(SQLIdentifier{"a", "T2"});

  context.set_column_name(expression_a2, {"a"s});

  // "a" is ambiguous now
  EXPECT_EQ(context.resolve_identifier_relaxed({"a"s}), nullptr);

  EXPECT_EQ(context.resolve_identifier_relaxed({"a"s, "T1"}), expression_a);

  context.set_table_name(expression_a2, "T2");
  EXPECT_EQ(context.resolve_identifier_relaxed({"a"s, "T2"}), expression_a2);
}

TEST_F(SQLIdentifierContextTest, ResolveOuterExpression) {
  /**
   * Simulate a scenario in which a Sub-Subquery accesses an Identifier from the outermost and intermediate queries
   */

  /**
   * Create context and context proxy for the outermost query
   */
  const auto outermost_expression_a = std::make_shared<SQLIdentifierExpression>(SQLIdentifier{"a", "T1"});
  const auto outermost_expression_b = std::make_shared<SQLIdentifierExpression>(SQLIdentifier{"b", "T1"});
  const auto outermost_expression_c = std::make_shared<SQLIdentifierExpression>(SQLIdentifier{"c", "T1"});
  const auto outermost_context = std::make_shared<SQLIdentifierContext>();
  outermost_context->set_column_name(outermost_expression_a, "outermost_a");
  outermost_context->set_column_name(outermost_expression_b, "b"); // Intentionally named just "b"
  outermost_context->set_column_name(outermost_expression_c, "c"); // Intentionally named just "c"
  outermost_context->set_table_name(outermost_expression_b, "Outermost");

  const auto outermost_context_proxy = std::make_shared<SQLIdentifierContextProxy>(outermost_context);

  /**
   * Create context and context proxy for the nested ("intermediate") query
   */
  auto intermediate_context = std::make_shared<SQLIdentifierContext>();
  const auto intermediate_expression_a = std::make_shared<SQLIdentifierExpression>(SQLIdentifier{"a", "T2"});
  const auto intermediate_expression_b = std::make_shared<SQLIdentifierExpression>(SQLIdentifier{"b", "T2"});
  intermediate_context->set_column_name(intermediate_expression_a, "intermediate_a");
  intermediate_context->set_column_name(intermediate_expression_b, "b"); // Intentionally named just "b"
  intermediate_context->set_table_name(intermediate_expression_b, "Intermediate");

  const auto intermediate_context_proxy = std::make_shared<SQLIdentifierContextProxy>(intermediate_context, outermost_context_proxy);

  /**
   * Test whether identifiers are resolved correctly
   */
  EXPECT_EQ(context.resolve_identifier_relaxed({"a"}), expression_a);
  EXPECT_EQ(context.resolve_identifier_relaxed({"b"}), expression_b);
  EXPECT_EQ(context.resolve_identifier_relaxed({"b", "T1"}), expression_b);

  EXPECT_EQ(intermediate_context_proxy->resolve_identifier_relaxed({"b", "Intermediate"}), intermediate_expression_b);
  EXPECT_EQ(intermediate_context_proxy->resolve_identifier_relaxed({"intermediate_a"}), intermediate_expression_a);
  EXPECT_EQ(intermediate_context_proxy->resolve_identifier_relaxed({"intermediate_a", "Intermediate"}), nullptr);

  EXPECT_EQ(intermediate_context_proxy->resolve_identifier_relaxed({"outermost_a"}), outermost_expression_a);
  EXPECT_EQ(intermediate_context_proxy->resolve_identifier_relaxed({"b", "Outermost"}), outermost_expression_b);

  /**
   * Test whether the proxies tracked accesses to their contexts correctly
   */
  ASSERT_EQ(outermost_context_proxy->accessed_expressions().size(), 2u);
  EXPECT_EQ(outermost_context_proxy->accessed_expressions().at(0), outermost_expression_a);
  EXPECT_EQ(outermost_context_proxy->accessed_expressions().at(1), outermost_expression_b);

  ASSERT_EQ(intermediate_context_proxy->accessed_expressions().size(), 2u);
  EXPECT_EQ(intermediate_context_proxy->accessed_expressions().at(0), intermediate_expression_b);
  EXPECT_EQ(intermediate_context_proxy->accessed_expressions().at(1), intermediate_expression_a);
}

TEST_F(SQLIdentifierContextTest, GetExpressionIdentifier) {
  EXPECT_EQ(context.get_expression_identifier(expression_a), SQLIdentifier("a", "T1"));
  EXPECT_EQ(context.get_expression_identifier(expression_unnamed), std::nullopt);
}

TEST_F(SQLIdentifierContextTest, DeepEqualsIsUsed) {
  /**
   * Test that we can use equivalent Expression objects that are stored in different Objects
   */

  const auto expression_a2 = std::make_shared<SQLIdentifierExpression>(SQLIdentifier{"a", "T1"});
  context.set_column_name(expression_a2, "a2");
  context.set_table_name(expression_a2, "T2");
  EXPECT_EQ(context.resolve_identifier_relaxed({"a2"s, "T2"}), expression_a);
  EXPECT_EQ(context.get_expression_identifier(expression_a2), SQLIdentifier("a2"s, "T2"));
}

TEST_F(SQLIdentifierContextTest, ResolveTableName) {
  /**
   * Test that all Expressions of a table name can be found
   */

  const auto t1_expressions = std::vector<std::shared_ptr<AbstractExpression>>({expression_a, expression_b});
  const auto t2_expressions = std::vector<std::shared_ptr<AbstractExpression>>({expression_c});
  EXPECT_EQ(context.resolve_table_name("T1"), t1_expressions);
  EXPECT_EQ(context.resolve_table_name("T2"), t2_expressions);
}

}  // namespace opossum
