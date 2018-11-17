#include <memory>

#include "gtest/gtest.h"

#include "expression/abstract_expression.hpp"
#include "expression/expression_functional.hpp"
#include "expression/lqp_column_expression.hpp"
#include "expression/parameter_expression.hpp"
#include "logical_query_plan/mock_node.hpp"
#include "sql/parameter_id_allocator.hpp"
#include "sql/sql_identifier_resolver.hpp"
#include "sql/sql_identifier_resolver_proxy.hpp"

using namespace std::string_literals;            // NOLINT
using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

class SQLIdentifierResolverTest : public ::testing::Test {
 public:
  void SetUp() override {
    node_a = MockNode::make(MockNode::ColumnDefinitions{
        {{DataType::Int, "a"}, {DataType::Int, "b"}, {DataType::Int, "c"}, {DataType::Int, "d"}}});
    node_b =
        MockNode::make(MockNode::ColumnDefinitions{{{DataType::Int, "a"}, {DataType::Int, "b"}, {DataType::Int, "c"}}});
    node_c =
        MockNode::make(MockNode::ColumnDefinitions{{{DataType::Int, "a"}, {DataType::Int, "b"}, {DataType::Int, "c"}}});

    expression_a = std::make_shared<LQPColumnExpression>(LQPColumnReference(node_a, ColumnID{0}));
    expression_b = std::make_shared<LQPColumnExpression>(LQPColumnReference(node_a, ColumnID{1}));
    expression_c = std::make_shared<LQPColumnExpression>(LQPColumnReference(node_a, ColumnID{2}));
    expression_unnamed = std::make_shared<LQPColumnExpression>(LQPColumnReference(node_a, ColumnID{3}));

    context.set_column_name(expression_a, {"a"s});
    context.set_column_name(expression_b, {"b"s});
    context.set_column_name(expression_c, {"c"s});
    context.set_table_name(expression_a, {"T1"s});
    context.set_table_name(expression_b, {"T1"s});
    context.set_table_name(expression_c, {"T2"s});

    parameter_id_allocator = std::make_shared<ParameterIDAllocator>();
  }

  std::shared_ptr<MockNode> node_a, node_b, node_c;
  std::shared_ptr<AbstractExpression> expression_a, expression_b, expression_c, expression_unnamed;
  SQLIdentifierResolver context;
  std::shared_ptr<ParameterIDAllocator> parameter_id_allocator;
};

TEST_F(SQLIdentifierResolverTest, ResolveIdentifier) {
  EXPECT_EQ(context.resolve_identifier_relaxed({"a"s}), expression_a);
  EXPECT_EQ(context.resolve_identifier_relaxed({"b"s}), expression_b);
  EXPECT_EQ(context.resolve_identifier_relaxed({"c"s}), expression_c);
  EXPECT_EQ(context.resolve_identifier_relaxed({"a"s, "T1"}), expression_a);
  EXPECT_EQ(context.resolve_identifier_relaxed({"a"s, "T2"}), nullptr);
  EXPECT_EQ(context.resolve_identifier_relaxed({"x"s}), nullptr);
  EXPECT_EQ(context.resolve_identifier_relaxed({"x"s, "T1"}), nullptr);
}

TEST_F(SQLIdentifierResolverTest, ColumnNameChanges) {
  context.set_column_name(expression_a, "x");

  EXPECT_EQ(context.resolve_identifier_relaxed({"a"s}), nullptr);
  EXPECT_EQ(context.resolve_identifier_relaxed({"a"s, "T1"}), nullptr);
  EXPECT_EQ(context.resolve_identifier_relaxed({"x"s, "T1"}), expression_a);
  EXPECT_EQ(context.resolve_identifier_relaxed({"x"s}), expression_a);

  EXPECT_EQ(context.resolve_identifier_relaxed({"b"s}), expression_b);
}

TEST_F(SQLIdentifierResolverTest, TableNameChanges) {
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

TEST_F(SQLIdentifierResolverTest, ColumnNameRedundancy) {
  auto expression_a2 = std::make_shared<LQPColumnExpression>(LQPColumnReference(node_c, ColumnID{2}));

  context.set_column_name(expression_a2, {"a"s});

  // "a" is ambiguous now
  EXPECT_EQ(context.resolve_identifier_relaxed({"a"s}), nullptr);

  EXPECT_EQ(context.resolve_identifier_relaxed({"a"s, "T1"}), expression_a);

  context.set_table_name(expression_a2, "T2");
  EXPECT_EQ(context.resolve_identifier_relaxed({"a"s, "T2"}), expression_a2);
}

TEST_F(SQLIdentifierResolverTest, ResolveOuterExpression) {
  /**
   * Simulate a scenario in which a Sub-Subquery accesses an Identifier from the outermost and intermediate queries
   */

  /**
   * Create context and context proxy for the outermost query
   */
  const auto outermost_expression_a = std::make_shared<LQPColumnExpression>(LQPColumnReference(node_b, ColumnID{0}));
  const auto outermost_expression_b = std::make_shared<LQPColumnExpression>(LQPColumnReference(node_b, ColumnID{1}));
  const auto outermost_expression_c = std::make_shared<LQPColumnExpression>(LQPColumnReference(node_b, ColumnID{2}));
  const auto outermost_context = std::make_shared<SQLIdentifierResolver>();
  outermost_context->set_column_name(outermost_expression_a, "outermost_a");
  outermost_context->set_column_name(outermost_expression_b, "b");  // Intentionally named just "b"
  outermost_context->set_column_name(outermost_expression_c, "c");  // Intentionally named just "c"
  outermost_context->set_table_name(outermost_expression_b, "Outermost");

  const auto outermost_context_proxy =
      std::make_shared<SQLIdentifierResolverProxy>(outermost_context, parameter_id_allocator);

  /**
   * Create context and context proxy for the nested ("intermediate") query
   */
  auto intermediate_context = std::make_shared<SQLIdentifierResolver>();
  const auto intermediate_expression_a = std::make_shared<LQPColumnExpression>(LQPColumnReference(node_c, ColumnID{0}));
  const auto intermediate_expression_b = std::make_shared<LQPColumnExpression>(LQPColumnReference(node_c, ColumnID{1}));
  intermediate_context->set_column_name(intermediate_expression_a, "intermediate_a");
  intermediate_context->set_column_name(intermediate_expression_b, "b");  // Intentionally named just "b"
  intermediate_context->set_table_name(intermediate_expression_b, "Intermediate");

  const auto intermediate_context_proxy = std::make_shared<SQLIdentifierResolverProxy>(
      intermediate_context, parameter_id_allocator, outermost_context_proxy);

  /**
   * Test whether identifiers are resolved correctly
   */
  EXPECT_EQ(context.resolve_identifier_relaxed({"a"}), expression_a);
  EXPECT_EQ(context.resolve_identifier_relaxed({"b"}), expression_b);
  EXPECT_EQ(context.resolve_identifier_relaxed({"b", "T1"}), expression_b);

  EXPECT_EQ(*intermediate_context_proxy->resolve_identifier_relaxed({"b", "Intermediate"}),
            *correlated_parameter_(ParameterID{0}, intermediate_expression_b));
  EXPECT_EQ(*intermediate_context_proxy->resolve_identifier_relaxed({"intermediate_a"}),
            *correlated_parameter_(ParameterID{1}, intermediate_expression_a));
  EXPECT_EQ(*intermediate_context_proxy->resolve_identifier_relaxed({"b"}),
            *correlated_parameter_(ParameterID{0}, intermediate_expression_b));
  EXPECT_EQ(intermediate_context_proxy->resolve_identifier_relaxed({"intermediate_a", "Intermediate"}), nullptr);

  EXPECT_EQ(*intermediate_context_proxy->resolve_identifier_relaxed({"outermost_a"}),
            *correlated_parameter_(ParameterID{2}, outermost_expression_a));
  EXPECT_EQ(*intermediate_context_proxy->resolve_identifier_relaxed({"b", "Outermost"}),
            *correlated_parameter_(ParameterID{3}, outermost_expression_b));

  /**
   * Test whether the proxies tracked accesses to their contexts correctly
   */
  ASSERT_EQ(outermost_context_proxy->accessed_expressions().size(), 2u);
  EXPECT_EQ(outermost_context_proxy->accessed_expressions().count(outermost_expression_a), 1u);
  EXPECT_EQ(outermost_context_proxy->accessed_expressions().count(outermost_expression_b), 1u);

  ASSERT_EQ(intermediate_context_proxy->accessed_expressions().size(), 2u);
  EXPECT_EQ(intermediate_context_proxy->accessed_expressions().count(intermediate_expression_b), 1u);
  EXPECT_EQ(intermediate_context_proxy->accessed_expressions().count(intermediate_expression_a), 1u);
}

TEST_F(SQLIdentifierResolverTest, GetExpressionIdentifier) {
  EXPECT_EQ(context.get_expression_identifier(expression_a), SQLIdentifier("a", "T1"));
  EXPECT_EQ(context.get_expression_identifier(expression_unnamed), std::nullopt);
}

TEST_F(SQLIdentifierResolverTest, DeepEqualsIsUsed) {
  /**
   * Test that we can use equivalent Expression objects that are stored in different Objects
   */

  const auto expression_a2 = std::make_shared<LQPColumnExpression>(LQPColumnReference(node_a, ColumnID{0}));
  context.set_column_name(expression_a2, "a2");
  context.set_table_name(expression_a2, "T2");
  EXPECT_EQ(context.resolve_identifier_relaxed({"a2"s, "T2"}), expression_a);
  EXPECT_EQ(context.get_expression_identifier(expression_a2), SQLIdentifier("a2"s, "T2"));
}

TEST_F(SQLIdentifierResolverTest, ResolveTableName) {
  /**
   * Test that all Expressions of a table name can be found
   */

  const auto t1_expressions = std::vector<std::shared_ptr<AbstractExpression>>({expression_a, expression_b});
  const auto t2_expressions = std::vector<std::shared_ptr<AbstractExpression>>({expression_c});
  EXPECT_EQ(context.resolve_table_name("T1"), t1_expressions);
  EXPECT_EQ(context.resolve_table_name("T2"), t2_expressions);
}

}  // namespace opossum
