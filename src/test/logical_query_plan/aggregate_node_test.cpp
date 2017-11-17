#include <memory>
#include <string>
#include <vector>

#include "gtest/gtest.h"

#include "base_test.hpp"

#include "logical_query_plan/aggregate_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "optimizer/expression.hpp"
#include "storage/storage_manager.hpp"
#include "types.hpp"

namespace opossum {

class AggregateNodeTest : public BaseTest {
 protected:
  void SetUp() override {
    StorageManager::get().add_table("t_a", load_table("src/test/tables/int_int_int.tbl", Chunk::MAX_CHUNK_SIZE));

    _stored_table_node = std::make_shared<StoredTableNode>("t_a");

    // SELECT a, c, SUM(a+b), SUM(a+c) AS some_sum [...] GROUP BY a, c
    // Columns are ordered as specified in the SELECT list
    _aggregate_node = std::make_shared<AggregateNode>(
        std::vector<std::shared_ptr<Expression>>{
            Expression::create_aggregate_function(
                AggregateFunction::Sum,
                {Expression::create_binary_operator(ExpressionType::Addition, Expression::create_column(ColumnID{0}),
                                                    Expression::create_column(ColumnID{1}))}),
            Expression::create_aggregate_function(
                AggregateFunction::Sum,
                {Expression::create_binary_operator(ExpressionType::Addition, Expression::create_column(ColumnID{0}),
                                                    Expression::create_column(ColumnID{2}))},
                {std::string("some_sum")})},
        std::vector<ColumnID>{ColumnID{0}, ColumnID{2}});
    _aggregate_node->set_left_child(_stored_table_node);
  }

  void TearDown() override { StorageManager::get().reset(); }

  std::shared_ptr<StoredTableNode> _stored_table_node;
  std::shared_ptr<AggregateNode> _aggregate_node;
};

TEST_F(AggregateNodeTest, ColumnIdForColumnIdentifier) {
  EXPECT_EQ(_aggregate_node->get_column_id_by_named_column_reference({"a", std::nullopt}), 0);
  EXPECT_EQ(_aggregate_node->get_column_id_by_named_column_reference({"a", {"t_a"}}), 0);
  EXPECT_EQ(_aggregate_node->find_column_id_by_named_column_reference({"b", std::nullopt}), std::nullopt);
  EXPECT_EQ(_aggregate_node->find_column_id_by_named_column_reference({"b", {"t_a"}}), std::nullopt);
  EXPECT_EQ(_aggregate_node->get_column_id_by_named_column_reference({"c", std::nullopt}), 1);
  EXPECT_EQ(_aggregate_node->get_column_id_by_named_column_reference({"c", {"t_a"}}), 1);

  EXPECT_EQ(_aggregate_node->get_column_id_by_named_column_reference({"some_sum", std::nullopt}), 3);
  EXPECT_EQ(_aggregate_node->find_column_id_by_named_column_reference({"some_sum", {"t_a"}}), std::nullopt);
}

TEST_F(AggregateNodeTest, OriginalGroupByColumnIdsInOutputColumnIds) {
  const auto& column_ids = _aggregate_node->output_column_ids_to_input_column_ids();

  const auto iter_0 = std::find(column_ids.begin(), column_ids.end(), ColumnID{0});
  EXPECT_NE(iter_0, column_ids.end());
  EXPECT_EQ(std::distance(column_ids.begin(), iter_0), 0);

  const auto iter_1 = std::find(column_ids.begin(), column_ids.end(), ColumnID{1});
  EXPECT_EQ(iter_1, column_ids.end());

  const auto iter_2 = std::find(column_ids.begin(), column_ids.end(), ColumnID{2});
  EXPECT_NE(iter_2, column_ids.end());
  EXPECT_EQ(std::distance(column_ids.begin(), iter_2), 1);
}

TEST_F(AggregateNodeTest, ColumnIdForExpression) {
  EXPECT_EQ(_aggregate_node->get_column_id_for_expression(Expression::create_column(ColumnID{0})), 0);

  // "a+b" is not allowed
  EXPECT_THROW(
      _aggregate_node->get_column_id_for_expression(Expression::create_binary_operator(
          ExpressionType::Addition, Expression::create_column(ColumnID{0}), Expression::create_column(ColumnID{1}))),
      std::logic_error);

  // There is SUM(a+b)
  EXPECT_EQ(_aggregate_node->get_column_id_for_expression(Expression::create_aggregate_function(
                AggregateFunction::Sum,
                {Expression::create_binary_operator(ExpressionType::Addition, Expression::create_column(ColumnID{0}),
                                                    Expression::create_column(ColumnID{1}))})),
            2);

  // But there is no SUM(b+c)
  EXPECT_EQ(_aggregate_node->find_column_id_for_expression(Expression::create_aggregate_function(
                AggregateFunction::Sum,
                {Expression::create_binary_operator(ExpressionType::Addition, Expression::create_column(ColumnID{1}),
                                                    Expression::create_column(ColumnID{2}))})),
            std::nullopt);

  // TODO(mp): This expression is currently not found because the alias is missing.
  // This has to be fixed once expressions do not have an alias anymore.
  EXPECT_EQ(_aggregate_node->find_column_id_for_expression(Expression::create_aggregate_function(
                AggregateFunction::Sum,
                {Expression::create_binary_operator(ExpressionType::Addition, Expression::create_column(ColumnID{0}),
                                                    Expression::create_column(ColumnID{2}))})),
            std::nullopt);
}

TEST_F(AggregateNodeTest, AliasedSubqueryTest) {
  const auto aggregate_node_with_alias = std::make_shared<AggregateNode>(*_aggregate_node);
  aggregate_node_with_alias->set_alias(std::string("foo"));

  EXPECT_TRUE(aggregate_node_with_alias->knows_table("foo"));
  EXPECT_FALSE(aggregate_node_with_alias->knows_table("t_a"));

  EXPECT_EQ(aggregate_node_with_alias->get_column_id_by_named_column_reference({"a"}), ColumnID{0});
  EXPECT_EQ(aggregate_node_with_alias->get_column_id_by_named_column_reference({"a", {"foo"}}), ColumnID{0});
  EXPECT_EQ(aggregate_node_with_alias->find_column_id_by_named_column_reference({"a", {"t_a"}}), std::nullopt);
  EXPECT_EQ(aggregate_node_with_alias->get_column_id_by_named_column_reference({"some_sum", std::nullopt}),
            ColumnID{3});
  EXPECT_EQ(aggregate_node_with_alias->get_column_id_by_named_column_reference({"some_sum", {"foo"}}), ColumnID{3});
  EXPECT_EQ(aggregate_node_with_alias->find_column_id_by_named_column_reference({"some_sum", {"t_a"}}), std::nullopt);
}

TEST_F(AggregateNodeTest, Description) {
  auto description = _aggregate_node->description();

  EXPECT_EQ(description, "[Aggregate] SUM(t_a.a + t_a.b), SUM(t_a.a + t_a.c) AS \"some_sum\" GROUP BY [t_a.a, t_a.c]");
}

TEST_F(AggregateNodeTest, VerboseColumnNames) {
  EXPECT_EQ(_aggregate_node->get_verbose_column_name(ColumnID{0}), "SUM(t_a.a + t_a.b)");
  EXPECT_EQ(_aggregate_node->get_verbose_column_name(ColumnID{1}), "some_sum");
  EXPECT_EQ(_aggregate_node->get_verbose_column_name(ColumnID{2}), "t_a.a");
  EXPECT_EQ(_aggregate_node->get_verbose_column_name(ColumnID{3}), "t_a.c");
}

TEST_F(AggregateNodeTest, ColumnInputMapping) {
  auto column_ids = _aggregate_node->get_output_column_ids_for_table("t_a");

  // we are grouping by two columns
  EXPECT_EQ(column_ids.size(), 2u);

  EXPECT_EQ(column_ids[0], ColumnID{0});
  EXPECT_EQ(column_ids[1], ColumnID{1});
}

}  // namespace opossum
