#include <boost/algorithm/string.hpp>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "../base_test.hpp"
#include "SQLParser.h"
#include "gtest/gtest.h"

#include "constant_mappings.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/aggregate_node.hpp"
#include "logical_query_plan/create_view_node.hpp"
#include "logical_query_plan/drop_view_node.hpp"
#include "logical_query_plan/insert_node.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/limit_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/projection_node.hpp"
#include "logical_query_plan/sort_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "logical_query_plan/update_node.hpp"
#include "sql/sql_translator.hpp"
#include "storage/storage_manager.hpp"

namespace opossum {

class SQLTranslatorTest : public BaseTest {
 protected:
  void SetUp() override {
    std::shared_ptr<Table> table_a = load_table("src/test/tables/int_float.tbl", 2);
    StorageManager::get().add_table("table_a", std::move(table_a));

    std::shared_ptr<Table> table_b = load_table("src/test/tables/int_float2.tbl", 2);
    StorageManager::get().add_table("table_b", std::move(table_b));

    std::shared_ptr<Table> table_c = load_table("src/test/tables/int_float5.tbl", 2);
    StorageManager::get().add_table("table_c", std::move(table_c));
  }

  std::shared_ptr<AbstractLQPNode> compile_query(const std::string& query) {
    hsql::SQLParserResult parse_result;
    hsql::SQLParser::parseSQLString(query, &parse_result);

    if (!parse_result.isValid()) {
      throw std::runtime_error("Query is not valid.");
    }

    return SQLTranslator{false}.translate_parse_result(parse_result)[0];
  }
};

TEST_F(SQLTranslatorTest, SelectStarAllTest) {
  const auto query = "SELECT * FROM table_a;";
  auto result_node = compile_query(query);

  std::vector<ColumnID> expected_columns{ColumnID{0}, ColumnID{1}};
  EXPECT_EQ(expected_columns, result_node->output_column_ids_to_input_column_ids());

  EXPECT_EQ(result_node->type(), LQPNodeType::Projection);

  EXPECT_TRUE(result_node->left_child());
  EXPECT_EQ(result_node->left_child()->type(), LQPNodeType::StoredTable);

  EXPECT_FALSE(result_node->right_child());
  EXPECT_FALSE(result_node->left_child()->left_child());
}

/*
 * Disabled because: Opossums's Expressions are able to handle this kind of expression. However, a PredicateNode needs
 * the parsed expression as input. And it does not support nested Expressions, such as '1234 + 1'.
 * This is why this test is currently not supported. It will be enabled once we are able to parse these expressions
 * in the translator.
 */
TEST_F(SQLTranslatorTest, DISABLED_ExpressionTest /* #494 */) {
  const auto query = "SELECT * FROM table_a WHERE a = 1234 + 1";
  auto result_node = compile_query(query);

  EXPECT_EQ(result_node->type(), LQPNodeType::Projection);
  EXPECT_FALSE(result_node->right_child());

  auto ts_node_1 = std::dynamic_pointer_cast<PredicateNode>(result_node->left_child());
  EXPECT_EQ(ts_node_1->type(), LQPNodeType::Predicate);
  EXPECT_FALSE(ts_node_1->right_child());
  EXPECT_EQ(ts_node_1->column_id(), ColumnID{0});
  EXPECT_EQ(ts_node_1->scan_type(), ScanType::Equals);
  // TODO(anybody): once this is implemented, the value side has to be checked.
}

TEST_F(SQLTranslatorTest, TwoColumnFilter) {
  const auto query = "SELECT * FROM table_a WHERE a = \"b\"";
  auto result_node = compile_query(query);

  EXPECT_EQ(result_node->type(), LQPNodeType::Projection);
  EXPECT_FALSE(result_node->right_child());

  auto ts_node_1 = std::static_pointer_cast<PredicateNode>(result_node->left_child());
  EXPECT_EQ(ts_node_1->type(), LQPNodeType::Predicate);
  EXPECT_FALSE(ts_node_1->right_child());
  EXPECT_EQ(ts_node_1->scan_type(), ScanType::Equals);
  EXPECT_EQ(ts_node_1->column_id(), ColumnID{0});
  EXPECT_EQ(ts_node_1->value(), AllParameterVariant{ColumnID{1}});
}

TEST_F(SQLTranslatorTest, ExpressionStringTest) {
  const auto query = "SELECT * FROM table_a WHERE a = 'b'";
  auto result_node = compile_query(query);

  EXPECT_EQ(result_node->type(), LQPNodeType::Projection);
  EXPECT_FALSE(result_node->right_child());

  auto ts_node_1 = std::static_pointer_cast<PredicateNode>(result_node->left_child());
  EXPECT_EQ(ts_node_1->type(), LQPNodeType::Predicate);
  EXPECT_FALSE(ts_node_1->right_child());
  EXPECT_EQ(ts_node_1->column_id(), ColumnID{0});
  EXPECT_EQ(ts_node_1->scan_type(), ScanType::Equals);
  EXPECT_EQ(ts_node_1->value(), AllParameterVariant{std::string{"b"}});
}

TEST_F(SQLTranslatorTest, SelectWithAndCondition) {
  const auto query = "SELECT * FROM table_a WHERE a >= 1234 AND b < 457.9";
  auto result_node = compile_query(query);

  EXPECT_EQ(result_node->type(), LQPNodeType::Projection);
  EXPECT_FALSE(result_node->right_child());

  auto ts_node_1 = result_node->left_child();
  EXPECT_EQ(ts_node_1->type(), LQPNodeType::Predicate);
  EXPECT_FALSE(ts_node_1->right_child());

  auto ts_node_2 = ts_node_1->left_child();
  EXPECT_EQ(ts_node_2->type(), LQPNodeType::Predicate);
  EXPECT_FALSE(ts_node_2->right_child());

  auto t_node = ts_node_2->left_child();
  EXPECT_EQ(t_node->type(), LQPNodeType::StoredTable);
  EXPECT_FALSE(t_node->left_child());
  EXPECT_FALSE(t_node->right_child());
}

TEST_F(SQLTranslatorTest, AggregateWithGroupBy) {
  const auto query = "SELECT a, SUM(b) AS s FROM table_a GROUP BY a;";
  const auto result_node = compile_query(query);

  EXPECT_EQ(result_node->type(), LQPNodeType::Projection);
  EXPECT_FALSE(result_node->right_child());

  const auto projection_node = std::dynamic_pointer_cast<ProjectionNode>(result_node);
  EXPECT_NE(projection_node, nullptr);
  EXPECT_EQ(projection_node->column_expressions().size(), 2u);
  EXPECT_EQ(projection_node->output_column_names().size(), 2u);
  EXPECT_EQ(projection_node->output_column_names()[0], std::string("a"));
  EXPECT_EQ(projection_node->output_column_names()[1], std::string("s"));

  const auto aggregate_node = std::dynamic_pointer_cast<AggregateNode>(result_node->left_child());
  EXPECT_NE(aggregate_node, nullptr);
  EXPECT_EQ(aggregate_node->aggregate_expressions().size(), 1u);
  const std::vector<ColumnID> groupby_columns = {ColumnID{0}};
  EXPECT_EQ(aggregate_node->groupby_column_ids(), groupby_columns);
  EXPECT_EQ(aggregate_node->aggregate_expressions().at(0)->alias(), std::string("s"));

  auto t_node_1 = aggregate_node->left_child();
  EXPECT_EQ(t_node_1->type(), LQPNodeType::StoredTable);
  EXPECT_FALSE(t_node_1->left_child());
  EXPECT_FALSE(t_node_1->right_child());
}

TEST_F(SQLTranslatorTest, AggregateWithInvalidGroupBy) {
  // Cannot select b without it being in the GROUP BY clause.
  const auto query = "SELECT b, SUM(b) AS s FROM table_a GROUP BY a;";
  EXPECT_THROW(compile_query(query), std::logic_error);
}

TEST_F(SQLTranslatorTest, AggregateWithExpression) {
  const auto query = "SELECT SUM(a+b) AS s, SUM(a*b) as f FROM table_a";
  const auto result_node = compile_query(query);

  EXPECT_EQ(result_node->type(), LQPNodeType::Projection);
  EXPECT_FALSE(result_node->right_child());

  const auto projection_node = std::dynamic_pointer_cast<ProjectionNode>(result_node);
  EXPECT_NE(projection_node, nullptr);
  EXPECT_EQ(projection_node->column_expressions().size(), 2u);
  EXPECT_EQ(projection_node->output_column_names().size(), 2u);
  EXPECT_EQ(projection_node->output_column_names()[0], std::string("s"));
  EXPECT_EQ(projection_node->output_column_names()[1], std::string("f"));

  const auto aggregate_node = std::dynamic_pointer_cast<AggregateNode>(result_node->left_child());
  EXPECT_EQ(aggregate_node->aggregate_expressions().size(), 2u);
  EXPECT_EQ(aggregate_node->groupby_column_ids().size(), 0u);
  EXPECT_EQ(aggregate_node->aggregate_expressions().at(0)->alias(), std::string("s"));
  EXPECT_EQ(aggregate_node->aggregate_expressions().at(1)->alias(), std::string("f"));

  auto t_node_1 = aggregate_node->left_child();
  EXPECT_EQ(t_node_1->type(), LQPNodeType::StoredTable);
  EXPECT_FALSE(t_node_1->left_child());
  EXPECT_FALSE(t_node_1->right_child());
}

TEST_F(SQLTranslatorTest, AggregateWithCountDistinct) {
  const auto query = "SELECT a, COUNT(DISTINCT b) AS s FROM table_a GROUP BY a;";
  const auto result_node = compile_query(query);

  EXPECT_EQ(result_node->type(), LQPNodeType::Projection);
  EXPECT_FALSE(result_node->right_child());

  const auto projection_node = std::dynamic_pointer_cast<ProjectionNode>(result_node);
  EXPECT_NE(projection_node, nullptr);
  EXPECT_EQ(projection_node->column_expressions().size(), 2u);
  EXPECT_EQ(projection_node->output_column_names().size(), 2u);
  EXPECT_EQ(projection_node->output_column_names()[0], std::string("a"));
  EXPECT_EQ(projection_node->output_column_names()[1], std::string("s"));

  const auto aggregate_node = std::dynamic_pointer_cast<AggregateNode>(result_node->left_child());
  EXPECT_NE(aggregate_node, nullptr);
  EXPECT_EQ(aggregate_node->aggregate_expressions().size(), 1u);
  const std::vector<ColumnID> groupby_columns = {ColumnID{0}};
  EXPECT_EQ(aggregate_node->groupby_column_ids(), groupby_columns);
  EXPECT_EQ(aggregate_node->aggregate_expressions().at(0)->alias(), std::string("s"));
  EXPECT_EQ(aggregate_node->aggregate_expressions().at(0)->aggregate_function(), AggregateFunction::CountDistinct);

  auto t_node_1 = aggregate_node->left_child();
  EXPECT_EQ(t_node_1->type(), LQPNodeType::StoredTable);
  EXPECT_FALSE(t_node_1->left_child());
  EXPECT_FALSE(t_node_1->right_child());
}

TEST_F(SQLTranslatorTest, SelectMultipleOrderBy) {
  const auto query = "SELECT * FROM table_a ORDER BY a DESC, b ASC;";
  auto result_node = compile_query(query);

  auto sort_node = std::dynamic_pointer_cast<SortNode>(result_node);
  EXPECT_EQ(sort_node->type(), LQPNodeType::Sort);

  const auto& order_by_definitions = sort_node->order_by_definitions();
  EXPECT_EQ(order_by_definitions.size(), 2u);

  const auto& definition_1 = order_by_definitions[0];
  EXPECT_EQ(definition_1.column_id, ColumnID{0});
  EXPECT_EQ(definition_1.order_by_mode, OrderByMode::Descending);

  const auto& definition_2 = order_by_definitions[1];
  EXPECT_EQ(definition_2.column_id, ColumnID{1});
  EXPECT_EQ(definition_2.order_by_mode, OrderByMode::Ascending);

  // The sort node has an input node, but we don't care what kind it is in this test.
  EXPECT_TRUE(sort_node->left_child());
  EXPECT_FALSE(sort_node->right_child());
}

TEST_F(SQLTranslatorTest, SelectInnerJoin) {
  const auto query = "SELECT * FROM table_a AS a INNER JOIN table_b AS b ON a.a = b.a;";
  auto result_node = compile_query(query);

  EXPECT_EQ(result_node->type(), LQPNodeType::Projection);
  auto projection_node = std::dynamic_pointer_cast<ProjectionNode>(result_node);
  EXPECT_EQ(projection_node->output_column_count(), 4u);
  std::vector<std::string> output_columns = {"a", "b", "a", "b"};
  EXPECT_EQ(projection_node->output_column_names(), output_columns);

  EXPECT_EQ(result_node->left_child()->type(), LQPNodeType::Join);
  auto join_node = std::dynamic_pointer_cast<JoinNode>(result_node->left_child());
  EXPECT_EQ(join_node->scan_type(), ScanType::Equals);
  EXPECT_EQ(join_node->join_mode(), JoinMode::Inner);
  EXPECT_EQ((*join_node->join_column_ids()).first, ColumnID{0});
  EXPECT_EQ((*join_node->join_column_ids()).second, ColumnID{0});
}

// Verifies that LEFT/RIGHT JOIN are handled correctly and LEFT/RIGHT OUTER JOIN identically
TEST_F(SQLTranslatorTest, SelectLeftRightOuterJoins) {
  using namespace std::string_literals;  // NOLINT (Linter does not know about using namespace)

  for (auto mode : {JoinMode::Left, JoinMode::Right}) {
    std::string mode_str = boost::to_upper_copy(join_mode_to_string.at(mode));
    const auto query = "SELECT * FROM table_a AS a "s + mode_str + " JOIN table_b AS b ON a.a = b.a;";
    auto result_node = compile_query(query);

    EXPECT_EQ(result_node->type(), LQPNodeType::Projection);
    auto projection_node = std::dynamic_pointer_cast<ProjectionNode>(result_node);
    std::vector<std::string> output_columns = {"a", "b", "a", "b"};
    EXPECT_EQ(projection_node->output_column_names(), output_columns);

    EXPECT_EQ(result_node->left_child()->type(), LQPNodeType::Join);
    auto join_node = std::dynamic_pointer_cast<JoinNode>(result_node->left_child());
    EXPECT_EQ(join_node->scan_type(), ScanType::Equals);
    EXPECT_EQ(join_node->join_mode(), mode);
    EXPECT_EQ((*join_node->join_column_ids()).first, ColumnID{0} /* "a" */);
    EXPECT_EQ((*join_node->join_column_ids()).second, ColumnID{0} /* "a" */);

    // "OUTER" should be translated the exact same way
    const auto query_outer = "SELECT * FROM table_a AS a "s + mode_str + " OUTER JOIN table_b AS b ON a.a = b.a;";
    auto result_node_outer = compile_query(query_outer);

    EXPECT_EQ(result_node_outer->type(), LQPNodeType::Projection);
    auto projection_node_outer = std::dynamic_pointer_cast<ProjectionNode>(result_node_outer);
    std::vector<std::string> output_columns_outer = {"a", "b", "a", "b"};
    EXPECT_EQ(projection_node_outer->output_column_names(), output_columns_outer);

    EXPECT_EQ(result_node_outer->left_child()->type(), LQPNodeType::Join);
    auto join_node_outer = std::dynamic_pointer_cast<JoinNode>(result_node_outer->left_child());
    EXPECT_EQ(join_node_outer->scan_type(), join_node->scan_type());
    EXPECT_EQ(join_node_outer->join_mode(), join_node->join_mode());
    EXPECT_EQ((*join_node_outer->join_column_ids()).first, (*join_node->join_column_ids()).first);
    EXPECT_EQ((*join_node_outer->join_column_ids()).second, (*join_node->join_column_ids()).second);
  }
}

TEST_F(SQLTranslatorTest, SelectOuterJoin) {
  const auto query = "SELECT * FROM table_a AS a OUTER JOIN table_b AS b ON a.a = b.a;";
  auto result_node = compile_query(query);

  EXPECT_EQ(result_node->type(), LQPNodeType::Projection);
  auto projection_node = std::dynamic_pointer_cast<ProjectionNode>(result_node);
  std::vector<std::string> output_columns = {"a", "b", "a", "b"};
  EXPECT_EQ(projection_node->output_column_names(), output_columns);

  EXPECT_EQ(result_node->left_child()->type(), LQPNodeType::Join);
  auto join_node = std::dynamic_pointer_cast<JoinNode>(result_node->left_child());
  EXPECT_EQ(join_node->scan_type(), ScanType::Equals);
  EXPECT_EQ(join_node->join_mode(), JoinMode::Outer);
  EXPECT_EQ((*join_node->join_column_ids()).first, ColumnID{0} /* "a" */);
  EXPECT_EQ((*join_node->join_column_ids()).second, ColumnID{0} /* "a" */);
}

TEST_F(SQLTranslatorTest, SelectNaturalJoin) {
  const auto query = "SELECT * FROM table_a NATURAL JOIN table_c;";
  auto result_node = compile_query(query);

  // skip the projection that is always inserted on the top
  result_node = result_node->left_child();

  EXPECT_EQ(result_node->type(), LQPNodeType::Projection);
  auto projection_node = std::dynamic_pointer_cast<ProjectionNode>(result_node);
  std::vector<std::string> output_columns = {"a", "b", "d"};
  EXPECT_EQ(projection_node->output_column_names(), output_columns);

  EXPECT_EQ(projection_node->left_child()->type(), LQPNodeType::Predicate);
  auto predicate = std::dynamic_pointer_cast<PredicateNode>(projection_node->left_child());
  EXPECT_FALSE(predicate->right_child());
  EXPECT_EQ(predicate->column_id(), ColumnID{0});
  EXPECT_EQ(predicate->scan_type(), ScanType::Equals);
  EXPECT_EQ(predicate->value(), AllParameterVariant{ColumnID{2}});

  EXPECT_EQ(predicate->left_child()->type(), LQPNodeType::Join);
  auto cross = std::dynamic_pointer_cast<JoinNode>(predicate->left_child());
  EXPECT_EQ(cross->join_mode(), JoinMode::Cross);
  EXPECT_EQ(cross->left_child()->type(), LQPNodeType::StoredTable);
  EXPECT_EQ(cross->right_child()->type(), LQPNodeType::StoredTable);
}

TEST_F(SQLTranslatorTest, SelectCrossJoin) {
  const auto query = "SELECT * FROM table_a AS a, table_b AS b WHERE a.a = b.a;";
  auto result_node = compile_query(query);

  EXPECT_EQ(result_node->type(), LQPNodeType::Projection);
  auto projection_node = std::dynamic_pointer_cast<ProjectionNode>(result_node);
  std::vector<std::string> output_columns = {"a", "b", "a", "b"};
  EXPECT_EQ(projection_node->output_column_names(), output_columns);

  EXPECT_EQ(result_node->left_child()->type(), LQPNodeType::Predicate);
  EXPECT_EQ(result_node->left_child()->left_child()->type(), LQPNodeType::Join);
  auto join_node = std::dynamic_pointer_cast<JoinNode>(result_node->left_child()->left_child());
  EXPECT_FALSE(join_node->scan_type());
  EXPECT_EQ(join_node->join_mode(), JoinMode::Cross);
}

TEST_F(SQLTranslatorTest, SelectLimit) {
  const auto query = "SELECT * FROM table_a LIMIT 2;";
  auto result_node = compile_query(query);

  EXPECT_EQ(result_node->type(), LQPNodeType::Limit);
  auto limit_node = std::dynamic_pointer_cast<LimitNode>(result_node);
  EXPECT_EQ(limit_node->num_rows(), 2u);
  EXPECT_EQ(limit_node->left_child()->type(), LQPNodeType::Projection);
}

TEST_F(SQLTranslatorTest, InsertValues) {
  const auto query = "INSERT INTO table_a VALUES (10, 12.5);";
  auto result_node = compile_query(query);

  EXPECT_EQ(result_node->type(), LQPNodeType::Insert);
  auto insert_node = std::dynamic_pointer_cast<InsertNode>(result_node);
  EXPECT_EQ(insert_node->table_name(), "table_a");
  EXPECT_EQ(insert_node->left_child()->type(), LQPNodeType::Projection);

  auto projection = std::dynamic_pointer_cast<ProjectionNode>(insert_node->left_child());
  EXPECT_NE(projection, nullptr);

  auto expressions = projection->column_expressions();
  EXPECT_EQ(expressions[0]->type(), ExpressionType::Literal);
  EXPECT_EQ(boost::get<int32_t>(expressions[0]->value()), 10);
  EXPECT_EQ(expressions[1]->type(), ExpressionType::Literal);
  EXPECT_EQ(boost::get<float>(expressions[1]->value()), 12.5);

  EXPECT_EQ(projection->left_child()->type(), LQPNodeType::DummyTable);
}

TEST_F(SQLTranslatorTest, InsertValuesColumnReorder) {
  const auto query = "INSERT INTO table_a (b, a) VALUES (10, 12.5);";
  auto result_node = compile_query(query);

  EXPECT_EQ(result_node->type(), LQPNodeType::Insert);
  auto insert_node = std::dynamic_pointer_cast<InsertNode>(result_node);
  EXPECT_EQ(insert_node->table_name(), "table_a");
  EXPECT_EQ(insert_node->left_child()->type(), LQPNodeType::Projection);

  auto projection = std::dynamic_pointer_cast<ProjectionNode>(insert_node->left_child());
  EXPECT_NE(projection, nullptr);

  auto expressions = projection->column_expressions();
  EXPECT_EQ(expressions[0]->type(), ExpressionType::Literal);
  EXPECT_EQ(boost::get<float>(expressions[0]->value()), 12.5);
  EXPECT_EQ(expressions[1]->type(), ExpressionType::Literal);
  EXPECT_EQ(boost::get<int32_t>(expressions[1]->value()), 10);

  EXPECT_EQ(projection->left_child()->type(), LQPNodeType::DummyTable);
}

TEST_F(SQLTranslatorTest, InsertValuesIncompleteColumns) {
  const auto query = "INSERT INTO table_a (a) VALUES (10);";
  auto result_node = compile_query(query);

  EXPECT_EQ(result_node->type(), LQPNodeType::Insert);
  auto insert_node = std::dynamic_pointer_cast<InsertNode>(result_node);
  EXPECT_EQ(insert_node->table_name(), "table_a");
  EXPECT_EQ(insert_node->left_child()->type(), LQPNodeType::Projection);

  auto projection = std::dynamic_pointer_cast<ProjectionNode>(insert_node->left_child());
  EXPECT_NE(projection, nullptr);

  auto expressions = projection->column_expressions();
  EXPECT_EQ(expressions[0]->type(), ExpressionType::Literal);
  EXPECT_EQ(boost::get<int32_t>(expressions[0]->value()), 10);
  EXPECT_TRUE(expressions[1]->is_null_literal());

  EXPECT_EQ(projection->left_child()->type(), LQPNodeType::DummyTable);
}

TEST_F(SQLTranslatorTest, InsertSubquery) {
  const auto query = "INSERT INTO table_a SELECT a, b FROM table_b;";
  auto result_node = compile_query(query);

  EXPECT_EQ(result_node->type(), LQPNodeType::Insert);
  auto insert_node = std::dynamic_pointer_cast<InsertNode>(result_node);
  EXPECT_EQ(insert_node->table_name(), "table_a");
  EXPECT_EQ(insert_node->left_child()->type(), LQPNodeType::Projection);

  auto projection = std::dynamic_pointer_cast<ProjectionNode>(insert_node->left_child());
  EXPECT_NE(projection, nullptr);

  auto expressions = projection->column_expressions();
  EXPECT_EQ(expressions[0]->type(), ExpressionType::Column);
  EXPECT_EQ(expressions[0]->column_id(), ColumnID{0});
  EXPECT_EQ(expressions[1]->type(), ExpressionType::Column);
  EXPECT_EQ(expressions[1]->column_id(), ColumnID{1});
}

TEST_F(SQLTranslatorTest, Update) {
  const auto query = "UPDATE table_a SET b = 3.2 WHERE a > 1;";
  auto result_node = compile_query(query);

  EXPECT_EQ(result_node->type(), LQPNodeType::Update);
  auto update_node = std::dynamic_pointer_cast<UpdateNode>(result_node);
  EXPECT_EQ(update_node->table_name(), "table_a");
  EXPECT_EQ(update_node->left_child()->type(), LQPNodeType::Predicate);

  auto expressions = update_node->column_expressions();
  EXPECT_EQ(expressions[0]->type(), ExpressionType::Column);
  EXPECT_EQ(expressions[0]->column_id(), ColumnID{0});
  EXPECT_EQ(expressions[1]->type(), ExpressionType::Literal);
  EXPECT_FLOAT_EQ(boost::get<float>(expressions[1]->value()), 3.2);
  EXPECT_TRUE(expressions[1]->alias());
  EXPECT_EQ(*expressions[1]->alias(), "b");
}

TEST_F(SQLTranslatorTest, MixedAggregateAndGroupBySelectList) {
  /**
   * Test:
   *    - Select list can contain both GroupBy and Aggregate Columns in any order
   *    - The order of GroupBy Columns in the SelectList can differ from the order in the GroupByList
   */

  const auto query = R"(SELECT
                          table_b.a, SUM(table_c.a), table_a.b, table_b.b
                        FROM
                          table_a, table_b, table_c
                        GROUP BY
                          table_a.b, table_a.a, table_b.a, table_b.b)";

  /**
   * Expect this AST:
   *
   * [Projection] table_a.a, SUM(table_c.a), table_a.b, table_a.b
   *  |_[Aggregate] SUM(table_c.a) GROUP BY [table_a.b, table_a.a, table_b.a, table_b.b]
   *     |_[Cross Join]
   *        |_[Cross Join]
   *         |  |_[StoredTable] Name: 'table_b'
   *         |  |_[StoredTable] Name: 'table_c'
   *         |_[StoredTable] Name: 'table_a'
   */

  auto result = compile_query(query);

  /**
   * Assert the Projection
   */
  ASSERT_EQ(result->type(), LQPNodeType::Projection);
  const auto projection_node = std::dynamic_pointer_cast<ProjectionNode>(result);
  ASSERT_EQ(projection_node->column_expressions().size(), 4u);
  ASSERT_COLUMN_EXPRESSION(projection_node->column_expressions()[0], ColumnID{2});
  ASSERT_COLUMN_EXPRESSION(projection_node->column_expressions()[1], ColumnID{4});
  ASSERT_COLUMN_EXPRESSION(projection_node->column_expressions()[2], ColumnID{0});
  ASSERT_COLUMN_EXPRESSION(projection_node->column_expressions()[3], ColumnID{3});

  /**
   * Assert the Aggregate
   */
  ASSERT_EQ(projection_node->left_child()->type(), LQPNodeType::Aggregate);
  const auto aggregate_node = std::dynamic_pointer_cast<AggregateNode>(projection_node->left_child());
  ASSERT_EQ(aggregate_node->groupby_column_ids().size(), 4u);
  EXPECT_EQ(aggregate_node->groupby_column_ids()[0], ColumnID{5});
  EXPECT_EQ(aggregate_node->groupby_column_ids()[1], ColumnID{4});
  EXPECT_EQ(aggregate_node->groupby_column_ids()[2], ColumnID{0});
  EXPECT_EQ(aggregate_node->groupby_column_ids()[3], ColumnID{1});
  ASSERT_EQ(aggregate_node->aggregate_expressions().size(), 1u);
  const auto sum_expression = aggregate_node->aggregate_expressions()[0];
  ASSERT_EQ(sum_expression->type(), ExpressionType::Function);
  ASSERT_AGGREGATE_FUNCTION_EXPRESSION(sum_expression, AggregateFunction::Sum, ColumnID{2});

  /**
   * Assert rest of the AST
   */
  ASSERT_CROSS_JOIN_NODE(aggregate_node->left_child());
  ASSERT_CROSS_JOIN_NODE(aggregate_node->left_child()->left_child());
  ASSERT_STORED_TABLE_NODE(aggregate_node->left_child()->left_child()->left_child(), "table_b");
  ASSERT_STORED_TABLE_NODE(aggregate_node->left_child()->left_child()->right_child(), "table_c");
  ASSERT_STORED_TABLE_NODE(aggregate_node->left_child()->right_child(), "table_a");
}

TEST_F(SQLTranslatorTest, CreateView) {
  const auto query = "CREATE VIEW my_first_view AS SELECT * FROM table_a WHERE a = 'b';";
  auto result_node = compile_query(query);
  EXPECT_EQ(result_node->type(), LQPNodeType::CreateView);
  auto create_view_node = std::dynamic_pointer_cast<CreateViewNode>(result_node);

  EXPECT_EQ(create_view_node->type(), LQPNodeType::CreateView);
  EXPECT_EQ(create_view_node->view_name(), "my_first_view");

  auto view_lqp = create_view_node->lqp();
  EXPECT_EQ(view_lqp->type(), LQPNodeType::Projection);
  EXPECT_FALSE(view_lqp->right_child());

  auto ts_node_1 = std::static_pointer_cast<PredicateNode>(view_lqp->left_child());
  EXPECT_EQ(ts_node_1->type(), LQPNodeType::Predicate);
  EXPECT_FALSE(ts_node_1->right_child());
  EXPECT_EQ(ts_node_1->column_id(), ColumnID{0});
  EXPECT_EQ(ts_node_1->scan_type(), ScanType::Equals);
  EXPECT_EQ(ts_node_1->value(), AllParameterVariant{std::string{"b"}});
}

TEST_F(SQLTranslatorTest, CreateAliasView) {
  const auto query = "CREATE VIEW my_second_view (c, d) AS SELECT * FROM table_a WHERE a = 'b';";
  auto result_node = compile_query(query);
  EXPECT_EQ(result_node->type(), LQPNodeType::CreateView);
  auto create_view_node = std::dynamic_pointer_cast<CreateViewNode>(result_node);

  auto view_lqp = create_view_node->lqp();

  EXPECT_EQ(view_lqp->output_column_names(), std::vector<std::string>({"c", "d"}));
}

TEST_F(SQLTranslatorTest, DropView) {
  const auto query = "DROP VIEW my_third_view";
  auto result_node = compile_query(query);
  EXPECT_EQ(result_node->type(), LQPNodeType::DropView);
  auto drop_view_node = std::dynamic_pointer_cast<DropViewNode>(result_node);
  EXPECT_EQ(drop_view_node->view_name(), "my_third_view");
}

TEST_F(SQLTranslatorTest, AccessInvalidColumn) {
  const auto query = "SELECT * FROM table_a WHERE invalidname = 0;";
  EXPECT_THROW(compile_query(query), std::logic_error);
}

TEST_F(SQLTranslatorTest, AccessInvalidTable) {
  const auto query = "SELECT * FROM invalid_table;";
  EXPECT_THROW(compile_query(query), std::logic_error);
}

}  // namespace opossum
