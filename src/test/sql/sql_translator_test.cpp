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
#include "sql/sql_pipeline.hpp"
#include "sql/sql_translator.hpp"
#include "storage/storage_manager.hpp"

namespace {
std::shared_ptr<opossum::AbstractLQPNode> compile_query(const std::string& query) {
  return opossum::SQLPipeline{query, opossum::UseMvcc::No}.get_unoptimized_logical_plans().at(0);
}

void load_test_tables() {
  opossum::StorageManager::get().add_table("table_a", opossum::load_table("src/test/tables/int_float.tbl", 2));
  opossum::StorageManager::get().add_table("table_b", opossum::load_table("src/test/tables/int_float2.tbl", 2));
  opossum::StorageManager::get().add_table("table_c", opossum::load_table("src/test/tables/int_float5.tbl", 2));
}
}  // namespace

namespace opossum {

class SQLTranslatorTest : public BaseTest {
 protected:
  void SetUp() override { load_test_tables(); }
  void TearDown() override { StorageManager::reset(); }
};

TEST_F(SQLTranslatorTest, SelectStarAllTest) {
  const auto query = "SELECT * FROM table_a;";
  const auto result_node = compile_query(query);

  ASSERT_EQ(result_node->type(), LQPNodeType::Projection);
  ASSERT_TRUE(result_node->left_child());
  ASSERT_EQ(result_node->left_child()->type(), LQPNodeType::StoredTable);
  EXPECT_FALSE(result_node->right_child());
  EXPECT_FALSE(result_node->left_child()->left_child());
  ASSERT_EQ(result_node->output_column_references().size(), 2u);

  EXPECT_EQ(result_node->output_column_references()[0], LQPColumnReference(result_node->left_child(), ColumnID{0}));
  EXPECT_EQ(result_node->output_column_references()[1], LQPColumnReference(result_node->left_child(), ColumnID{1}));
}

TEST_F(SQLTranslatorTest, ExpressionTest) {
  const auto query = "SELECT * FROM table_a WHERE a = 1233 + 1";
  const auto result_node = compile_query(query);

  EXPECT_EQ(result_node->type(), LQPNodeType::Projection);
  EXPECT_EQ(result_node->left_child()->type(), LQPNodeType::Projection);
  EXPECT_FALSE(result_node->right_child());

  ASSERT_EQ(result_node->left_child()->left_child()->type(), LQPNodeType::Predicate);
  const auto predicate_node = std::dynamic_pointer_cast<PredicateNode>(result_node->left_child()->left_child());
  EXPECT_FALSE(predicate_node->right_child());
  EXPECT_EQ(predicate_node->predicate_condition(), PredicateCondition::Equals);

  ASSERT_EQ(predicate_node->left_child()->type(), LQPNodeType::Projection);
  const auto projection_node = std::dynamic_pointer_cast<ProjectionNode>(predicate_node->left_child());
  EXPECT_TRUE(projection_node->column_expressions().back()->is_arithmetic_operator());

  const auto original_node = projection_node->left_child();

  // The value() of the PredicateNode is the LQPColumnReference to the (added) projection node column
  // containing the nested expression
  ASSERT_TRUE(is_lqp_column_reference(predicate_node->value()));
  EXPECT_EQ(predicate_node->column_reference(), LQPColumnReference(original_node, ColumnID{0}));
  EXPECT_EQ(boost::get<LQPColumnReference>(predicate_node->value()), LQPColumnReference(projection_node, ColumnID{2}));
}

TEST_F(SQLTranslatorTest, ExpressionWithColumnTest) {
  const auto query = "SELECT * FROM table_a WHERE a = 1231 + b";
  const auto result_node = compile_query(query);

  ASSERT_EQ(result_node->left_child()->left_child()->left_child()->type(), LQPNodeType::Projection);
  const auto projection_node =
      std::dynamic_pointer_cast<ProjectionNode>(result_node->left_child()->left_child()->left_child());

  const auto expression = projection_node->column_expressions().back();
  EXPECT_TRUE(expression->is_arithmetic_operator());
  EXPECT_EQ(expression->type(), ExpressionType::Addition);
  EXPECT_EQ(expression->left_child()->type(), ExpressionType::Literal);
  EXPECT_EQ(expression->right_child()->type(), ExpressionType::Column);
}

TEST_F(SQLTranslatorTest, TwoColumnFilter) {
  const auto query = "SELECT * FROM table_a WHERE a = \"b\"";
  const auto result_node = compile_query(query);

  EXPECT_EQ(result_node->type(), LQPNodeType::Projection);
  EXPECT_FALSE(result_node->right_child());

  ASSERT_EQ(result_node->left_child()->type(), LQPNodeType::Predicate);
  auto predicate_node = std::dynamic_pointer_cast<PredicateNode>(result_node->left_child());
  EXPECT_FALSE(predicate_node->right_child());
  EXPECT_EQ(predicate_node->predicate_condition(), PredicateCondition::Equals);
  EXPECT_EQ(predicate_node->column_reference(), LQPColumnReference(predicate_node->left_child(), ColumnID{0}));
  EXPECT_EQ(predicate_node->value(),
            AllParameterVariant(LQPColumnReference(predicate_node->left_child(), ColumnID{1})));

  EXPECT_EQ(result_node->output_column_references()[0], LQPColumnReference(predicate_node->left_child(), ColumnID{0}));
  EXPECT_EQ(result_node->output_column_references()[1], LQPColumnReference(predicate_node->left_child(), ColumnID{1}));
}

TEST_F(SQLTranslatorTest, ExpressionStringTest) {
  const auto query = "SELECT * FROM table_a WHERE a = 'b'";
  const auto result_node = compile_query(query);

  EXPECT_EQ(result_node->type(), LQPNodeType::Projection);
  EXPECT_FALSE(result_node->right_child());

  auto predicate_node = std::dynamic_pointer_cast<PredicateNode>(result_node->left_child());
  EXPECT_EQ(predicate_node->type(), LQPNodeType::Predicate);
  EXPECT_FALSE(predicate_node->right_child());
  EXPECT_EQ(predicate_node->column_reference(), LQPColumnReference(predicate_node->left_child(), ColumnID{0}));
  EXPECT_EQ(predicate_node->predicate_condition(), PredicateCondition::Equals);
  EXPECT_EQ(predicate_node->value(), AllParameterVariant{std::string{"b"}});
}

TEST_F(SQLTranslatorTest, SelectWithAndCondition) {
  const auto query = "SELECT * FROM table_a WHERE a >= 1234 AND b < 457.9";
  const auto result_node = compile_query(query);

  EXPECT_EQ(result_node->type(), LQPNodeType::Projection);
  EXPECT_FALSE(result_node->right_child());

  const auto predicate_node_a = result_node->left_child();
  EXPECT_EQ(predicate_node_a->type(), LQPNodeType::Predicate);
  EXPECT_FALSE(predicate_node_a->right_child());

  const auto predicate_node_b = predicate_node_a->left_child();
  EXPECT_EQ(predicate_node_b->type(), LQPNodeType::Predicate);
  EXPECT_FALSE(predicate_node_b->right_child());

  const auto stored_table_node = predicate_node_b->left_child();
  EXPECT_EQ(stored_table_node->type(), LQPNodeType::StoredTable);
  EXPECT_FALSE(stored_table_node->left_child());
  EXPECT_FALSE(stored_table_node->right_child());
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
  ASSERT_NE(aggregate_node, nullptr);
  ASSERT_NE(aggregate_node->left_child(), nullptr);

  const auto stored_table_node = aggregate_node->left_child();
  EXPECT_EQ(stored_table_node->type(), LQPNodeType::StoredTable);
  EXPECT_FALSE(stored_table_node->left_child());
  EXPECT_FALSE(stored_table_node->right_child());

  EXPECT_EQ(aggregate_node->aggregate_expressions().size(), 1u);
  const std::vector<LQPColumnReference> groupby_columns = {LQPColumnReference{stored_table_node, ColumnID{0}}};
  EXPECT_EQ(aggregate_node->groupby_column_references(), groupby_columns);
  EXPECT_EQ(aggregate_node->aggregate_expressions().at(0)->alias(), std::string("s"));
}

TEST_F(SQLTranslatorTest, AggregateWithInvalidGroupBy) {
  // Cannot select b without it being in the GROUP BY clause.
  const auto query = "SELECT b, SUM(b) AS s FROM table_a GROUP BY a;";
  EXPECT_THROW(compile_query(query), std::runtime_error);
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
  EXPECT_EQ(aggregate_node->groupby_column_references().size(), 0u);
  EXPECT_EQ(aggregate_node->aggregate_expressions().at(0)->alias(), std::string("s"));
  EXPECT_EQ(aggregate_node->aggregate_expressions().at(1)->alias(), std::string("f"));

  const auto stored_table_node = aggregate_node->left_child();
  EXPECT_EQ(stored_table_node->type(), LQPNodeType::StoredTable);
  EXPECT_FALSE(stored_table_node->left_child());
  EXPECT_FALSE(stored_table_node->right_child());
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
  EXPECT_NE(aggregate_node->left_child(), nullptr);
  EXPECT_EQ(aggregate_node->aggregate_expressions().size(), 1u);
  const std::vector<LQPColumnReference> groupby_columns(
      {LQPColumnReference{aggregate_node->left_child(), ColumnID{0}}});
  EXPECT_EQ(aggregate_node->groupby_column_references(), groupby_columns);
  EXPECT_EQ(aggregate_node->aggregate_expressions().at(0)->alias(), std::string("s"));
  EXPECT_EQ(aggregate_node->aggregate_expressions().at(0)->aggregate_function(), AggregateFunction::CountDistinct);

  const auto stored_table_node = aggregate_node->left_child();
  EXPECT_EQ(stored_table_node->type(), LQPNodeType::StoredTable);
  EXPECT_FALSE(stored_table_node->left_child());
  EXPECT_FALSE(stored_table_node->right_child());
}

TEST_F(SQLTranslatorTest, SelectMultipleOrderBy) {
  const auto query = "SELECT * FROM table_a ORDER BY a DESC, b ASC;";
  const auto result_node = compile_query(query);

  ASSERT_EQ(result_node->type(), LQPNodeType::Sort);
  const auto sort_node = std::dynamic_pointer_cast<SortNode>(result_node);

  ASSERT_NE(result_node->left_child(), nullptr);
  ASSERT_EQ(result_node->left_child()->type(), LQPNodeType::Projection);
  ASSERT_NE(result_node->left_child()->left_child(), nullptr);
  ASSERT_EQ(result_node->left_child()->left_child()->type(), LQPNodeType::StoredTable);

  const auto stored_table_node = std::dynamic_pointer_cast<StoredTableNode>(result_node->left_child()->left_child());

  const auto& order_by_definitions = sort_node->order_by_definitions();
  EXPECT_EQ(order_by_definitions.size(), 2u);

  const auto& definition_1 = order_by_definitions[0];
  EXPECT_EQ(definition_1.column_reference, LQPColumnReference(stored_table_node, ColumnID{0}));
  EXPECT_EQ(definition_1.order_by_mode, OrderByMode::Descending);

  const auto& definition_2 = order_by_definitions[1];
  EXPECT_EQ(definition_2.column_reference, LQPColumnReference(stored_table_node, ColumnID{1}));
  EXPECT_EQ(definition_2.order_by_mode, OrderByMode::Ascending);

  // The sort node has an input node, but we don't care what kind it is in this test.
  EXPECT_TRUE(sort_node->left_child());
  EXPECT_FALSE(sort_node->right_child());
}

TEST_F(SQLTranslatorTest, SelectInnerJoin) {
  const auto query = "SELECT * FROM table_a AS a INNER JOIN table_b AS b ON a.a = b.a;";
  auto result_node = compile_query(query);

  EXPECT_EQ(result_node->type(), LQPNodeType::Projection);
  const auto projection_node = std::dynamic_pointer_cast<ProjectionNode>(result_node);
  EXPECT_EQ(projection_node->output_column_count(), 4u);
  std::vector<std::string> output_columns = {"a", "b", "a", "b"};
  EXPECT_EQ(projection_node->output_column_names(), output_columns);

  EXPECT_EQ(result_node->left_child()->type(), LQPNodeType::Join);
  const auto join_node = std::dynamic_pointer_cast<JoinNode>(result_node->left_child());

  ASSERT_NE(result_node->left_child()->left_child(), nullptr);
  EXPECT_EQ(result_node->left_child()->left_child()->type(), LQPNodeType::StoredTable);
  ASSERT_NE(result_node->left_child()->right_child(), nullptr);
  EXPECT_EQ(result_node->left_child()->right_child()->type(), LQPNodeType::StoredTable);

  const auto table_a_node = result_node->left_child()->left_child();
  const auto table_b_node = result_node->left_child()->right_child();

  EXPECT_EQ(join_node->predicate_condition(), PredicateCondition::Equals);
  EXPECT_EQ(join_node->join_mode(), JoinMode::Inner);
  EXPECT_EQ(join_node->join_column_references()->first, LQPColumnReference(table_a_node, ColumnID{0}));
  EXPECT_EQ(join_node->join_column_references()->second, LQPColumnReference(table_b_node, ColumnID{0}));
}

class SQLTranslatorJoinTest : public ::testing::TestWithParam<JoinMode> {
  void SetUp() override { load_test_tables(); }
  void TearDown() override { StorageManager::reset(); }
};

// Verifies that LEFT/RIGHT JOIN are handled correctly and LEFT/RIGHT OUTER JOIN identically
TEST_P(SQLTranslatorJoinTest, SelectLeftRightOuterJoins) {
  using namespace std::string_literals;  // NOLINT (Linter does not know about using namespace)

  const auto mode = GetParam();

  std::string mode_str = boost::to_upper_copy(join_mode_to_string.at(mode));

  const auto query = "SELECT * FROM table_a AS a "s + mode_str + " JOIN table_b AS b ON a.a = b.a;";
  const auto result_node = compile_query(query);

  EXPECT_EQ(result_node->type(), LQPNodeType::Projection);
  const auto projection_node = std::dynamic_pointer_cast<ProjectionNode>(result_node);
  std::vector<std::string> output_columns = {"a", "b", "a", "b"};
  EXPECT_EQ(projection_node->output_column_names(), output_columns);

  EXPECT_EQ(result_node->left_child()->type(), LQPNodeType::Join);
  const auto join_node = std::dynamic_pointer_cast<JoinNode>(result_node->left_child());

  ASSERT_NE(join_node->left_child(), nullptr);
  ASSERT_EQ(join_node->left_child()->type(), LQPNodeType::StoredTable);
  ASSERT_NE(join_node->right_child(), nullptr);
  ASSERT_EQ(join_node->right_child()->type(), LQPNodeType::StoredTable);
  const auto table_a_node = join_node->left_child();
  const auto table_b_node = join_node->right_child();

  EXPECT_EQ(join_node->predicate_condition(), PredicateCondition::Equals);
  EXPECT_EQ(join_node->join_mode(), mode);
  EXPECT_EQ(join_node->join_column_references()->first, LQPColumnReference(table_a_node, ColumnID{0}));
  EXPECT_EQ(join_node->join_column_references()->second, LQPColumnReference(table_b_node, ColumnID{0}));
}

INSTANTIATE_TEST_CASE_P(SQLTranslatorJoinTestInstanciation, SQLTranslatorJoinTest,
                        ::testing::Values(JoinMode::Left, JoinMode::Right, JoinMode::Outer), );  // NOLINT

TEST_F(SQLTranslatorTest, SelectSelfJoin) {
  const auto query = "SELECT * FROM table_a AS t1 JOIN table_a AS t2 ON t1.a = t2.b;";
  auto result_node = compile_query(query);

  ASSERT_NE(result_node, nullptr);
  ASSERT_NE(result_node->left_child(), nullptr);
  ASSERT_NE(result_node->left_child()->left_child(), nullptr);
  ASSERT_NE(result_node->left_child()->right_child(), nullptr);

  EXPECT_EQ(result_node->type(), LQPNodeType::Projection);
  EXPECT_EQ(result_node->left_child()->type(), LQPNodeType::Join);
  EXPECT_EQ(result_node->left_child()->left_child()->type(), LQPNodeType::StoredTable);
  EXPECT_EQ(result_node->left_child()->right_child()->type(), LQPNodeType::StoredTable);

  const auto t1_node = result_node->left_child()->left_child();
  const auto t2_node = result_node->left_child()->right_child();
  const auto join_node = std::dynamic_pointer_cast<JoinNode>(result_node->left_child());
  const auto projection_node = std::dynamic_pointer_cast<ProjectionNode>(result_node);

  ASSERT_TRUE(join_node->join_column_references());
  EXPECT_EQ(join_node->join_column_references()->first, LQPColumnReference(t1_node, ColumnID{0}));
  EXPECT_EQ(join_node->join_column_references()->second, LQPColumnReference(t2_node, ColumnID{1}));
}

TEST_F(SQLTranslatorTest, SelectNaturalJoin) {
  const auto query = "SELECT * FROM table_a NATURAL JOIN table_c;";
  auto result_node = compile_query(query);

  std::vector<std::string> output_column_names{"a", "b", "d"};

  EXPECT_EQ(result_node->type(), LQPNodeType::Projection);
  auto projection_node_a = std::dynamic_pointer_cast<ProjectionNode>(result_node);
  EXPECT_EQ(projection_node_a->output_column_names(), output_column_names);

  ASSERT_NE(result_node->left_child(), nullptr);
  EXPECT_EQ(result_node->left_child()->type(), LQPNodeType::Projection);
  auto projection_node_b = std::dynamic_pointer_cast<ProjectionNode>(result_node->left_child());
  EXPECT_EQ(projection_node_b->output_column_names(), output_column_names);

  ASSERT_EQ(projection_node_b->left_child()->type(), LQPNodeType::Predicate);
  const auto predicate_node = std::dynamic_pointer_cast<PredicateNode>(projection_node_b->left_child());

  ASSERT_NE(predicate_node->left_child(), nullptr);
  EXPECT_EQ(predicate_node->left_child()->type(), LQPNodeType::Join);
  auto cross_node = std::dynamic_pointer_cast<JoinNode>(predicate_node->left_child());
  EXPECT_EQ(cross_node->join_mode(), JoinMode::Cross);
  EXPECT_EQ(cross_node->left_child()->type(), LQPNodeType::StoredTable);
  EXPECT_EQ(cross_node->right_child()->type(), LQPNodeType::StoredTable);

  const auto table_a_node = cross_node->left_child();
  const auto table_b_node = cross_node->right_child();

  EXPECT_FALSE(predicate_node->right_child());
  EXPECT_EQ(predicate_node->column_reference(), LQPColumnReference(table_a_node, ColumnID{0}));
  EXPECT_EQ(predicate_node->predicate_condition(), PredicateCondition::Equals);
  EXPECT_EQ(predicate_node->value(), AllParameterVariant{LQPColumnReference(table_b_node, ColumnID{0})});
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
  EXPECT_FALSE(join_node->predicate_condition());
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
  const auto query = "INSERT INTO table_a (b, a) VALUES (12.5, 10);";
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
  const auto insert_node = std::dynamic_pointer_cast<InsertNode>(result_node);
  EXPECT_EQ(insert_node->table_name(), "table_a");
  EXPECT_EQ(insert_node->left_child()->type(), LQPNodeType::Projection);

  const auto projection = std::dynamic_pointer_cast<ProjectionNode>(insert_node->left_child());
  ASSERT_NE(projection, nullptr);

  ASSERT_NE(projection->left_child(), nullptr);
  const auto table_b_node = projection->left_child();

  const auto expressions = projection->column_expressions();
  EXPECT_EQ(expressions[0]->type(), ExpressionType::Column);
  EXPECT_EQ(expressions[0]->column_reference(), LQPColumnReference(table_b_node, ColumnID{0}));
  EXPECT_EQ(expressions[1]->type(), ExpressionType::Column);
  EXPECT_EQ(expressions[1]->column_reference(), LQPColumnReference(table_b_node, ColumnID{1}));
}

TEST_F(SQLTranslatorTest, InsertInvalidDataType) {
  auto query = "INSERT INTO table_a VALUES (10, 11);";
  EXPECT_THROW(compile_query(query), std::runtime_error);

  query = "INSERT INTO table_a (b, a) VALUES (10, 12.5);";
  EXPECT_THROW(compile_query(query), std::runtime_error);
}

TEST_F(SQLTranslatorTest, Update) {
  const auto query = "UPDATE table_a SET b = 3.2 WHERE a > 1;";
  auto result_node = compile_query(query);

  EXPECT_EQ(result_node->type(), LQPNodeType::Update);
  const auto update_node = std::dynamic_pointer_cast<UpdateNode>(result_node);
  EXPECT_EQ(update_node->table_name(), "table_a");
  EXPECT_EQ(update_node->left_child()->type(), LQPNodeType::Predicate);
  ASSERT_NE(update_node->left_child()->left_child(), nullptr);

  const auto table_a_node = update_node->left_child()->left_child();

  const auto expressions = update_node->column_expressions();
  EXPECT_EQ(expressions[0]->type(), ExpressionType::Column);
  EXPECT_EQ(expressions[0]->column_reference(), LQPColumnReference(table_a_node, ColumnID{0}));
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
   * [Projection] table_b.a, SUM(table_c.a), table_a.b, table_a.b
   *  |_[Aggregate] SUM(table_c.a) GROUP BY [table_a.b, table_a.a, table_b.a, table_b.b]
   *     |_[Cross Join]
   *      | |_[Cross Join]                     (left_child)
   *      |   |_[StoredTable] Name: 'table_a'
   *      |   |_[StoredTable] Name: 'table_b'
   *      |_[StoredTable] Name: 'table_c'      (right_child)
   */

  const auto result = compile_query(query);

  ASSERT_NE(result->left_child(), nullptr);                                             // Aggregate
  ASSERT_NE(result->left_child()->left_child(), nullptr);                               // CrossJoin
  ASSERT_NE(result->left_child()->left_child()->left_child(), nullptr);                 // CrossJoin
  ASSERT_NE(result->left_child()->left_child()->left_child()->left_child(), nullptr);   // table_a
  ASSERT_NE(result->left_child()->left_child()->left_child()->right_child(), nullptr);  // table_b
  ASSERT_NE(result->left_child()->left_child()->right_child(), nullptr);                // table_c

  ASSERT_EQ(result->left_child()->type(), LQPNodeType::Aggregate);
  const auto aggregate_node = std::dynamic_pointer_cast<AggregateNode>(result->left_child());
  const auto table_a_node = result->left_child()->left_child()->left_child()->left_child();
  const auto table_b_node = result->left_child()->left_child()->left_child()->right_child();
  const auto table_c_node = result->left_child()->left_child()->right_child();

  /**
   * Assert the Projection
   */
  ASSERT_EQ(result->type(), LQPNodeType::Projection);
  const auto projection_node = std::dynamic_pointer_cast<ProjectionNode>(result);
  ASSERT_EQ(projection_node->column_expressions().size(), 4u);
  ASSERT_COLUMN_EXPRESSION(projection_node->column_expressions()[0], LQPColumnReference(table_b_node, ColumnID{0}));
  ASSERT_COLUMN_EXPRESSION(projection_node->column_expressions()[1], LQPColumnReference(aggregate_node, ColumnID{4}));
  ASSERT_COLUMN_EXPRESSION(projection_node->column_expressions()[2], LQPColumnReference(table_a_node, ColumnID{1}));
  ASSERT_COLUMN_EXPRESSION(projection_node->column_expressions()[3], LQPColumnReference(table_b_node, ColumnID{1}));

  /**
   * Assert the Aggregate
   */
  ASSERT_EQ(aggregate_node->groupby_column_references().size(), 4u);
  EXPECT_EQ(aggregate_node->groupby_column_references()[0], LQPColumnReference(table_a_node, ColumnID{1}));
  EXPECT_EQ(aggregate_node->groupby_column_references()[1], LQPColumnReference(table_a_node, ColumnID{0}));
  EXPECT_EQ(aggregate_node->groupby_column_references()[2], LQPColumnReference(table_b_node, ColumnID{0}));
  EXPECT_EQ(aggregate_node->groupby_column_references()[3], LQPColumnReference(table_b_node, ColumnID{1}));
  ASSERT_EQ(aggregate_node->aggregate_expressions().size(), 1u);
  const auto sum_expression = aggregate_node->aggregate_expressions()[0];
  ASSERT_EQ(sum_expression->type(), ExpressionType::Function);
  ASSERT_AGGREGATE_FUNCTION_EXPRESSION(sum_expression, AggregateFunction::Sum,
                                       LQPColumnReference(table_c_node, ColumnID{0}));

  ASSERT_STORED_TABLE_NODE(table_b_node, "table_b");
  ASSERT_STORED_TABLE_NODE(table_c_node, "table_c");
  ASSERT_STORED_TABLE_NODE(table_a_node, "table_a");
}

TEST_F(SQLTranslatorTest, CreateView) {
  const auto query = "CREATE VIEW my_first_view AS SELECT * FROM table_a WHERE a = 'b';";
  const auto result_node = compile_query(query);
  EXPECT_EQ(result_node->type(), LQPNodeType::CreateView);
  const auto create_view_node = std::dynamic_pointer_cast<CreateViewNode>(result_node);

  EXPECT_EQ(create_view_node->type(), LQPNodeType::CreateView);
  EXPECT_EQ(create_view_node->view_name(), "my_first_view");

  const auto view_lqp = create_view_node->lqp();
  EXPECT_EQ(view_lqp->type(), LQPNodeType::Projection);
  EXPECT_FALSE(view_lqp->right_child());

  const auto ts_node_1 = std::static_pointer_cast<PredicateNode>(view_lqp->left_child());
  EXPECT_EQ(ts_node_1->type(), LQPNodeType::Predicate);
  EXPECT_FALSE(ts_node_1->right_child());
  const auto table_a_node = ts_node_1->left_child();

  EXPECT_EQ(ts_node_1->column_reference(), LQPColumnReference(table_a_node, ColumnID{0}));
  EXPECT_EQ(ts_node_1->predicate_condition(), PredicateCondition::Equals);
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
  EXPECT_THROW(compile_query(query), std::runtime_error);
}

TEST_F(SQLTranslatorTest, AccessInvalidTable) {
  const auto query = "SELECT * FROM invalid_table;";
  EXPECT_THROW(compile_query(query), std::runtime_error);
}

TEST_F(SQLTranslatorTest, ColumnAlias) {
  const auto query = "SELECT z, y FROM table_a AS x (y, z)";
  auto result_node = compile_query(query);

  EXPECT_EQ(result_node->type(), LQPNodeType::Projection);

  const auto& expressions = std::dynamic_pointer_cast<ProjectionNode>(result_node->left_child())->column_expressions();
  EXPECT_EQ(expressions.size(), 2u);
  EXPECT_EQ(*expressions[0]->alias(), std::string("y"));
  EXPECT_EQ(*expressions[1]->alias(), std::string("z"));
}

}  // namespace opossum
