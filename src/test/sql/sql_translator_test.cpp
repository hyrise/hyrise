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
#include "logical_query_plan/dummy_table_node.hpp"
#include "logical_query_plan/insert_node.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/limit_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/projection_node.hpp"
#include "logical_query_plan/sort_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "logical_query_plan/update_node.hpp"
#include "null_value.hpp"
#include "sql/sql_pipeline.hpp"
#include "sql/sql_translator.hpp"
#include "storage/storage_manager.hpp"

using namespace std::string_literals;  // NOLINT

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
  void SetUp() override {
    load_test_tables();
    _stored_table_node_a = StoredTableNode::make("table_a");
    _stored_table_node_b = StoredTableNode::make("table_b");
    _stored_table_node_c = StoredTableNode::make("table_c");
    _table_a_a = _stored_table_node_a->get_column("a"s);
    _table_a_b = _stored_table_node_a->get_column("b"s);
    _table_b_a = _stored_table_node_b->get_column("a"s);
    _table_b_b = _stored_table_node_b->get_column("b"s);
    _table_c_a = _stored_table_node_c->get_column("a"s);
    _table_c_d = _stored_table_node_c->get_column("d"s);
  }
  void TearDown() override { StorageManager::reset(); }

  std::shared_ptr<StoredTableNode> _stored_table_node_a;
  std::shared_ptr<StoredTableNode> _stored_table_node_b;
  std::shared_ptr<StoredTableNode> _stored_table_node_c;
  LQPColumnReference _table_a_a;
  LQPColumnReference _table_a_b;
  LQPColumnReference _table_b_a;
  LQPColumnReference _table_b_b;
  LQPColumnReference _table_c_a;
  LQPColumnReference _table_c_d;
};

TEST_F(SQLTranslatorTest, SelectStarAllTest) {
  const auto query = "SELECT * FROM table_a;";
  const auto result_node = compile_query(query);

  auto projection_node = ProjectionNode::make_pass_through(_stored_table_node_a);

  EXPECT_LQP_EQ(projection_node, result_node);
}

TEST_F(SQLTranslatorTest, ExpressionTest) {
  const auto query = "SELECT * FROM table_a WHERE a = 1233 + 1";
  const auto result_node = compile_query(query);

  EXPECT_EQ(result_node->type(), LQPNodeType::Projection);
  EXPECT_EQ(result_node->left_input()->type(), LQPNodeType::Projection);
  EXPECT_FALSE(result_node->right_input());

  ASSERT_EQ(result_node->left_input()->left_input()->type(), LQPNodeType::Predicate);
  const auto predicate_node = std::dynamic_pointer_cast<PredicateNode>(result_node->left_input()->left_input());
  EXPECT_FALSE(predicate_node->right_input());
  EXPECT_EQ(predicate_node->predicate_condition(), PredicateCondition::Equals);

  ASSERT_EQ(predicate_node->left_input()->type(), LQPNodeType::Projection);
  const auto projection_node = std::dynamic_pointer_cast<ProjectionNode>(predicate_node->left_input());
  EXPECT_TRUE(projection_node->column_expressions().back()->is_arithmetic_operator());

  const auto original_node = projection_node->left_input();

  // The value() of the PredicateNode is the LQPColumnReference to the (added) projection node column
  // containing the nested expression
  ASSERT_TRUE(is_lqp_column_reference(predicate_node->value()));
  EXPECT_EQ(predicate_node->column_reference(), LQPColumnReference(original_node, ColumnID{0}));
  EXPECT_EQ(boost::get<LQPColumnReference>(predicate_node->value()), LQPColumnReference(projection_node, ColumnID{2}));
}

TEST_F(SQLTranslatorTest, ExpressionWithColumnTest) {
  const auto query = "SELECT * FROM table_a WHERE a = 1231 + b";
  const auto result_node = compile_query(query);

  ASSERT_EQ(result_node->left_input()->left_input()->left_input()->type(), LQPNodeType::Projection);
  const auto projection_node =
      std::dynamic_pointer_cast<ProjectionNode>(result_node->left_input()->left_input()->left_input());

  const auto expression = projection_node->column_expressions().back();
  EXPECT_TRUE(expression->is_arithmetic_operator());
  EXPECT_EQ(expression->type(), ExpressionType::Addition);
  EXPECT_EQ(expression->left_child()->type(), ExpressionType::Literal);
  EXPECT_EQ(expression->right_child()->type(), ExpressionType::Column);
}

TEST_F(SQLTranslatorTest, TwoColumnFilter) {
  const auto query = "SELECT * FROM table_a WHERE a = \"b\"";
  const auto result_node = compile_query(query);

  auto projection_node = ProjectionNode::make_pass_through(
      PredicateNode::make(_table_a_a, PredicateCondition::Equals, _table_a_b, _stored_table_node_a));

  EXPECT_LQP_EQ(projection_node, result_node);
}

TEST_F(SQLTranslatorTest, ExpressionStringTest) {
  const auto query = "SELECT * FROM table_a WHERE a = 'b'";
  const auto result_node = compile_query(query);

  auto projection_node = ProjectionNode::make_pass_through(
      PredicateNode::make(_table_a_a, PredicateCondition::Equals, "b", _stored_table_node_a));

  EXPECT_LQP_EQ(projection_node, result_node);
}

TEST_F(SQLTranslatorTest, SelectWithAndCondition) {
  const auto query = "SELECT * FROM table_a WHERE a >= 1234 AND b < 457.9";
  const auto result_node = compile_query(query);

  auto projection_node = ProjectionNode::make_pass_through(PredicateNode::make(
      _table_a_b, PredicateCondition::LessThan, 457.9f,
      PredicateNode::make(_table_a_a, PredicateCondition::GreaterThanEquals, int64_t(1234), _stored_table_node_a)));

  EXPECT_LQP_EQ(projection_node, result_node);
}

TEST_F(SQLTranslatorTest, AggregateWithGroupBy) {
  const auto query = "SELECT a, SUM(b) AS s FROM table_a GROUP BY a;";
  const auto result_node = compile_query(query);

  EXPECT_EQ(result_node->type(), LQPNodeType::Projection);
  EXPECT_FALSE(result_node->right_input());

  const auto projection_node = std::dynamic_pointer_cast<ProjectionNode>(result_node);
  EXPECT_NE(projection_node, nullptr);
  EXPECT_EQ(projection_node->column_expressions().size(), 2u);
  EXPECT_EQ(projection_node->output_column_names().size(), 2u);
  EXPECT_EQ(projection_node->output_column_names()[0], std::string("a"));
  EXPECT_EQ(projection_node->output_column_names()[1], std::string("s"));

  const auto aggregate_node = std::dynamic_pointer_cast<AggregateNode>(result_node->left_input());
  ASSERT_NE(aggregate_node, nullptr);
  ASSERT_NE(aggregate_node->left_input(), nullptr);

  const auto stored_table_node = aggregate_node->left_input();
  EXPECT_EQ(stored_table_node->type(), LQPNodeType::StoredTable);
  EXPECT_FALSE(stored_table_node->left_input());
  EXPECT_FALSE(stored_table_node->right_input());

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
  EXPECT_FALSE(result_node->right_input());

  const auto projection_node = std::dynamic_pointer_cast<ProjectionNode>(result_node);
  EXPECT_NE(projection_node, nullptr);
  EXPECT_EQ(projection_node->column_expressions().size(), 2u);
  EXPECT_EQ(projection_node->output_column_names().size(), 2u);
  EXPECT_EQ(projection_node->output_column_names()[0], std::string("s"));
  EXPECT_EQ(projection_node->output_column_names()[1], std::string("f"));

  const auto aggregate_node = std::dynamic_pointer_cast<AggregateNode>(result_node->left_input());
  EXPECT_EQ(aggregate_node->aggregate_expressions().size(), 2u);
  EXPECT_EQ(aggregate_node->groupby_column_references().size(), 0u);
  EXPECT_EQ(aggregate_node->aggregate_expressions().at(0)->alias(), std::string("s"));
  EXPECT_EQ(aggregate_node->aggregate_expressions().at(1)->alias(), std::string("f"));

  const auto stored_table_node = aggregate_node->left_input();
  EXPECT_EQ(stored_table_node->type(), LQPNodeType::StoredTable);
  EXPECT_FALSE(stored_table_node->left_input());
  EXPECT_FALSE(stored_table_node->right_input());
}

TEST_F(SQLTranslatorTest, AggregateWithCountDistinct) {
  const auto query = "SELECT a, COUNT(DISTINCT b) AS s FROM table_a GROUP BY a;";
  const auto result_node = compile_query(query);

  EXPECT_EQ(result_node->type(), LQPNodeType::Projection);
  EXPECT_FALSE(result_node->right_input());

  const auto projection_node = std::dynamic_pointer_cast<ProjectionNode>(result_node);
  EXPECT_NE(projection_node, nullptr);
  EXPECT_EQ(projection_node->column_expressions().size(), 2u);
  EXPECT_EQ(projection_node->output_column_names().size(), 2u);
  EXPECT_EQ(projection_node->output_column_names()[0], std::string("a"));
  EXPECT_EQ(projection_node->output_column_names()[1], std::string("s"));

  const auto aggregate_node = std::dynamic_pointer_cast<AggregateNode>(result_node->left_input());
  EXPECT_NE(aggregate_node, nullptr);
  EXPECT_NE(aggregate_node->left_input(), nullptr);
  EXPECT_EQ(aggregate_node->aggregate_expressions().size(), 1u);
  const std::vector<LQPColumnReference> groupby_columns(
      {LQPColumnReference{aggregate_node->left_input(), ColumnID{0}}});
  EXPECT_EQ(aggregate_node->groupby_column_references(), groupby_columns);
  EXPECT_EQ(aggregate_node->aggregate_expressions().at(0)->alias(), std::string("s"));
  EXPECT_EQ(aggregate_node->aggregate_expressions().at(0)->aggregate_function(), AggregateFunction::CountDistinct);

  const auto stored_table_node = aggregate_node->left_input();
  EXPECT_EQ(stored_table_node->type(), LQPNodeType::StoredTable);
  EXPECT_FALSE(stored_table_node->left_input());
  EXPECT_FALSE(stored_table_node->right_input());
}

TEST_F(SQLTranslatorTest, SelectMultipleOrderBy) {
  const auto query = "SELECT * FROM table_a ORDER BY a DESC, b ASC;";
  const auto result_node = compile_query(query);

  const auto projection_node =
      SortNode::make(OrderByDefinitions{{_table_a_a, OrderByMode::Descending}, {_table_a_b, OrderByMode::Ascending}},
                     ProjectionNode::make_pass_through(_stored_table_node_a));

  EXPECT_LQP_EQ(projection_node, result_node);
}

TEST_F(SQLTranslatorTest, SelectInnerJoin) {
  const auto query = "SELECT * FROM table_a AS a INNER JOIN table_b AS b ON a.a = b.a;";
  auto result_node = compile_query(query);

  _stored_table_node_a->set_alias("a");
  _stored_table_node_b->set_alias("b");

  // clang-format off
  const auto projection_node =
  ProjectionNode::make_pass_through(
    JoinNode::make(JoinMode::Inner, LQPColumnReferencePair{_table_a_a, _table_b_a}, PredicateCondition::Equals,
      _stored_table_node_a,
      _stored_table_node_b));
  // clang-format on

  EXPECT_LQP_EQ(projection_node, result_node);
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

  const auto stored_table_node_a = StoredTableNode::make("table_a", "a");
  const auto stored_table_node_b = StoredTableNode::make("table_b", "b");
  const auto table_a_a = stored_table_node_a->get_column("a"s);
  const auto table_b_a = stored_table_node_b->get_column("a"s);

  // clang-format off
  const auto projection_node =
  ProjectionNode::make_pass_through(
    JoinNode::make(mode, LQPColumnReferencePair{table_a_a, table_b_a}, PredicateCondition::Equals,
        stored_table_node_a,
        stored_table_node_b));
  // clang-format on

  EXPECT_LQP_EQ(projection_node, result_node);
}

INSTANTIATE_TEST_CASE_P(SQLTranslatorJoinTestInstanciation, SQLTranslatorJoinTest,
                        ::testing::Values(JoinMode::Left, JoinMode::Right, JoinMode::Outer), );  // NOLINT

TEST_F(SQLTranslatorTest, SelectSelfJoin) {
  const auto query = "SELECT * FROM table_a AS t1 JOIN table_a AS t2 ON t1.a = t2.b;";
  auto result_node = compile_query(query);

  const auto stored_table_node_t1 = StoredTableNode::make("table_a", "t1");
  const auto stored_table_node_t2 = StoredTableNode::make("table_a", "t2");
  const auto t1_a = stored_table_node_t1->get_column("a"s);
  const auto t2_b = stored_table_node_t2->get_column("b"s);

  // clang-format off
  const auto projection_node =
  ProjectionNode::make_pass_through(
    JoinNode::make(JoinMode::Inner, LQPColumnReferencePair{t1_a, t2_b}, PredicateCondition::Equals,
      stored_table_node_t1,
      stored_table_node_t2));
  // clang-format on

  EXPECT_LQP_EQ(projection_node, result_node);
}

TEST_F(SQLTranslatorTest, SelectNaturalJoin) {
  const auto query = "SELECT * FROM table_a NATURAL JOIN table_c;";
  auto result_node = compile_query(query);

  // clang-format off
  const auto projection_node =
  ProjectionNode::make(LQPExpression::create_columns({_table_a_a, _table_a_b, _table_c_d}),
    ProjectionNode::make(LQPExpression::create_columns({_table_a_a, _table_a_b, _table_c_d}),
      PredicateNode::make(_table_a_a, PredicateCondition::Equals, _table_c_a,
        JoinNode::make(JoinMode::Cross,
          _stored_table_node_a,
          _stored_table_node_c))));
  // clang-format on

  EXPECT_LQP_EQ(projection_node, result_node);
}

TEST_F(SQLTranslatorTest, SelectCrossJoin) {
  const auto query = "SELECT * FROM table_a AS a, table_b AS b WHERE a.a = b.a;";
  auto result_node = compile_query(query);

  _stored_table_node_a->set_alias("a");
  _stored_table_node_b->set_alias("b");

  // clang-format off
  const auto projection_node =
  ProjectionNode::make_pass_through(
    PredicateNode::make(_table_a_a, PredicateCondition::Equals, _table_b_a,
      JoinNode::make(JoinMode::Cross,
        _stored_table_node_a,
        _stored_table_node_b)));
  // clang-format on

  EXPECT_LQP_EQ(projection_node, result_node);
}

TEST_F(SQLTranslatorTest, SelectLimit) {
  const auto query = "SELECT * FROM table_a LIMIT 2;";
  auto result_node = compile_query(query);

  const auto lqp = LimitNode::make(2, ProjectionNode::make_pass_through(_stored_table_node_a));

  EXPECT_LQP_EQ(lqp, result_node);
}

TEST_F(SQLTranslatorTest, InsertValues) {
  const auto query = "INSERT INTO table_a VALUES (10, 12.5);";
  auto result_node = compile_query(query);

  const auto value_a = LQPExpression::create_literal(10);
  const auto value_b = LQPExpression::create_literal(12.5f);

  const auto lqp = InsertNode::make(
      "table_a",
      ProjectionNode::make(std::vector<std::shared_ptr<LQPExpression>>{value_a, value_b}, DummyTableNode::make()));

  EXPECT_LQP_EQ(lqp, result_node);
}

TEST_F(SQLTranslatorTest, InsertValuesColumnReorder) {
  const auto query = "INSERT INTO table_a (b, a) VALUES (12.5, 10);";
  auto result_node = compile_query(query);

  const auto value_a = LQPExpression::create_literal(10);
  const auto value_b = LQPExpression::create_literal(12.5f);

  const auto lqp = InsertNode::make(
      "table_a",
      ProjectionNode::make(std::vector<std::shared_ptr<LQPExpression>>{value_a, value_b}, DummyTableNode::make()));

  EXPECT_LQP_EQ(lqp, result_node);
}

TEST_F(SQLTranslatorTest, InsertValuesIncompleteColumns) {
  const auto query = "INSERT INTO table_a (a) VALUES (10);";
  auto result_node = compile_query(query);

  const auto value_a = LQPExpression::create_literal(10);
  const auto value_b = LQPExpression::create_literal(NullValue{});

  const auto lqp = InsertNode::make(
      "table_a",
      ProjectionNode::make(std::vector<std::shared_ptr<LQPExpression>>{value_a, value_b}, DummyTableNode::make()));

  EXPECT_LQP_EQ(lqp, result_node);
}

TEST_F(SQLTranslatorTest, InsertSubquery) {
  const auto query = "INSERT INTO table_a SELECT a, b FROM table_b;";
  auto result_node = compile_query(query);

  const auto columns = LQPExpression::create_columns({_table_b_a, _table_b_b});

  const auto lqp = InsertNode::make("table_a", ProjectionNode::make(columns, _stored_table_node_b));

  EXPECT_LQP_EQ(lqp, result_node);
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

  const auto update_a = LQPExpression::create_column(_table_a_a);
  const auto update_b = LQPExpression::create_literal(3.2f);

  const auto lqp = UpdateNode::make(
      "table_a", std::vector<std::shared_ptr<LQPExpression>>{update_a, update_b},
      PredicateNode::make(_table_a_a, PredicateCondition::GreaterThan, int64_t(1), _stored_table_node_a));

  EXPECT_LQP_EQ(lqp, result_node);
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
   *      | |_[Cross Join]                     (left_input)
   *      |   |_[StoredTable] Name: 'table_a'
   *      |   |_[StoredTable] Name: 'table_b'
   *      |_[StoredTable] Name: 'table_c'      (right_input)
   */

  const auto result = compile_query(query);

  ASSERT_NE(result->left_input(), nullptr);                                             // Aggregate
  ASSERT_NE(result->left_input()->left_input(), nullptr);                               // CrossJoin
  ASSERT_NE(result->left_input()->left_input()->left_input(), nullptr);                 // CrossJoin
  ASSERT_NE(result->left_input()->left_input()->left_input()->left_input(), nullptr);   // table_a
  ASSERT_NE(result->left_input()->left_input()->left_input()->right_input(), nullptr);  // table_b
  ASSERT_NE(result->left_input()->left_input()->right_input(), nullptr);                // table_c

  ASSERT_EQ(result->left_input()->type(), LQPNodeType::Aggregate);
  const auto aggregate_node = std::dynamic_pointer_cast<AggregateNode>(result->left_input());
  const auto table_a_node = result->left_input()->left_input()->left_input()->left_input();
  const auto table_b_node = result->left_input()->left_input()->left_input()->right_input();
  const auto table_c_node = result->left_input()->left_input()->right_input();

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

  const auto view_node = ProjectionNode::make_pass_through(
      PredicateNode::make(_table_a_a, PredicateCondition::Equals, "b", _stored_table_node_a));

  const auto lqp = std::make_shared<CreateViewNode>("my_first_view", view_node);

  EXPECT_LQP_EQ(lqp, result_node);
}

TEST_F(SQLTranslatorTest, CreateAliasView) {
  const auto query = "CREATE VIEW my_second_view (c, d) AS SELECT * FROM table_a WHERE a = 'b';";
  auto result_node = compile_query(query);

  const auto alias_c = LQPExpression::create_column(_table_a_a, "c");
  const auto alias_d = LQPExpression::create_column(_table_a_b, "d");

  // clang-format off
  const auto view_node =
  ProjectionNode::make(std::vector<std::shared_ptr<LQPExpression>>{alias_c, alias_d},
    ProjectionNode::make_pass_through(
      PredicateNode::make(_table_a_a, PredicateCondition::Equals, "b",
        _stored_table_node_a)));
  // clang-format on

  const auto lqp = std::make_shared<CreateViewNode>("my_second_view", view_node);

  EXPECT_LQP_EQ(lqp, result_node);
}

TEST_F(SQLTranslatorTest, DropView) {
  const auto query = "DROP VIEW my_third_view";
  auto result_node = compile_query(query);

  const auto lqp = DropViewNode::make("my_third_view");

  EXPECT_LQP_EQ(lqp, result_node);
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

  const auto& expressions = std::dynamic_pointer_cast<ProjectionNode>(result_node->left_input())->column_expressions();
  EXPECT_EQ(expressions.size(), 2u);
  EXPECT_EQ(*expressions[0]->alias(), std::string("y"));
  EXPECT_EQ(*expressions[1]->alias(), std::string("z"));
}

}  // namespace opossum
