#include <expression/arithmetic_expression.hpp>
#include "gtest/gtest.h"

#include "constant_mappings.hpp"
#include "expression/abstract_expression.hpp"
#include "expression/binary_predicate_expression.hpp"
#include "expression/expression_factory.hpp"
#include "expression/case_expression.hpp"
#include "expression/value_expression.hpp"
#include "expression/arithmetic_expression.hpp"
#include "expression/lqp_column_expression.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/aggregate_node.hpp"
#include "logical_query_plan/alias_node.hpp"
#include "logical_query_plan/dummy_table_node.hpp"
#include "logical_query_plan/lqp_column_reference.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/projection_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "logical_query_plan/sort_node.hpp"
#include "logical_query_plan/union_node.hpp"
#include "storage/storage_manager.hpp"
#include "sql/sql_translator.hpp"
#include "utils/load_table.hpp"
#include "testing_assert.hpp"

using namespace opossum::expression_factory;  // NOLINT

namespace {
void load_test_tables() {
  opossum::StorageManager::get().add_table("int_float", opossum::load_table("src/test/tables/int_float.tbl"));
  opossum::StorageManager::get().add_table("int_float2", opossum::load_table("src/test/tables/int_float2.tbl"));
  opossum::StorageManager::get().add_table("int_float5", opossum::load_table("src/test/tables/int_float5.tbl"));
}
}  // namespace

namespace opossum {

class SQLTranslatorTest : public ::testing::Test {
 public:
  void SetUp() override {
    load_test_tables();
    stored_table_node_int_float = StoredTableNode::make("int_float");
    stored_table_node_int_float2 = StoredTableNode::make("int_float2");
    stored_table_node_int_float5 = StoredTableNode::make("int_float5");
    int_float_a = stored_table_node_int_float->get_column("a");
    int_float_b = stored_table_node_int_float->get_column("b");
    int_float5_a = stored_table_node_int_float5->get_column("a");
    int_float5_d = stored_table_node_int_float5->get_column("d");
//    _table_b_a = _stored_table_node_b->get_column("a"s);
//    _table_b_b = _stored_table_node_b->get_column("b"s);
//    _table_c_a = _stored_table_node_c->get_column("a"s);
//    _table_c_d = _stored_table_node_c->get_column("d"s);

    int_float_a_expression = std::make_shared<LQPColumnExpression>(int_float_a);
    int_float_b_expression = std::make_shared<LQPColumnExpression>(int_float_b);

  }

  void TearDown() override {
    StorageManager::reset();
  }

  std::shared_ptr<opossum::AbstractLQPNode> compile_query(const std::string& query) {
    const auto lqps = SQLTranslator{}.translate_sql(query);
    Assert(lqps.size() == 1, "Expected just one LQP");
    return lqps.at(0);
  }

  std::shared_ptr<StoredTableNode> stored_table_node_int_float;
  std::shared_ptr<StoredTableNode> stored_table_node_int_float2;
  std::shared_ptr<StoredTableNode> stored_table_node_int_float5;
  std::shared_ptr<AbstractExpression> int_float_a_expression;
  std::shared_ptr<AbstractExpression> int_float_b_expression;
  LQPColumnReference int_float_a, int_float_b, int_float5_a, int_float5_d;
//  LQPColumnReference _table_a_b;
//  LQPColumnReference _table_b_a;
//  LQPColumnReference _table_b_b;
//  LQPColumnReference _table_c_a;
//  LQPColumnReference _table_c_d;
};

// Not supported by SQLParser
TEST_F(SQLTranslatorTest, DISABLED_NoFromClause) {
  const auto actual_lqp = compile_query("SELECT 1 + 2;");

  // clang-format off
  const auto expected_lqp =
  ProjectionNode::make(expression_vector(addition(value(1), value(2))),
    DummyTableNode::make());
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SQLTranslatorTest, SelectSingleColumn) {
  const auto actual_lqp = compile_query("SELECT a FROM int_float;");

  const auto expected_expression = std::vector<std::shared_ptr<AbstractExpression>>({
                                                                                    int_float_a_expression
  });
  const auto expected_lqp = ProjectionNode::make(expected_expression, stored_table_node_int_float);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SQLTranslatorTest, SelectStar) {
  const auto actual_lqp_no_table = compile_query("SELECT * FROM int_float;");
  const auto actual_lqp_table = compile_query("SELECT int_float.* FROM int_float;");

  EXPECT_LQP_EQ(actual_lqp_no_table, stored_table_node_int_float);
  EXPECT_LQP_EQ(actual_lqp_table, stored_table_node_int_float);
}

TEST_F(SQLTranslatorTest, SelectStarSelectsOnlyFromColumns) {
  /**
   * Test that if temporary columns are introduced, these are not selected by "*"
   */

  // "a + b" is a temporary column that shouldn't be in the output
  const auto actual_lqp_no_table = compile_query("SELECT * FROM int_float WHERE a + b > 10");
  const auto actual_lqp_table = compile_query("SELECT int_float.* FROM int_float WHERE a + b > 10;");

  // clang-format off
  const auto expected_lqp =
  ProjectionNode::make(expression_vector(int_float_a, int_float_b),
    PredicateNode::make(greater_than(addition(int_float_a, int_float_b), 10),
      ProjectionNode::make(expression_vector(addition(int_float_a, int_float_b), int_float_a, int_float_b), stored_table_node_int_float)
    ));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp_no_table, expected_lqp);
  EXPECT_LQP_EQ(actual_lqp_table, expected_lqp);
}

TEST_F(SQLTranslatorTest, SimpleArithmeticExpression) {
  const auto actual_lqp = compile_query("SELECT a * b FROM int_float;");

  const auto expected_expression = std::vector<std::shared_ptr<AbstractExpression>>({
    std::make_shared<ArithmeticExpression>(ArithmeticOperator::Multiplication, int_float_a_expression, int_float_b_expression)
  });
  const auto expected_lqp = ProjectionNode::make(expected_expression, stored_table_node_int_float);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SQLTranslatorTest, CaseExpressionSimple) {
  const auto actual_lqp = compile_query("SELECT"
                                        "    CASE (a + b) * 3"
                                        "       WHEN 123 THEN 'Hello'"
                                        "       WHEN 1234 THEN 'World'"
                                        "       ELSE 'Nope'"
                                        "    END"
                                        " FROM int_float;");

  // clang-format off
  const auto a_plus_b_times_3 = multiplication(addition(int_float_a, int_float_b), 3);

  const auto expression = case_(equals(a_plus_b_times_3, 123), "Hello",
                                case_(equals(a_plus_b_times_3, 1234), "World",
                                      "Nope"));
  // clang-format on

  const auto expected_lqp = ProjectionNode::make(expression_vector(expression), stored_table_node_int_float);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SQLTranslatorTest, CaseExpressionSearched) {
  const auto actual_lqp = compile_query("SELECT"
                                        "    CASE"
                                        "       WHEN a = 123 THEN b"
                                        "       WHEN a = 1234 THEN a"
                                        "       ELSE NULL"
                                        "    END"
                                        " FROM int_float;");

  // clang-format off
  const auto expression = case_(equals(int_float_a, 123),
                                int_float_b,
                                case_(equals(int_float_a, 1234),
                                      int_float_a,
                                      null()));
  // clang-format on

  const auto expected_lqp = ProjectionNode::make(expression_vector(expression), stored_table_node_int_float);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SQLTranslatorTest, AliasesInSelectList) {
  const auto actual_lqp = compile_query("SELECT a AS column_a, b, b + a AS sum_column FROM int_float;");

  const auto aliases = std::vector<std::string>{{"column_a", "b", "sum_column"}};
  const auto expressions = expression_vector(int_float_a, int_float_b, addition(int_float_b, int_float_a));

  // clang-format off
  const auto expected_lqp =
  AliasNode::make(expressions, aliases,
    ProjectionNode::make(expressions, stored_table_node_int_float)
  );
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SQLTranslatorTest, WhereSimple) {
  const auto actual_lqp = compile_query("SELECT a FROM int_float WHERE a < 200;");

  // clang-format off
  const auto expected_lqp =
  ProjectionNode::make(expression_vector(int_float_a),
    PredicateNode::make(less_than(int_float_a, 200), stored_table_node_int_float));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SQLTranslatorTest, WhereWithArithmetics) {
  const auto actual_lqp = compile_query("SELECT a FROM int_float WHERE a * b >= b + a;");

  const auto a_times_b = multiplication(int_float_a, int_float_b);
  const auto b_plus_a = addition(int_float_b, int_float_a);

  // clang-format off
  const auto expected_lqp =
  ProjectionNode::make(expression_vector(int_float_a),
    PredicateNode::make(greater_than_equals(a_times_b, b_plus_a),
      ProjectionNode::make(expression_vector(a_times_b, b_plus_a, int_float_a, int_float_b), stored_table_node_int_float
  )));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SQLTranslatorTest, WhereWithOr) {
  const auto actual_lqp = compile_query("SELECT a FROM int_float WHERE 5 >= b + a OR (a > 2 AND b > 2);");

  const auto b_plus_a = addition(int_float_b, int_float_a);

  // clang-format off
  const auto expected_lqp =
  ProjectionNode::make(expression_vector(int_float_a),
    UnionNode::make(UnionMode::Positions,
      ProjectionNode::make(expression_vector(int_float_a, int_float_b), // get rid of "b+a" for the Union
        PredicateNode::make(greater_than_equals(5, b_plus_a),
          ProjectionNode::make(expression_vector(b_plus_a, int_float_a, int_float_b), // compute "b+a"
            stored_table_node_int_float))),
      PredicateNode::make(greater_than(int_float_a, 2),
        PredicateNode::make(greater_than(int_float_b, 2),
           stored_table_node_int_float))
    )
  );
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SQLTranslatorTest, WhereWithBetween) {
  const auto actual_lqp = compile_query("SELECT a FROM int_float WHERE a BETWEEN b and 5;");

  const auto a_times_b = multiplication(int_float_a, int_float_b);
  const auto b_plus_a = addition(int_float_b, int_float_a);

  // clang-format off
  const auto expected_lqp =
  ProjectionNode::make(expression_vector(int_float_a),
    PredicateNode::make(between(int_float_a, int_float_b, 5),
      stored_table_node_int_float
  ));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SQLTranslatorTest, AggregateWithGroupBy) {
  const auto actual_lqp = compile_query("SELECT SUM(a * 3) * b FROM int_float GROUP BY b");

  const auto a_times_3 = multiplication(int_float_a, 3);

  // clang-format off
  const auto expected_lqp =
  ProjectionNode::make(expression_vector(multiplication(sum(a_times_3), int_float_b)),
    AggregateNode::make(expression_vector(int_float_b), expression_vector(sum(a_times_3)),
      ProjectionNode::make(expression_vector(int_float_a, int_float_b, a_times_3),
      stored_table_node_int_float
  )));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SQLTranslatorTest, GroupByOnly) {
  const auto actual_lqp = compile_query("SELECT * FROM int_float GROUP BY b + 3, a / b, b");

  const auto b_plus_3 = addition(int_float_b, 3);
  const auto a_divided_by_b = division(int_float_a, int_float_b);
  const auto group_by_expressions = expression_vector(b_plus_3, a_divided_by_b, int_float_b);

  // clang-format off
  const auto expected_lqp =
  AggregateNode::make(expression_vector(b_plus_3, a_divided_by_b, int_float_b), expression_vector(),
    ProjectionNode::make(expression_vector(int_float_a, int_float_b, b_plus_3, a_divided_by_b),
      stored_table_node_int_float
  ));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SQLTranslatorTest, AggregateAndGroupByWildcard) {
  // - "int_float.*" will select only "b", because a is not in GROUP BY
  // - y is an alias assigned in the SELECT list and can be used in the GROUP BY list
  const auto actual_lqp = compile_query("SELECT int_float.*, b+3 AS y, SUM(a+b) FROM int_float GROUP BY y, b");

  const auto sum_a_plus_b = sum(addition(int_float_a, int_float_b));
  const auto b_plus_3 = addition(int_float_b, 3);

  const auto aliases = std::vector<std::string>({"b", "y", "SUM(a + b)"});
  const auto select_list_expressions = expression_vector(int_float_b, b_plus_3, sum(addition(int_float_a, int_float_b)));

  // clang-format off
  const auto expected_lqp =
  AliasNode::make(select_list_expressions, aliases,
    ProjectionNode::make(select_list_expressions,
      AggregateNode::make(expression_vector(b_plus_3, int_float_b), expression_vector(sum_a_plus_b),
        ProjectionNode::make(expression_vector(int_float_a, int_float_b, addition(int_float_a, int_float_b), b_plus_3),
          stored_table_node_int_float
  ))));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SQLTranslatorTest, AggregateForwarding) {
  // Test that a referenced Aggregate does not result in redundant (and illegal!) AggregateNodes

  const auto actual_lqp = compile_query("SELECT x + 3 FROM (SELECT MIN(a) as x FROM int_float) AS t;");

  // clang-format off
  const auto expected_lqp =
  ProjectionNode::make(expression_vector(addition(min(int_float_a), 3)),
    AliasNode::make(expression_vector(min(int_float_a)), std::vector<std::string>({"x"}),
      AggregateNode::make(expression_vector(), expression_vector(min(int_float_a)),
        stored_table_node_int_float
  )));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SQLTranslatorTest, SubSelectFromSimple) {
  const auto actual_lqp_a = compile_query("SELECT z.x, z.a, z.b FROM (SELECT a + b AS x, * FROM int_float) AS z");
  const auto actual_lqp_b = compile_query("SELECT * FROM (SELECT a + b AS x, * FROM int_float) AS z");
  const auto actual_lqp_c = compile_query("SELECT z.* FROM (SELECT a + b AS x, * FROM int_float) AS z");

  const auto expressions = expression_vector(addition(int_float_a, int_float_b), int_float_a, int_float_b);
  const auto aliases = std::vector<std::string>({"x", "a", "b"});

  // Redundant AliasNode due to the SQLTranslator architecture. Doesn't look nice, but not really an issue.
  // clang-format off
  const auto expected_lqp =
  AliasNode::make(expressions, aliases,
    AliasNode::make(expressions, aliases,
      ProjectionNode::make(expressions, stored_table_node_int_float)
  ));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp_a, expected_lqp);
  EXPECT_LQP_EQ(actual_lqp_b, expected_lqp);
  EXPECT_LQP_EQ(actual_lqp_c, expected_lqp);
}

TEST_F(SQLTranslatorTest, SubSelectSelectList) {
  // "d" is from the outer query
  const auto actual_lqp_a = compile_query("SELECT (SELECT MIN(a + d) FROM int_float), a FROM int_float5 AS f");

  const auto a_plus_d = addition(int_float_a, external(int_float5_d, 0));

  // clang-format off
  const auto sub_select_lqp =
  AggregateNode::make(expression_vector(), expression_vector(min(a_plus_d)),
    ProjectionNode::make(expression_vector(int_float_a, int_float_b, a_plus_d), stored_table_node_int_float)
  );
  // clang-format on

  const auto select_expressions = expression_vector(select(sub_select_lqp, expression_vector(int_float5_d)), int_float5_a);

  // clang-format off
  const auto expected_lqp = ProjectionNode::make(select_expressions, stored_table_node_int_float5);
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp_a, expected_lqp);
}

TEST_F(SQLTranslatorTest, OrderByTest) {
  const auto actual_lqp = compile_query("SELECT * FROM int_float ORDER BY a, a+b DESC, b ASC");

  const auto order_by_modes = std::vector<OrderByMode>({OrderByMode::Ascending, OrderByMode::Descending, OrderByMode::Ascending});

  // clang-format off
  const auto expected_lqp =
  ProjectionNode::make(expression_vector(int_float_a, int_float_b),
    SortNode::make(expression_vector(int_float_a, addition(int_float_a, int_float_b), int_float_b), order_by_modes,
      ProjectionNode::make(expression_vector(addition(int_float_a, int_float_b), int_float_a, int_float_b),
        stored_table_node_int_float
  )));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp)
}

TEST_F(SQLTranslatorTest, InArray) {
  const auto actual_lqp = compile_query("SELECT * FROM int_float WHERE a + 7 IN (1+2,3,4)");

  const auto a_plus_7_in = in(addition(int_float_a, 7), array(addition(1,2), 3, 4));

  // clang-format off
  const auto expected_lqp =
  ProjectionNode::make(expression_vector(int_float_a, int_float_b),
    PredicateNode::make(not_equals(a_plus_7_in, 0),
      ProjectionNode::make(expression_vector(a_plus_7_in, int_float_a, int_float_b),
         stored_table_node_int_float
  )));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

//TEST_F(SQLTranslatorTest, ExpressionStringTest) {
//  const auto query = "SELECT * FROM table_a WHERE a = 'b'";
//  const auto result_node = compile_query(query);
//
//  auto projection_node = ProjectionNode::make_pass_through(
//      PredicateNode::make(_table_a_a, PredicateCondition::Equals, "b", _stored_table_node_a));
//
//  EXPECT_LQP_EQ(projection_node, result_node);
//}
//TEST_F(SQLTranslatorTest, AggregateWithInvalidGroupBy) {
//  // Cannot select b without it being in the GROUP BY clause.
//  const auto query = "SELECT b, SUM(b) AS s FROM table_a GROUP BY a;";
//  EXPECT_THROW(compile_query(query), std::runtime_error);
//}
//
//TEST_F(SQLTranslatorTest, AggregateWithCountDistinct) {
//  const auto query = "SELECT a, COUNT(DISTINCT b) AS s FROM table_a GROUP BY a;";
//  const auto result_node = compile_query(query);
//
//  EXPECT_EQ(result_node->type(), LQPNodeType::Projection);
//  EXPECT_FALSE(result_node->right_input());
//
//  const auto projection_node = std::dynamic_pointer_cast<ProjectionNode>(result_node);
//  EXPECT_NE(projection_node, nullptr);
//  EXPECT_EQ(projection_node->column_expressions().size(), 2u);
//  EXPECT_EQ(projection_node->output_column_names().size(), 2u);
//  EXPECT_EQ(projection_node->output_column_names()[0], std::string("a"));
//  EXPECT_EQ(projection_node->output_column_names()[1], std::string("s"));
//
//  const auto aggregate_node = std::dynamic_pointer_cast<AggregateNode>(result_node->left_input());
//  EXPECT_NE(aggregate_node, nullptr);
//  EXPECT_NE(aggregate_node->left_input(), nullptr);
//  EXPECT_EQ(aggregate_node->aggregate_expressions().size(), 1u);
//  const std::vector<LQPColumnReference> groupby_columns(
//      {LQPColumnReference{aggregate_node->left_input(), ColumnID{0}}});
//  EXPECT_EQ(aggregate_node->groupby_column_references(), groupby_columns);
//  EXPECT_EQ(aggregate_node->aggregate_expressions().at(0)->alias(), std::string("s"));
//  EXPECT_EQ(aggregate_node->aggregate_expressions().at(0)->aggregate_function(), AggregateFunction::CountDistinct);
//
//  const auto stored_table_node = aggregate_node->left_input();
//  EXPECT_EQ(stored_table_node->type(), LQPNodeType::StoredTable);
//  EXPECT_FALSE(stored_table_node->left_input());
//  EXPECT_FALSE(stored_table_node->right_input());
//}
//
//TEST_F(SQLTranslatorTest, SelectMultipleOrderBy) {
//  const auto query = "SELECT * FROM table_a ORDER BY a DESC, b ASC;";
//  const auto result_node = compile_query(query);
//
//  const auto projection_node =
//      SortNode::make(OrderByDefinitions{{_table_a_a, OrderByMode::Descending}, {_table_a_b, OrderByMode::Ascending}},
//                     ProjectionNode::make_pass_through(_stored_table_node_a));
//
//  EXPECT_LQP_EQ(projection_node, result_node);
//}
//
//TEST_F(SQLTranslatorTest, SelectInnerJoin) {
//  const auto query = "SELECT * FROM table_a AS a INNER JOIN table_b AS b ON a.a = b.a;";
//  auto result_node = compile_query(query);
//
//  _stored_table_node_a->set_alias("a");
//  _stored_table_node_b->set_alias("b");
//
//  // clang-format off
//  const auto projection_node =
//  ProjectionNode::make_pass_through(
//    JoinNode::make(JoinMode::Inner, LQPColumnReferencePair{_table_a_a, _table_b_a}, PredicateCondition::Equals,
//      _stored_table_node_a,
//      _stored_table_node_b));
//  // clang-format on
//
//  EXPECT_LQP_EQ(projection_node, result_node);
//}
//
//class SQLTranslatorJoinTest : public ::testing::TestWithParam<JoinMode> {
//  void SetUp() override { load_test_tables(); }
//  void TearDown() override { StorageManager::reset(); }
//};
//
//// Verifies that LEFT/RIGHT JOIN are handled correctly and LEFT/RIGHT OUTER JOIN identically
//TEST_P(SQLTranslatorJoinTest, SelectLeftRightOuterJoins) {
//  using namespace std::string_literals;  // NOLINT (Linter does not know about using namespace)
//
//  const auto mode = GetParam();
//
//  std::string mode_str = boost::to_upper_copy(join_mode_to_string.at(mode));
//
//  const auto query = "SELECT * FROM table_a AS a "s + mode_str + " JOIN table_b AS b ON a.a = b.a;";
//  const auto result_node = compile_query(query);
//
//  const auto stored_table_node_a = StoredTableNode::make("table_a", "a");
//  const auto stored_table_node_b = StoredTableNode::make("table_b", "b");
//  const auto table_a_a = stored_table_node_a->get_column("a"s);
//  const auto table_b_a = stored_table_node_b->get_column("a"s);
//
//  // clang-format off
//  const auto projection_node =
//  ProjectionNode::make_pass_through(
//    JoinNode::make(mode, LQPColumnReferencePair{table_a_a, table_b_a}, PredicateCondition::Equals,
//        stored_table_node_a,
//        stored_table_node_b));
//  // clang-format on
//
//  EXPECT_LQP_EQ(projection_node, result_node);
//}
//
//INSTANTIATE_TEST_CASE_P(SQLTranslatorJoinTestInstanciation, SQLTranslatorJoinTest,
//                        ::testing::Values(JoinMode::Left, JoinMode::Right, JoinMode::Outer), );  // NOLINT
//
//TEST_F(SQLTranslatorTest, SelectSelfJoin) {
//  const auto query = "SELECT * FROM table_a AS t1 JOIN table_a AS t2 ON t1.a = t2.b;";
//  auto result_node = compile_query(query);
//
//  const auto stored_table_node_t1 = StoredTableNode::make("table_a", "t1");
//  const auto stored_table_node_t2 = StoredTableNode::make("table_a", "t2");
//  const auto t1_a = stored_table_node_t1->get_column("a"s);
//  const auto t2_b = stored_table_node_t2->get_column("b"s);
//
//  // clang-format off
//  const auto projection_node =
//  ProjectionNode::make_pass_through(
//    JoinNode::make(JoinMode::Inner, LQPColumnReferencePair{t1_a, t2_b}, PredicateCondition::Equals,
//      stored_table_node_t1,
//      stored_table_node_t2));
//  // clang-format on
//
//  EXPECT_LQP_EQ(projection_node, result_node);
//}
//
//TEST_F(SQLTranslatorTest, SelectNaturalJoin) {
//  const auto query = "SELECT * FROM table_a NATURAL JOIN table_c;";
//  auto result_node = compile_query(query);
//
//  // clang-format off
//  const auto projection_node =
//  ProjectionNode::make(LQPExpression::create_columns({_table_a_a, _table_a_b, _table_c_d}),
//    ProjectionNode::make(LQPExpression::create_columns({_table_a_a, _table_a_b, _table_c_d}),
//      PredicateNode::make(_table_a_a, PredicateCondition::Equals, _table_c_a,
//        JoinNode::make(JoinMode::Cross,
//          _stored_table_node_a,
//          _stored_table_node_c))));
//  // clang-format on
//
//  EXPECT_LQP_EQ(projection_node, result_node);
//}
//
//TEST_F(SQLTranslatorTest, SelectCrossJoin) {
//  const auto query = "SELECT * FROM table_a AS a, table_b AS b WHERE a.a = b.a;";
//  auto result_node = compile_query(query);
//
//  _stored_table_node_a->set_alias("a");
//  _stored_table_node_b->set_alias("b");
//
//  // clang-format off
//  const auto projection_node =
//  ProjectionNode::make_pass_through(
//    PredicateNode::make(_table_a_a, PredicateCondition::Equals, _table_b_a,
//      JoinNode::make(JoinMode::Cross,
//        _stored_table_node_a,
//        _stored_table_node_b)));
//  // clang-format on
//
//  EXPECT_LQP_EQ(projection_node, result_node);
//}
//
//TEST_F(SQLTranslatorTest, SelectLimit) {
//  const auto query = "SELECT * FROM table_a LIMIT 2;";
//  auto result_node = compile_query(query);
//
//  const auto lqp = LimitNode::make(2, ProjectionNode::make_pass_through(_stored_table_node_a));
//
//  EXPECT_LQP_EQ(lqp, result_node);
//}
//
//TEST_F(SQLTranslatorTest, InsertValues) {
//  const auto query = "INSERT INTO table_a VALUES (10, 12.5);";
//  auto result_node = compile_query(query);
//
//  const auto value_a = LQPExpression::create_literal(10);
//  const auto value_b = LQPExpression::create_literal(12.5f);
//
//  const auto lqp = InsertNode::make(
//      "table_a",
//      ProjectionNode::make(std::vector<std::shared_ptr<LQPExpression>>{value_a, value_b}, DummyTableNode::make()));
//
//  EXPECT_LQP_EQ(lqp, result_node);
//}
//
//TEST_F(SQLTranslatorTest, InsertValuesColumnReorder) {
//  const auto query = "INSERT INTO table_a (b, a) VALUES (12.5, 10);";
//  auto result_node = compile_query(query);
//
//  const auto value_a = LQPExpression::create_literal(10);
//  const auto value_b = LQPExpression::create_literal(12.5f);
//
//  const auto lqp = InsertNode::make(
//      "table_a",
//      ProjectionNode::make(std::vector<std::shared_ptr<LQPExpression>>{value_a, value_b}, DummyTableNode::make()));
//
//  EXPECT_LQP_EQ(lqp, result_node);
//}
//
//TEST_F(SQLTranslatorTest, InsertValuesIncompleteColumns) {
//  const auto query = "INSERT INTO table_a (a) VALUES (10);";
//  auto result_node = compile_query(query);
//
//  const auto value_a = LQPExpression::create_literal(10);
//  const auto value_b = LQPExpression::create_literal(NullValue{});
//
//  const auto lqp = InsertNode::make(
//      "table_a",
//      ProjectionNode::make(std::vector<std::shared_ptr<LQPExpression>>{value_a, value_b}, DummyTableNode::make()));
//
//  EXPECT_LQP_EQ(lqp, result_node);
//}
//
//TEST_F(SQLTranslatorTest, InsertSubquery) {
//  const auto query = "INSERT INTO table_a SELECT a, b FROM table_b;";
//  auto result_node = compile_query(query);
//
//  const auto columns = LQPExpression::create_columns({_table_b_a, _table_b_b});
//
//  const auto lqp = InsertNode::make("table_a", ProjectionNode::make(columns, _stored_table_node_b));
//
//  EXPECT_LQP_EQ(lqp, result_node);
//}
//
//TEST_F(SQLTranslatorTest, InsertInvalidDataType) {
//  auto query = "INSERT INTO table_a VALUES (10, 11);";
//  EXPECT_THROW(compile_query(query), std::runtime_error);
//
//  query = "INSERT INTO table_a (b, a) VALUES (10, 12.5);";
//  EXPECT_THROW(compile_query(query), std::runtime_error);
//}
//
//TEST_F(SQLTranslatorTest, Update) {
//  const auto query = "UPDATE table_a SET b = 3.2 WHERE a > 1;";
//  auto result_node = compile_query(query);
//
//  const auto update_a = LQPExpression::create_column(_table_a_a);
//  const auto update_b = LQPExpression::create_literal(3.2f);
//
//  const auto lqp = UpdateNode::make(
//      "table_a", std::vector<std::shared_ptr<LQPExpression>>{update_a, update_b},
//      PredicateNode::make(_table_a_a, PredicateCondition::GreaterThan, int64_t(1), _stored_table_node_a));
//
//  EXPECT_LQP_EQ(lqp, result_node);
//}
//
//TEST_F(SQLTranslatorTest, WhereSubquery) {
//  const auto query = "SELECT * FROM table_a WHERE a < (SELECT MAX(a) FROM table_b);";
//  auto result_node = compile_query(query);
//
//  EXPECT_EQ(result_node->type(), LQPNodeType::Projection);
//  const auto final_projection_node = std::dynamic_pointer_cast<ProjectionNode>(result_node);
//
//  EXPECT_EQ(final_projection_node->left_input()->type(), LQPNodeType::Projection);
//  const auto reduce_projection_node = std::dynamic_pointer_cast<ProjectionNode>(final_projection_node->left_input());
//  EXPECT_EQ(reduce_projection_node->output_column_references().size(),
//            final_projection_node->output_column_references().size());
//
//  EXPECT_EQ(reduce_projection_node->left_input()->type(), LQPNodeType::Predicate);
//  const auto predicate_node = std::dynamic_pointer_cast<PredicateNode>(reduce_projection_node->left_input());
//  EXPECT_EQ(predicate_node->predicate_condition(), PredicateCondition::LessThan);
//
//  EXPECT_EQ(predicate_node->left_input()->type(), LQPNodeType::Projection);
//  const auto expand_projection_node = std::dynamic_pointer_cast<ProjectionNode>(predicate_node->left_input());
//  EXPECT_EQ(expand_projection_node->output_column_references().size(), 3u);
//
//  const auto subselect_expression = expand_projection_node->column_expressions().back();
//  EXPECT_EQ(subselect_expression->type(), ExpressionType::Subselect);
//}
//
//TEST_F(SQLTranslatorTest, InSubquery) {
//  const auto query = "SELECT * FROM table_a WHERE a IN (SELECT a FROM table_b);";
//  auto result_node = compile_query(query);
//
//  EXPECT_EQ(result_node->type(), LQPNodeType::Projection);
//  const auto final_projection_node = std::dynamic_pointer_cast<ProjectionNode>(result_node);
//
//  EXPECT_EQ(final_projection_node->left_input()->type(), LQPNodeType::Join);
//  const auto semi_join_node = std::dynamic_pointer_cast<JoinNode>(final_projection_node->left_input());
//  EXPECT_EQ(semi_join_node->join_mode(), JoinMode::Semi);
//
//  const auto table_a_node = semi_join_node->left_input();
//  ASSERT_STORED_TABLE_NODE(table_a_node, "table_a");
//
//  EXPECT_EQ(semi_join_node->right_input()->type(), LQPNodeType::Projection);
//
//  const auto table_b_node = semi_join_node->right_input()->left_input();
//  ASSERT_STORED_TABLE_NODE(table_b_node, "table_b");
//}
//
//TEST_F(SQLTranslatorTest, InSubquerySeveralColumns) {
//  const auto query = "SELECT * FROM table_a WHERE a IN (SELECT * FROM table_b);";
//  EXPECT_THROW(compile_query(query), std::runtime_error);
//}
//
//TEST_F(SQLTranslatorTest, SelectSubquery) {
//  const auto query = "SELECT a, (SELECT MAX(b) FROM table_b) FROM table_a;";
//  auto result_node = compile_query(query);
//
//  EXPECT_EQ(result_node->type(), LQPNodeType::Projection);
//  const auto projection_node = std::dynamic_pointer_cast<ProjectionNode>(result_node);
//
//  const auto subselect_expression = projection_node->column_expressions().back();
//  EXPECT_EQ(subselect_expression->type(), ExpressionType::Subselect);
//}
//
//TEST_F(SQLTranslatorTest, MixedAggregateAndGroupBySelectList) {
//  /**
//   * Test:
//   *    - Select list can contain both GroupBy and Aggregate Columns in any order
//   *    - The order of GroupBy Columns in the SelectList can differ from the order in the GroupByList
//   */
//
//  const auto query = R"(SELECT
//                          table_b.a, SUM(table_c.a), table_a.b, table_b.b
//                        FROM
//                          table_a, table_b, table_c
//                        GROUP BY
//                          table_a.b, table_a.a, table_b.a, table_b.b)";
//
//  /**
//   * Expect this AST:
//   *
//   * [Projection] table_b.a, SUM(table_c.a), table_a.b, table_a.b
//   *  |_[Aggregate] SUM(table_c.a) GROUP BY [table_a.b, table_a.a, table_b.a, table_b.b]
//   *     |_[Cross Join]
//   *      | |_[Cross Join]                     (left_input)
//   *      |   |_[StoredTable] Name: 'table_a'
//   *      |   |_[StoredTable] Name: 'table_b'
//   *      |_[StoredTable] Name: 'table_c'      (right_input)
//   */
//
//  const auto result = compile_query(query);
//
//  ASSERT_NE(result->left_input(), nullptr);                                             // Aggregate
//  ASSERT_NE(result->left_input()->left_input(), nullptr);                               // CrossJoin
//  ASSERT_NE(result->left_input()->left_input()->left_input(), nullptr);                 // CrossJoin
//  ASSERT_NE(result->left_input()->left_input()->left_input()->left_input(), nullptr);   // table_a
//  ASSERT_NE(result->left_input()->left_input()->left_input()->right_input(), nullptr);  // table_b
//  ASSERT_NE(result->left_input()->left_input()->right_input(), nullptr);                // table_c
//
//  ASSERT_EQ(result->left_input()->type(), LQPNodeType::Aggregate);
//  const auto aggregate_node = std::dynamic_pointer_cast<AggregateNode>(result->left_input());
//  const auto table_a_node = result->left_input()->left_input()->left_input()->left_input();
//  const auto table_b_node = result->left_input()->left_input()->left_input()->right_input();
//  const auto table_c_node = result->left_input()->left_input()->right_input();
//
//  /**
//   * Assert the Projection
//   */
//  ASSERT_EQ(result->type(), LQPNodeType::Projection);
//  const auto projection_node = std::dynamic_pointer_cast<ProjectionNode>(result);
//  ASSERT_EQ(projection_node->column_expressions().size(), 4u);
//  ASSERT_COLUMN_EXPRESSION(projection_node->column_expressions()[0], LQPColumnReference(table_b_node, ColumnID{0}));
//  ASSERT_COLUMN_EXPRESSION(projection_node->column_expressions()[1], LQPColumnReference(aggregate_node, ColumnID{4}));
//  ASSERT_COLUMN_EXPRESSION(projection_node->column_expressions()[2], LQPColumnReference(table_a_node, ColumnID{1}));
//  ASSERT_COLUMN_EXPRESSION(projection_node->column_expressions()[3], LQPColumnReference(table_b_node, ColumnID{1}));
//
//  /**
//   * Assert the Aggregate
//   */
//  ASSERT_EQ(aggregate_node->groupby_column_references().size(), 4u);
//  EXPECT_EQ(aggregate_node->groupby_column_references()[0], LQPColumnReference(table_a_node, ColumnID{1}));
//  EXPECT_EQ(aggregate_node->groupby_column_references()[1], LQPColumnReference(table_a_node, ColumnID{0}));
//  EXPECT_EQ(aggregate_node->groupby_column_references()[2], LQPColumnReference(table_b_node, ColumnID{0}));
//  EXPECT_EQ(aggregate_node->groupby_column_references()[3], LQPColumnReference(table_b_node, ColumnID{1}));
//  ASSERT_EQ(aggregate_node->aggregate_expressions().size(), 1u);
//  const auto sum_expression = aggregate_node->aggregate_expressions()[0];
//  ASSERT_EQ(sum_expression->type(), ExpressionType::Function);
//  ASSERT_AGGREGATE_FUNCTION_EXPRESSION(sum_expression, AggregateFunction::Sum,
//                                       LQPColumnReference(table_c_node, ColumnID{0}));
//
//  ASSERT_STORED_TABLE_NODE(table_b_node, "table_b");
//  ASSERT_STORED_TABLE_NODE(table_c_node, "table_c");
//  ASSERT_STORED_TABLE_NODE(table_a_node, "table_a");
//}
//
//TEST_F(SQLTranslatorTest, CreateView) {
//  const auto query = "CREATE VIEW my_first_view AS SELECT * FROM table_a WHERE a = 'b';";
//  const auto result_node = compile_query(query);
//
//  const auto view_node = ProjectionNode::make_pass_through(
//      PredicateNode::make(_table_a_a, PredicateCondition::Equals, "b", _stored_table_node_a));
//
//  const auto lqp = std::make_shared<CreateViewNode>("my_first_view", view_node);
//
//  EXPECT_LQP_EQ(lqp, result_node);
//}
//
//TEST_F(SQLTranslatorTest, CreateAliasView) {
//  const auto query = "CREATE VIEW my_second_view (c, d) AS SELECT * FROM table_a WHERE a = 'b';";
//  auto result_node = compile_query(query);
//
//  const auto alias_c = LQPExpression::create_column(_table_a_a, "c");
//  const auto alias_d = LQPExpression::create_column(_table_a_b, "d");
//
//  // clang-format off
//  const auto view_node =
//  ProjectionNode::make(std::vector<std::shared_ptr<LQPExpression>>{alias_c, alias_d},
//    ProjectionNode::make_pass_through(
//      PredicateNode::make(_table_a_a, PredicateCondition::Equals, "b",
//        _stored_table_node_a)));
//  // clang-format on
//
//  const auto lqp = std::make_shared<CreateViewNode>("my_second_view", view_node);
//
//  EXPECT_LQP_EQ(lqp, result_node);
//}
//
//TEST_F(SQLTranslatorTest, DropView) {
//  const auto query = "DROP VIEW my_third_view";
//  auto result_node = compile_query(query);
//
//  const auto lqp = DropViewNode::make("my_third_view");
//
//  EXPECT_LQP_EQ(lqp, result_node);
//}
//
//TEST_F(SQLTranslatorTest, AccessInvalidColumn) {
//  const auto query = "SELECT * FROM table_a WHERE invalidname = 0;";
//  EXPECT_THROW(compile_query(query), std::runtime_error);
//}
//
//TEST_F(SQLTranslatorTest, AccessInvalidTable) {
//  const auto query = "SELECT * FROM invalid_table;";
//  EXPECT_THROW(compile_query(query), std::runtime_error);
//}
//
//TEST_F(SQLTranslatorTest, ColumnAlias) {
//  const auto query = "SELECT z, y FROM table_a AS x (y, z)";
//  auto result_node = compile_query(query);
//
//  EXPECT_EQ(result_node->type(), LQPNodeType::Projection);
//
//  const auto& expressions = std::dynamic_pointer_cast<ProjectionNode>(result_node->left_input())->column_expressions();
//  EXPECT_EQ(expressions.size(), 2u);
//  EXPECT_EQ(*expressions[0]->alias(), std::string("y"));
//  EXPECT_EQ(*expressions[1]->alias(), std::string("z"));
//}

}  // namespace opossum
