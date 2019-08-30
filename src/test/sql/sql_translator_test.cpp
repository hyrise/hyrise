#include "base_test.hpp"
#include "constant_mappings.hpp"
#include "expression/abstract_expression.hpp"
#include "expression/arithmetic_expression.hpp"
#include "expression/binary_predicate_expression.hpp"
#include "expression/case_expression.hpp"
#include "expression/expression_functional.hpp"
#include "expression/expression_utils.hpp"
#include "expression/lqp_column_expression.hpp"
#include "expression/value_expression.hpp"
#include "hyrise.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/aggregate_node.hpp"
#include "logical_query_plan/alias_node.hpp"
#include "logical_query_plan/create_prepared_plan_node.hpp"
#include "logical_query_plan/create_table_node.hpp"
#include "logical_query_plan/create_view_node.hpp"
#include "logical_query_plan/delete_node.hpp"
#include "logical_query_plan/drop_table_node.hpp"
#include "logical_query_plan/drop_view_node.hpp"
#include "logical_query_plan/dummy_table_node.hpp"
#include "logical_query_plan/insert_node.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/limit_node.hpp"
#include "logical_query_plan/lqp_column_reference.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/projection_node.hpp"
#include "logical_query_plan/sort_node.hpp"
#include "logical_query_plan/static_table_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "logical_query_plan/union_node.hpp"
#include "logical_query_plan/update_node.hpp"
#include "logical_query_plan/validate_node.hpp"
#include "sql/create_sql_parser_error_message.hpp"
#include "sql/sql_translator.hpp"
#include "storage/table.hpp"
#include "testing_assert.hpp"
#include "utils/load_table.hpp"
#include "utils/meta_table_manager.hpp"

using namespace opossum::expression_functional;  // NOLINT
using namespace std::string_literals;            // NOLINT

namespace opossum {

class SQLTranslatorTest : public BaseTest {
 public:
  void SetUp() override {
    Hyrise::get().storage_manager.add_table("int_float", load_table("resources/test_data/tbl/int_float.tbl"));
    Hyrise::get().storage_manager.add_table("int_string", load_table("resources/test_data/tbl/int_string.tbl"));
    Hyrise::get().storage_manager.add_table("int_float2", load_table("resources/test_data/tbl/int_float2.tbl"));
    Hyrise::get().storage_manager.add_table("int_float5", load_table("resources/test_data/tbl/int_float5.tbl"));
    Hyrise::get().storage_manager.add_table("int_int_int", load_table("resources/test_data/tbl/int_int_int.tbl"));

    stored_table_node_int_float = StoredTableNode::make("int_float");
    stored_table_node_int_string = StoredTableNode::make("int_string");
    stored_table_node_int_float2 = StoredTableNode::make("int_float2");
    stored_table_node_int_float5 = StoredTableNode::make("int_float5");
    stored_table_node_int_int_int = StoredTableNode::make("int_int_int");

    int_float_a = stored_table_node_int_float->get_column("a");
    int_float_b = stored_table_node_int_float->get_column("b");
    int_string_a = stored_table_node_int_string->get_column("a");
    int_string_b = stored_table_node_int_string->get_column("b");
    int_float2_a = stored_table_node_int_float2->get_column("a");
    int_float2_b = stored_table_node_int_float2->get_column("b");
    int_float5_a = stored_table_node_int_float5->get_column("a");
    int_float5_d = stored_table_node_int_float5->get_column("d");
    int_int_int_a = stored_table_node_int_int_int->get_column("a");
    int_int_int_b = stored_table_node_int_int_int->get_column("b");
    int_int_int_c = stored_table_node_int_int_int->get_column("c");
  }

  std::shared_ptr<opossum::AbstractLQPNode> compile_query(const std::string& query,
                                                          const UseMvcc use_mvcc = UseMvcc::No) {
    hsql::SQLParserResult parser_result;
    hsql::SQLParser::parseSQLString(query, &parser_result);
    Assert(parser_result.isValid(), create_sql_parser_error_message(query, parser_result));

    const auto lqps = SQLTranslator{use_mvcc}.translate_parser_result(parser_result);

    Assert(lqps.size() == 1, "Expected just one LQP");
    return lqps.at(0);
  }

  std::pair<std::shared_ptr<opossum::AbstractLQPNode>, std::vector<ParameterID>> compile_prepared_query(
      const std::string& query) {
    hsql::SQLParserResult parser_result;
    hsql::SQLParser::parseSQLString(query, &parser_result);
    Assert(parser_result.isValid(), create_sql_parser_error_message(query, parser_result));

    SQLTranslator sql_translator{UseMvcc::No};
    const auto lqps = sql_translator.translate_parser_result(parser_result);
    Assert(lqps.size() == 1, "Expected just one LQP");
    return {lqps.at(0), sql_translator.parameter_ids_of_value_placeholders()};
  }

  std::shared_ptr<StoredTableNode> stored_table_node_int_float;
  std::shared_ptr<StoredTableNode> stored_table_node_int_string;
  std::shared_ptr<StoredTableNode> stored_table_node_int_float2;
  std::shared_ptr<StoredTableNode> stored_table_node_int_float5;
  std::shared_ptr<StoredTableNode> stored_table_node_int_int_int;
  LQPColumnReference int_float_a, int_float_b, int_string_a, int_string_b, int_float5_a, int_float5_d, int_float2_a,
      int_float2_b, int_int_int_a, int_int_int_b, int_int_int_c;
};

TEST_F(SQLTranslatorTest, NoFromClause) {
  const auto actual_lqp = compile_query("SELECT 1 + 2;");

  // clang-format off
  const auto expected_lqp =
  ProjectionNode::make(expression_vector(add_(value_(1), value_(2))),
    DummyTableNode::make());
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SQLTranslatorTest, ExpressionStringTest) {
  const auto actual_lqp = compile_query("SELECT * FROM int_float WHERE a = 'b'");

  // clang-format off
  const auto expected_lqp =
  PredicateNode::make(equals_(int_float_a, pmr_string{"b"}),
    stored_table_node_int_float);
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SQLTranslatorTest, SelectSingleColumn) {
  const auto actual_lqp = compile_query("SELECT a FROM int_float;");

  const auto expected_lqp = ProjectionNode::make(expression_vector(int_float_a), stored_table_node_int_float);

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
  const auto actual_lqp_no_table = compile_query("SELECT * FROM int_float ORDER BY a + b");
  const auto actual_lqp_table = compile_query("SELECT int_float.* FROM int_float ORDER BY a + b");

  // clang-format off
  const auto expected_lqp =
  ProjectionNode::make(expression_vector(int_float_a, int_float_b),
    SortNode::make(expression_vector(add_(int_float_a, int_float_b)), std::vector<OrderByMode>{OrderByMode::Ascending},
      ProjectionNode::make(expression_vector(add_(int_float_a, int_float_b), int_float_a, int_float_b),
        stored_table_node_int_float)));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp_no_table, expected_lqp);
  EXPECT_LQP_EQ(actual_lqp_table, expected_lqp);
}

TEST_F(SQLTranslatorTest, SimpleArithmeticExpression) {
  const auto actual_lqp_a = compile_query("SELECT a * b FROM int_float;");
  const auto actual_lqp_b = compile_query("SELECT a / b FROM int_float;");
  const auto actual_lqp_c = compile_query("SELECT a + b FROM int_float;");
  const auto actual_lqp_d = compile_query("SELECT a - b FROM int_float;");
  const auto actual_lqp_e = compile_query("SELECT a % b FROM int_float;");

  // clang-format off
  const auto expected_lqp_a = ProjectionNode::make(expression_vector(mul_(int_float_a, int_float_b)), stored_table_node_int_float);  // NOLINT
  const auto expected_lqp_b = ProjectionNode::make(expression_vector(div_(int_float_a, int_float_b)), stored_table_node_int_float);  // NOLINT
  const auto expected_lqp_c = ProjectionNode::make(expression_vector(add_(int_float_a, int_float_b)), stored_table_node_int_float);  // NOLINT
  const auto expected_lqp_d = ProjectionNode::make(expression_vector(sub_(int_float_a, int_float_b)), stored_table_node_int_float);  // NOLINT
  const auto expected_lqp_e = ProjectionNode::make(expression_vector(mod_(int_float_a, int_float_b)), stored_table_node_int_float);  // NOLINT
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp_a, expected_lqp_a);
  EXPECT_LQP_EQ(actual_lqp_b, expected_lqp_b);
  EXPECT_LQP_EQ(actual_lqp_c, expected_lqp_c);
  EXPECT_LQP_EQ(actual_lqp_d, expected_lqp_d);
  EXPECT_LQP_EQ(actual_lqp_e, expected_lqp_e);
}

TEST_F(SQLTranslatorTest, NestedArithmeticExpression) {
  // With parentheses: `(a*b) + ((a/b) % 5))`
  const auto actual_lqp = compile_query("SELECT a * b + a / b % 5 FROM int_float;");

  // clang-format off
  const auto expected_lqp =
  ProjectionNode::make(expression_vector(add_(mul_(int_float_a, int_float_b), mod_(div_(int_float_a, int_float_b), 5))),
    stored_table_node_int_float);
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SQLTranslatorTest, CaseExpressionSimple) {
  const auto actual_lqp = compile_query(
      "SELECT"
      "    CASE (a + b) * 3"
      "       WHEN 123 THEN 'Hello'"
      "       WHEN 1234 THEN 'World'"
      "       ELSE 'Nope'"
      "    END"
      " FROM int_float;");

  // clang-format off
  const auto a_plus_b_times_3 = mul_(add_(int_float_a, int_float_b), 3);

  const auto expression = case_(equals_(a_plus_b_times_3, 123), "Hello",
                                case_(equals_(a_plus_b_times_3, 1234), "World",
                                      "Nope"));
  // clang-format on

  const auto expected_lqp = ProjectionNode::make(expression_vector(expression), stored_table_node_int_float);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SQLTranslatorTest, CaseExpressionSimpleNoElse) {
  const auto actual_lqp = compile_query(
      "SELECT"
      "    CASE (a + b) * 3"
      "       WHEN 123 THEN 'Hello'"
      "       WHEN 1234 THEN 'World'"
      "    END"
      " FROM int_float;");

  // clang-format off
  const auto a_plus_b_times_3 = mul_(add_(int_float_a, int_float_b), 3);

  const auto expression = case_(equals_(a_plus_b_times_3, 123), "Hello",
                                case_(equals_(a_plus_b_times_3, 1234), "World",
                                      null_()));
  // clang-format on

  const auto expected_lqp = ProjectionNode::make(expression_vector(expression), stored_table_node_int_float);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SQLTranslatorTest, CaseExpressionSearched) {
  const auto actual_lqp = compile_query(
      "SELECT"
      "    CASE"
      "       WHEN a = 123 THEN b"
      "       WHEN a = 1234 THEN a"
      "       ELSE NULL"
      "    END"
      " FROM int_float;");

  // clang-format off
  const auto expression = case_(equals_(int_float_a, 123),
                                int_float_b,
                                case_(equals_(int_float_a, 1234),
                                      int_float_a,
                                      null_()));
  // clang-format on

  const auto expected_lqp = ProjectionNode::make(expression_vector(expression), stored_table_node_int_float);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SQLTranslatorTest, SelectListAlias) {
  const auto actual_lqp = compile_query("SELECT a AS column_a, b, b + a AS sum_column FROM int_float;");

  const auto aliases = std::vector<std::string>{{"column_a", "b", "sum_column"}};
  const auto expressions = expression_vector(int_float_a, int_float_b, add_(int_float_b, int_float_a));

  // clang-format off
  const auto expected_lqp =
  AliasNode::make(expressions, aliases,
    ProjectionNode::make(expressions, stored_table_node_int_float));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SQLTranslatorTest, SelectListAliasUsedInWhere) {
  const auto actual_lqp_a = compile_query("SELECT a AS x FROM int_float WHERE a > 5");
  const auto actual_lqp_b = compile_query("SELECT a AS x FROM int_float WHERE x > 5");

  const auto aliases = std::vector<std::string>({"x"});

  // clang-format off
  const auto expected_lqp =
  AliasNode::make(expression_vector(int_float_a), aliases,
    ProjectionNode::make(expression_vector(int_float_a),
      PredicateNode::make(greater_than_(int_float_a, value_(5)),
        stored_table_node_int_float)));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp_a, expected_lqp);
  EXPECT_LQP_EQ(actual_lqp_b, expected_lqp);
}

TEST_F(SQLTranslatorTest, SelectListAliasUsedInGroupBy) {
  const auto actual_lqp_a = compile_query("SELECT a AS x FROM int_float GROUP BY a");
  const auto actual_lqp_b = compile_query("SELECT a AS x FROM int_float GROUP BY x");
  const auto actual_lqp_c = compile_query("SELECT a AS x FROM int_float GROUP BY int_float.a");
  const auto actual_lqp_d = compile_query("SELECT a AS x FROM int_float GROUP BY int_float.x");

  const auto aliases = std::vector<std::string>({"x"});

  // clang-format off
  const auto expected_lqp =
  AliasNode::make(expression_vector(int_float_a), aliases,
    AggregateNode::make(expression_vector(int_float_a), expression_vector(),
      stored_table_node_int_float));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp_a, expected_lqp);
  EXPECT_LQP_EQ(actual_lqp_b, expected_lqp);
  EXPECT_LQP_EQ(actual_lqp_c, expected_lqp);
  EXPECT_LQP_EQ(actual_lqp_d, expected_lqp);
}

TEST_F(SQLTranslatorTest, SelectListAliasUsedInGroupByAndHaving) {
  const auto actual_lqp_a = compile_query("SELECT a AS x FROM int_float GROUP BY x HAVING a > 5");
  const auto actual_lqp_b = compile_query("SELECT a AS x FROM int_float GROUP BY x HAVING x > 5");

  const auto aliases = std::vector<std::string>({"x"});

  // clang-format off
  const auto expected_lqp =
  AliasNode::make(expression_vector(int_float_a), aliases,
    PredicateNode::make(greater_than_(int_float_a, value_(5)),
      AggregateNode::make(expression_vector(int_float_a), expression_vector(),
        stored_table_node_int_float)));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp_a, expected_lqp);
  EXPECT_LQP_EQ(actual_lqp_b, expected_lqp);
}

TEST_F(SQLTranslatorTest, SelectListAliasUsedInOrderBy) {
  const auto actual_lqp_a = compile_query("SELECT a AS x, b AS y FROM int_float ORDER BY a, b");
  const auto actual_lqp_b = compile_query("SELECT a AS x, b AS y FROM int_float ORDER BY x, y");

  const auto aliases = std::vector<std::string>({"x", "y"});
  const auto order_by_modes = std::vector<OrderByMode>({OrderByMode::Ascending, OrderByMode::Ascending});

  // clang-format off
  const auto expected_lqp =
  AliasNode::make(expression_vector(int_float_a, int_float_b), aliases,
    SortNode::make(expression_vector(int_float_a, int_float_b), order_by_modes,
      stored_table_node_int_float));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp_a, expected_lqp);
  EXPECT_LQP_EQ(actual_lqp_b, expected_lqp);
}

TEST_F(SQLTranslatorTest, SelectListAliasUsedInJoin) {
  const auto actual_lqp_a = compile_query(
      "SELECT R.a, R.b FROM (SELECT a AS c, b AS d FROM int_float) AS R JOIN int_float2 AS S ON R.b = S.b");
  const auto actual_lqp_b = compile_query(
      "SELECT R.c, R.d FROM (SELECT a AS c, b AS d FROM int_float) AS R JOIN int_float2 AS S ON R.d = S.b");

  const auto aliases = std::vector<std::string>({"c", "d"});

  // clang-format off
  const auto expected_lqp_a =
  ProjectionNode::make(expression_vector(int_float_a, int_float_b),
    JoinNode::make(JoinMode::Inner, equals_(int_float_b, int_float2_b),
      AliasNode::make(expression_vector(int_float_a, int_float_b), aliases,
        stored_table_node_int_float),
      stored_table_node_int_float2));

  const auto expected_lqp_b =
  AliasNode::make(expression_vector(int_float_a, int_float_b), aliases,
    expected_lqp_a);
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp_a, expected_lqp_a);
  EXPECT_LQP_EQ(actual_lqp_b, expected_lqp_b);
}

TEST_F(SQLTranslatorTest, SelectListAliasesDifferentForSimilarColumns) {
  const auto actual_lqp = compile_query("SELECT a AS a1, b AS b2, b AS b3, a AS a3, b AS b1, a AS a2 FROM int_float");

  const auto aliases = std::vector<std::string>({"a1", "b2", "b3", "a3", "b1", "a2"});
  const auto expressions =
      expression_vector(int_float_a, int_float_b, int_float_b, int_float_a, int_float_b, int_float_a);

  // clang-format off
  const auto expected_lqp =
  AliasNode::make(expressions, aliases,
    ProjectionNode::make(expressions,
      stored_table_node_int_float));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SQLTranslatorTest, SelectListAliasesDifferentForSimilarAggregates) {
  const auto actual_lqp = compile_query("SELECT COUNT(*) AS cnt1, COUNT(*) AS cnt2, COUNT(*) AS cnt3 FROM int_float");

  const auto aliases = std::vector<std::string>({"cnt1", "cnt2", "cnt3"});
  const auto aggregates = expression_vector(count_star_(), count_star_(), count_star_());

  // clang-format off
  const auto expected_lqp =
  AliasNode::make(aggregates, aliases,
    ProjectionNode::make(aggregates,
      AggregateNode::make(expression_vector(), expression_vector(count_star_()),
        stored_table_node_int_float)));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SQLTranslatorTest, SelectListAliasesDifferentForSimilarColumnsInSubquery) {
  const auto actual_lqp = compile_query(
      "SELECT a1, b2, a3 FROM (SELECT a AS a1, b AS b2, b AS b3, a AS a3, b AS b1, a AS a2 FROM int_float) AS R");

  const auto outer_aliases = std::vector<std::string>({"a1", "b2", "a3"});
  const auto inner_aliases = std::vector<std::string>({"a1", "b2", "b3", "a3", "b1", "a2"});
  const auto outer_expressions = expression_vector(int_float_a, int_float_b, int_float_a);
  const auto inner_expressions =
      expression_vector(int_float_a, int_float_b, int_float_b, int_float_a, int_float_b, int_float_a);

  // clang-format off
  const auto expected_lqp =
  AliasNode::make(outer_expressions, outer_aliases,
    ProjectionNode::make(outer_expressions,
      AliasNode::make(inner_expressions, inner_aliases,
        ProjectionNode::make(inner_expressions,
          stored_table_node_int_float))));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SQLTranslatorTest, SelectListAliasesDifferentForSimilarAggregatesInSubquery) {
  const auto actual_lqp =
      compile_query("SELECT * FROM (SELECT COUNT(*) AS cnt1, COUNT(*) AS cnt2, COUNT(*) AS cnt3 FROM int_float) AS R");

  const auto aliases = std::vector<std::string>({"cnt1", "cnt2", "cnt3"});
  const auto aggregates = expression_vector(count_star_(), count_star_(), count_star_());

  // clang-format off
  // #1186: Redundant AliasNode due to the SQLTranslator architecture.
  const auto expected_lqp =
  AliasNode::make(aggregates, aliases,
    AliasNode::make(aggregates, aliases,
      ProjectionNode::make(aggregates,
        AggregateNode::make(expression_vector(), expression_vector(count_star_()),
          stored_table_node_int_float))));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SQLTranslatorTest, SelectListAliasesDifferentForSimilarColumnsAndFromColumnAliasing) {
  const auto actual_lqp =
      compile_query("SELECT x AS x1, x AS x2, x AS x3, y AS y1, y AS y2, y AS y3 FROM int_float AS R (x, y)");

  const auto aliases = std::vector<std::string>({"x1", "x2", "x3", "y1", "y2", "y3"});
  const auto expressions =
      expression_vector(int_float_a, int_float_a, int_float_a, int_float_b, int_float_b, int_float_b);

  // clang-format off
  const auto expected_lqp =
  AliasNode::make(expressions, aliases,
    ProjectionNode::make(expressions,
      stored_table_node_int_float));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SQLTranslatorTest, SelectListAliasesDifferentForSimilarColumnsInSubqueryAndFromColumnAliasing) {
  const auto actual_lqp = compile_query(
      "SELECT u, z, w FROM (SELECT a AS a1, b AS b2, b AS b3, a AS a3, b AS b1, a AS a2 FROM int_float)"
      "AS R (y, x, v, w, u, z)");

  const auto outer_aliases = std::vector<std::string>({"u", "z", "w"});
  const auto inner_aliases = std::vector<std::string>({"a1", "b2", "b3", "a3", "b1", "a2"});
  const auto outer_expressions = expression_vector(int_float_b, int_float_a, int_float_a);
  const auto inner_expressions =
      expression_vector(int_float_a, int_float_b, int_float_b, int_float_a, int_float_b, int_float_a);

  // clang-format off
  const auto expected_lqp =
  AliasNode::make(outer_expressions, outer_aliases,
    ProjectionNode::make(outer_expressions,
      AliasNode::make(inner_expressions, inner_aliases,
        ProjectionNode::make(inner_expressions,
          stored_table_node_int_float))));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SQLTranslatorTest, SelectListAliasesDifferentForSimilarColumnsUsedInJoin) {
  const auto actual_lqp = compile_query(
      "SELECT l.a1, l.a2, r.a1 FROM ("
      "  SELECT a AS a1, a AS a2 FROM int_float"
      ") AS l JOIN ("
      "  SELECT a AS a1, a AS a2 FROM int_float2"
      ") AS r ON l.a1 = r.a2;");

  const auto outer_aliases = std::vector<std::string>({"a1", "a2", "a1"});
  const auto inner_aliases = std::vector<std::string>({"a1", "a2"});
  const auto expressions = expression_vector(int_float_a, int_float_a, int_float2_a);

  // clang-format off
  const auto expected_lqp =
  AliasNode::make(expressions, outer_aliases,
    ProjectionNode::make(expressions,
      JoinNode::make(JoinMode::Inner, equals_(int_float_a, int_float2_a),
        AliasNode::make(expression_vector(int_float_a, int_float_a), inner_aliases,
          ProjectionNode::make(expression_vector(int_float_a, int_float_a),
            stored_table_node_int_float)),
        AliasNode::make(expression_vector(int_float2_a, int_float2_a), inner_aliases,
          ProjectionNode::make(expression_vector(int_float2_a, int_float2_a),
            stored_table_node_int_float2)))));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SQLTranslatorTest, SelectListAliasesDifferentForSimilarColumnsUsedInCorrelatedSubquery) {
  const auto actual_lqp = compile_query(
      "SELECT b AS b1, b AS b2 FROM int_float WHERE a < (SELECT MAX(b) FROM int_float2 WHERE int_float2.b > b1)");

  const auto aliases = std::vector<std::string>({"b1", "b2"});
  const auto expressions = expression_vector(int_float_a, int_float_a, int_float2_a);

  // clang-format off
  const auto parameter_int_float_b = correlated_parameter_(ParameterID{0}, int_float_b);
  const auto subquery_lqp =
  AggregateNode::make(expression_vector(), expression_vector(max_(int_float2_b)),
    PredicateNode::make(greater_than_(int_float2_b, parameter_int_float_b),
      stored_table_node_int_float2));
  const auto subquery = lqp_subquery_(subquery_lqp, std::make_pair(ParameterID{0}, int_float_b));

  const auto expected_lqp =
  AliasNode::make(expression_vector(int_float_b, int_float_b), aliases,
    ProjectionNode::make(expression_vector(int_float_b, int_float_b),
      PredicateNode::make(less_than_(int_float_a, subquery),
        stored_table_node_int_float)));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SQLTranslatorTest, SelectListAliasesUsedInView) {
  const auto result_node = compile_query("CREATE VIEW alias_view AS SELECT a AS x, b as y FROM int_float WHERE a > 10");

  // clang-format off
  const auto aliases = std::vector<std::string>({"x", "y"});

  const auto view_lqp =
  AliasNode::make(expression_vector(int_float_a, int_float_b), aliases,
    PredicateNode::make(greater_than_(int_float_a, value_(10)),
      stored_table_node_int_float));

  const auto view_columns = std::unordered_map<ColumnID, std::string>({
                                                                      {ColumnID{0}, "x"},
                                                                      {ColumnID{1}, "y"}
                                                                      });
  // clang-format on

  const auto view = std::make_shared<LQPView>(view_lqp, view_columns);

  const auto expected_lqp = CreateViewNode::make("alias_view", view, false);

  EXPECT_LQP_EQ(result_node, expected_lqp);
}

TEST_F(SQLTranslatorTest, SelectListAliasesDifferentForSimilarColumnsUsedInView) {
  const auto result_node =
      compile_query("CREATE VIEW alias_view AS SELECT a AS a1, a AS a2 FROM int_float WHERE a > 10");

  // clang-format off
  const auto aliases = std::vector<std::string>({"a1", "a2"});

  const auto view_lqp =
  AliasNode::make(expression_vector(int_float_a, int_float_a), aliases,
    ProjectionNode::make(expression_vector(int_float_a, int_float_a),
      PredicateNode::make(greater_than_(int_float_a, value_(10)),
        stored_table_node_int_float)));

  const auto view_columns = std::unordered_map<ColumnID, std::string>({
                                                                      {ColumnID{0}, "a1"},
                                                                      {ColumnID{1}, "a2"}
                                                                      });
  // clang-format on

  const auto view = std::make_shared<LQPView>(view_lqp, view_columns);

  const auto expected_lqp = CreateViewNode::make("alias_view", view, false);

  EXPECT_LQP_EQ(result_node, expected_lqp);
}

TEST_F(SQLTranslatorTest, SelectListManyAliasesDifferentForSimilarColumnsUsedInView) {
  const auto result_node =
      compile_query("CREATE VIEW alias_view (a3, a4) AS SELECT a AS a1, a AS a2 FROM int_float WHERE a > 10");

  // clang-format off
  const auto aliases = std::vector<std::string>({"a1", "a2"});

  const auto view_lqp =
  AliasNode::make(expression_vector(int_float_a, int_float_a), aliases,
    ProjectionNode::make(expression_vector(int_float_a, int_float_a),
      PredicateNode::make(greater_than_(int_float_a, value_(10)),
        stored_table_node_int_float)));

  const auto view_columns = std::unordered_map<ColumnID, std::string>({{ColumnID{0}, "a3"}, {ColumnID{1}, "a4"}});
  // clang-format on

  const auto view = std::make_shared<LQPView>(view_lqp, view_columns);

  const auto expected_lqp = CreateViewNode::make("alias_view", view, false);

  EXPECT_LQP_EQ(result_node, expected_lqp);
}

TEST_F(SQLTranslatorTest, WhereSimple) {
  const auto actual_lqp = compile_query("SELECT a FROM int_float WHERE a < 200;");

  // clang-format off
  const auto expected_lqp =
  ProjectionNode::make(expression_vector(int_float_a),
    PredicateNode::make(less_than_(int_float_a, 200), stored_table_node_int_float));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SQLTranslatorTest, WhereWithArithmetics) {
  const auto actual_lqp = compile_query("SELECT a FROM int_float WHERE a * b >= b + a;");

  const auto a_times_b = mul_(int_float_a, int_float_b);
  const auto b_plus_a = add_(int_float_b, int_float_a);

  // clang-format off
  const auto expected_lqp =
  ProjectionNode::make(expression_vector(int_float_a),
    PredicateNode::make(greater_than_equals_(a_times_b, b_plus_a),
      stored_table_node_int_float));  // NOLINT
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SQLTranslatorTest, WhereWithLike) {
  const auto actual_lqp_a = compile_query("SELECT * FROM int_string WHERE b LIKE '%test1%';");
  const auto actual_lqp_b = compile_query("SELECT * FROM int_string WHERE b NOT LIKE '%test1%';");
  const auto actual_lqp_c = compile_query("SELECT * FROM int_string WHERE b NOT LIKE CONCAT('%test1', '%');");

  // clang-format off
  const auto expected_lqp_a = PredicateNode::make(like_(int_string_b, "%test1%"), stored_table_node_int_string);
  const auto expected_lqp_b = PredicateNode::make(not_like_(int_string_b, "%test1%"), stored_table_node_int_string);
  const auto expected_lqp_c =
  PredicateNode::make(not_like_(int_string_b, concat_("%test1", "%")),
      stored_table_node_int_string);
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp_a, expected_lqp_a);
  EXPECT_LQP_EQ(actual_lqp_b, expected_lqp_b);
  EXPECT_LQP_EQ(actual_lqp_c, expected_lqp_c);
}

TEST_F(SQLTranslatorTest, WhereWithLogical) {
  const auto actual_lqp = compile_query("SELECT a FROM int_float WHERE 5 >= b + a OR (a > 2 AND b > 2);");

  const auto b_plus_a = add_(int_float_b, int_float_a);

  // clang-format off
  const auto predicate = or_(greater_than_equals_(5, add_(int_float_b, int_float_a)),
                             and_(greater_than_(int_float_a, 2), greater_than_(int_float_b, 2)));

  const auto expected_lqp =
  ProjectionNode::make(expression_vector(int_float_a),
    PredicateNode::make(predicate,
      stored_table_node_int_float));  // NOLINT
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SQLTranslatorTest, WhereWithBetween) {
  const auto actual_lqp = compile_query("SELECT a FROM int_float WHERE a BETWEEN b and 5;");

  const auto a_times_b = mul_(int_float_a, int_float_b);
  const auto b_plus_a = add_(int_float_b, int_float_a);

  // clang-format off
  const auto expected_lqp =
  ProjectionNode::make(expression_vector(int_float_a),
    PredicateNode::make(between_inclusive_(int_float_a, int_float_b, 5),
      stored_table_node_int_float));  // NOLINT
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SQLTranslatorTest, WhereIsNull) {
  const auto actual_lqp = compile_query("SELECT b FROM int_float WHERE a + b IS NULL;");

  // clang-format off
  const auto expected_lqp =
  ProjectionNode::make(expression_vector(int_float_b),
    PredicateNode::make(is_null_(add_(int_float_a, int_float_b)),
      stored_table_node_int_float));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SQLTranslatorTest, WhereIsNotNull) {
  const auto actual_lqp = compile_query("SELECT b FROM int_float WHERE a IS NOT NULL;");

  // clang-format off
  const auto expected_lqp =
  ProjectionNode::make(expression_vector(int_float_b),
    PredicateNode::make(is_not_null_(int_float_a),
      stored_table_node_int_float));  // NOLINT
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SQLTranslatorTest, WhereExists) {
  const auto actual_lqp =
      compile_query("SELECT * FROM int_float WHERE EXISTS(SELECT * FROM int_float2 WHERE int_float.a = int_float2.a);");

  // clang-format off
  const auto parameter_int_float_a = correlated_parameter_(ParameterID{0}, int_float_a);
  const auto subquery_lqp =
  PredicateNode::make(equals_(parameter_int_float_a, int_float2_a), stored_table_node_int_float2);
  const auto subquery = lqp_subquery_(subquery_lqp, std::make_pair(ParameterID{0}, int_float_a));

  const auto expected_lqp =
  PredicateNode::make(exists_(subquery),
    stored_table_node_int_float);  // NOLINT
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SQLTranslatorTest, WhereNotExists) {
  const auto actual_lqp =
      compile_query("SELECT * FROM int_float WHERE EXISTS(SELECT * FROM int_float2 WHERE int_float.a = int_float2.a);");

  // clang-format off
  const auto parameter_int_float_a = correlated_parameter_(ParameterID{0}, int_float_a);
  const auto subquery_lqp =
  PredicateNode::make(equals_(parameter_int_float_a, int_float2_a), stored_table_node_int_float2);
  const auto subquery = lqp_subquery_(subquery_lqp, std::make_pair(ParameterID{0}, int_float_a));

  const auto expected_lqp =
  PredicateNode::make(not_exists_(subquery),
                      stored_table_node_int_float);  // NOLINT
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SQLTranslatorTest, WhereWithCorrelatedSubquery) {
  const auto actual_lqp =
      compile_query("SELECT * FROM int_float WHERE a > (SELECT MIN(a + int_float.b) FROM int_float2);");

  const auto parameter_b = correlated_parameter_(ParameterID{0}, int_float_b);

  // clang-format off
  const auto subquery_lqp =
  AggregateNode::make(expression_vector(), expression_vector(min_(add_(int_float2_a, parameter_b))),
    ProjectionNode::make(expression_vector(add_(int_float2_a, parameter_b)),
      stored_table_node_int_float2));
  const auto subquery = lqp_subquery_(subquery_lqp, std::make_pair(ParameterID{0}, int_float_b));

  const auto expected_lqp =
  PredicateNode::make(greater_than_(int_float_a, subquery),
      stored_table_node_int_float);
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SQLTranslatorTest, WhereSimpleNotPredicate) {
  const auto actual_lqp_a = compile_query("SELECT * FROM int_float WHERE NOT (a = b);");
  const auto actual_lqp_b = compile_query("SELECT * FROM int_float WHERE NOT (a != b);");
  const auto actual_lqp_c = compile_query("SELECT * FROM int_float WHERE NOT (a > b);");
  const auto actual_lqp_d = compile_query("SELECT * FROM int_float WHERE NOT (a < b);");
  const auto actual_lqp_e = compile_query("SELECT * FROM int_float WHERE NOT (a >= b);");
  const auto actual_lqp_f = compile_query("SELECT * FROM int_float WHERE NOT (a <= b);");
  const auto actual_lqp_g = compile_query("SELECT * FROM int_float WHERE NOT (a IS NULL);");
  const auto actual_lqp_h = compile_query("SELECT * FROM int_float WHERE NOT (a IS NOT NULL);");

  // clang-format off
  const auto expected_lqp_a = PredicateNode::make(not_equals_(int_float_a, int_float_b), stored_table_node_int_float);
  const auto expected_lqp_b = PredicateNode::make(equals_(int_float_a, int_float_b), stored_table_node_int_float);
  const auto expected_lqp_c = PredicateNode::make(less_than_equals_(int_float_a, int_float_b), stored_table_node_int_float);  // NOLINT
  const auto expected_lqp_d = PredicateNode::make(greater_than_equals_(int_float_a, int_float_b), stored_table_node_int_float);  // NOLINT
  const auto expected_lqp_e = PredicateNode::make(less_than_(int_float_a, int_float_b), stored_table_node_int_float);
  const auto expected_lqp_f = PredicateNode::make(greater_than_(int_float_a, int_float_b), stored_table_node_int_float);
  const auto expected_lqp_g = PredicateNode::make(is_not_null_(int_float_a), stored_table_node_int_float);
  const auto expected_lqp_h = PredicateNode::make(is_null_(int_float_a), stored_table_node_int_float);
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp_a, expected_lqp_a);
  EXPECT_LQP_EQ(actual_lqp_b, expected_lqp_b);
  EXPECT_LQP_EQ(actual_lqp_c, expected_lqp_c);
  EXPECT_LQP_EQ(actual_lqp_d, expected_lqp_d);
  EXPECT_LQP_EQ(actual_lqp_e, expected_lqp_e);
  EXPECT_LQP_EQ(actual_lqp_f, expected_lqp_f);
  EXPECT_LQP_EQ(actual_lqp_g, expected_lqp_g);
  EXPECT_LQP_EQ(actual_lqp_h, expected_lqp_h);
}

TEST_F(SQLTranslatorTest, AggregateWithGroupBy) {
  const auto actual_lqp = compile_query("SELECT SUM(a * 3) * b FROM int_float GROUP BY b");

  const auto a_times_3 = mul_(int_float_a, 3);

  // clang-format off
  const auto expected_lqp =
  ProjectionNode::make(expression_vector(mul_(sum_(a_times_3), int_float_b)),
    AggregateNode::make(expression_vector(int_float_b), expression_vector(sum_(a_times_3)),
      ProjectionNode::make(expression_vector(a_times_3, int_float_b),
        stored_table_node_int_float)));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SQLTranslatorTest, AggregateWithGroupByAndHaving) {
  const auto actual_lqp = compile_query("SELECT b, SUM(a) AS s FROM int_float GROUP BY b HAVING s > 1000");

  const auto select_list_expressions = expression_vector(int_float_b, sum_(int_float_a));
  const auto aliases = std::vector<std::string>({"b", "s"});

  // clang-format off
  const auto expected_lqp =
  AliasNode::make(select_list_expressions, aliases,
    PredicateNode::make(greater_than_(sum_(int_float_a), value_(1000)),
      AggregateNode::make(expression_vector(int_float_b), expression_vector(sum_(int_float_a)),
        stored_table_node_int_float)));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SQLTranslatorTest, AggregateWithGroupByAndUnrelatedHaving) {
  const auto actual_lqp = compile_query("SELECT b, COUNT(a) FROM int_float GROUP BY b HAVING SUM(a) > 1000");

  // clang-format off
  const auto expected_lqp =
  ProjectionNode::make(expression_vector(int_float_b, count_(int_float_a)),
    PredicateNode::make(greater_than_(sum_(int_float_a), value_(1000)),
      AggregateNode::make(expression_vector(int_float_b), expression_vector(count_(int_float_a), sum_(int_float_a)),
        stored_table_node_int_float)));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SQLTranslatorTest, Distinct) {
  const auto actual_lqp = compile_query("SELECT DISTINCT b FROM int_float");

  // clang-format off
  const auto expected_lqp =
  AggregateNode::make(expression_vector(int_float_b), expression_vector(),
    stored_table_node_int_float);
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SQLTranslatorTest, DistinctStar) {
  const auto actual_lqp = compile_query("SELECT DISTINCT * FROM int_float");

  // clang-format off
  const auto expected_lqp =
  AggregateNode::make(expression_vector(int_float_a, int_float_b), expression_vector(),
    stored_table_node_int_float);
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SQLTranslatorTest, DistinctAndGroupBy) {
  const auto actual_lqp = compile_query("SELECT DISTINCT b FROM int_float GROUP BY b");

  // clang-format off
  const auto expected_lqp =
  AggregateNode::make(expression_vector(int_float_b), expression_vector(),
    AggregateNode::make(expression_vector(int_float_b), expression_vector(),
      stored_table_node_int_float));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SQLTranslatorTest, AggregateWithDistinctAndRelatedGroupBy) {
  const auto actual_lqp = compile_query("SELECT DISTINCT b, SUM(a * 3) * b FROM int_float GROUP BY b");

  const auto a_times_3 = mul_(int_float_a, 3);

  // clang-format off
  const auto expected_lqp =
  AggregateNode::make(expression_vector(int_float_b, mul_(sum_(a_times_3), int_float_b)), expression_vector(),
    AggregateNode::make(expression_vector(int_float_b), expression_vector(sum_(a_times_3)),
      ProjectionNode::make(expression_vector(a_times_3, int_float_b),
        stored_table_node_int_float)));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SQLTranslatorTest, AggregateWithDistinctAndUnrelatedGroupBy) {
  const auto actual_lqp = compile_query("SELECT DISTINCT MIN(a) FROM int_float GROUP BY b");

  const auto a_times_3 = mul_(int_float_a, 3);

  // clang-format off
  const auto expected_lqp =
  AggregateNode::make(expression_vector(min_(int_float_a)), expression_vector(),
    AggregateNode::make(expression_vector(int_float_b), expression_vector(min_(int_float_a)),
    stored_table_node_int_float));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SQLTranslatorTest, AggregateCount) {
  const auto actual_lqp_count_a = compile_query("SELECT b, COUNT(a) FROM int_float GROUP BY b");
  // clang-format off
  const auto expected_lqp_a =
  AggregateNode::make(expression_vector(int_float_b), expression_vector(count_(int_float_a)),
    stored_table_node_int_float);
  // clang-format on
  EXPECT_LQP_EQ(actual_lqp_count_a, expected_lqp_a);

  const auto actual_lqp_count_star = compile_query("SELECT b, COUNT(*) FROM int_float GROUP BY b");
  // clang-format off
  const auto expected_lqp_star =
  AggregateNode::make(expression_vector(int_float_b), expression_vector(count_star_()),
    stored_table_node_int_float);
  // clang-format on
  EXPECT_LQP_EQ(actual_lqp_count_star, expected_lqp_star);

  const auto actual_lqp_count_distinct_a_plus_b =
      compile_query("SELECT a, b, COUNT(DISTINCT a + b) FROM int_float GROUP BY a, b");
  // clang-format off
  const auto expected_lqp_count_distinct_a_plus_b =
  AggregateNode::make(expression_vector(int_float_a, int_float_b), expression_vector(count_distinct_(add_(int_float_a, int_float_b))),  // NOLINT
    ProjectionNode::make(expression_vector(add_(int_float_a, int_float_b), int_float_a, int_float_b),
      stored_table_node_int_float));
  // clang-format on
  EXPECT_LQP_EQ(actual_lqp_count_distinct_a_plus_b, expected_lqp_count_distinct_a_plus_b);

  const auto actual_lqp_count_1 = compile_query("SELECT a, COUNT(1) FROM int_float GROUP BY a");
  // clang-format off
  const auto expected_lqp_count_1 =
  AggregateNode::make(expression_vector(int_float_a), expression_vector(count_(value_(1))),
    ProjectionNode::make(expression_vector(value_(1), int_float_a),
      stored_table_node_int_float));
  // clang-format on
  EXPECT_LQP_EQ(actual_lqp_count_1, expected_lqp_count_1);
}

TEST_F(SQLTranslatorTest, GroupByOnly) {
  const auto actual_lqp = compile_query("SELECT * FROM int_float GROUP BY b + 3, a / b, a, b");

  const auto b_plus_3 = add_(int_float_b, 3);
  const auto a_divided_by_b = div_(int_float_a, int_float_b);
  const auto group_by_expressions = expression_vector(b_plus_3, a_divided_by_b, int_float_b);

  // clang-format off
  const auto expected_lqp =
  AggregateNode::make(expression_vector(b_plus_3, a_divided_by_b, int_float_a, int_float_b), expression_vector(),
    ProjectionNode::make(expression_vector(b_plus_3, a_divided_by_b, int_float_a, int_float_b),
      stored_table_node_int_float));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SQLTranslatorTest, AggregateAndGroupByWildcard) {
  // - y is an alias assigned in the SELECT list and can be used in the GROUP BY list
  const auto actual_lqp = compile_query("SELECT int_float.*, b+3 AS y, SUM(a+b) FROM int_float GROUP BY a, y, b");

  const auto sum_a_plus_b = sum_(add_(int_float_a, int_float_b));
  const auto b_plus_3 = add_(int_float_b, 3);

  const auto aliases = std::vector<std::string>({"a", "b", "y", "SUM(a + b)"});
  const auto select_list_expressions =
      expression_vector(int_float_a, int_float_b, b_plus_3, sum_(add_(int_float_a, int_float_b)));

  // clang-format off
  const auto expected_lqp =
  AliasNode::make(select_list_expressions, aliases,
    ProjectionNode::make(select_list_expressions,
      AggregateNode::make(expression_vector(int_float_a, b_plus_3, int_float_b), expression_vector(sum_a_plus_b),
        ProjectionNode::make(expression_vector(add_(int_float_a, int_float_b), int_float_a, b_plus_3, int_float_b),
          stored_table_node_int_float))));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SQLTranslatorTest, AggregateAndGroupByWildcardTwoTables) {
  // - y is an alias assigned in the SELECT list and can be used in the GROUP BY list
  const auto actual_lqp =
      compile_query("SELECT t1.*, t2.a, SUM(t2.b) FROM int_float t1, int_float t2 GROUP BY t1.a, t1.b, t2.a");

  // clang-format off
  const auto expected_lqp =
  AggregateNode::make(expression_vector(int_float_a, int_float_b, int_float_a), expression_vector(sum_(int_float_b)),
    JoinNode::make(JoinMode::Cross, stored_table_node_int_float, stored_table_node_int_float));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SQLTranslatorTest, AggregateForwarding) {
  // Test that a referenced Aggregate does not result in redundant (and illegal!) AggregateNodes

  const auto actual_lqp = compile_query("SELECT x + 3 FROM (SELECT MIN(a) as x FROM int_float) AS t;");

  // clang-format off
  const auto expected_lqp =
  ProjectionNode::make(expression_vector(add_(min_(int_float_a), 3)),
    AliasNode::make(expression_vector(min_(int_float_a)), std::vector<std::string>({"x"}),
      AggregateNode::make(expression_vector(), expression_vector(min_(int_float_a)),
        stored_table_node_int_float)));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SQLTranslatorTest, SubqueryFromSimple) {
  const auto actual_lqp_a = compile_query("SELECT z.x, z.a, z.b FROM (SELECT a + b AS x, * FROM int_float) AS z");
  const auto actual_lqp_b = compile_query("SELECT * FROM (SELECT a + b AS x, * FROM int_float) AS z");
  const auto actual_lqp_c = compile_query("SELECT z.* FROM (SELECT a + b AS x, * FROM int_float) AS z");

  const auto expressions = expression_vector(add_(int_float_a, int_float_b), int_float_a, int_float_b);
  const auto aliases = std::vector<std::string>({"x", "a", "b"});

  // #1186: Redundant AliasNode due to the SQLTranslator architecture.
  // clang-format off
  const auto expected_lqp =
  AliasNode::make(expressions, aliases,
    AliasNode::make(expressions, aliases,
      ProjectionNode::make(expressions, stored_table_node_int_float)));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp_a, expected_lqp);
  EXPECT_LQP_EQ(actual_lqp_b, expected_lqp);
  EXPECT_LQP_EQ(actual_lqp_c, expected_lqp);
}

TEST_F(SQLTranslatorTest, SubquerySelectList) {
  const auto actual_lqp = compile_query("SELECT (SELECT MIN(a + d) FROM int_float), a FROM int_float5 AS f");

  // clang-format off
  const auto parameter_d = correlated_parameter_(ParameterID{0}, int_float5_d);
  const auto a_plus_d = add_(int_float_a, parameter_d);
  const auto subquery_lqp =
  AggregateNode::make(expression_vector(), expression_vector(min_(a_plus_d)),
    ProjectionNode::make(expression_vector(a_plus_d), stored_table_node_int_float));
  // clang-format on

  const auto subquery = lqp_subquery_(subquery_lqp, std::make_pair(ParameterID{0}, int_float5_d));

  // clang-format off
  const auto expected_lqp =
  ProjectionNode::make(expression_vector(subquery, int_float5_a),
     stored_table_node_int_float5);
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SQLTranslatorTest, OrderByTest) {
  const auto actual_lqp = compile_query("SELECT * FROM int_float ORDER BY a, a+b DESC, b ASC");

  const auto order_by_modes =
      std::vector<OrderByMode>({OrderByMode::Ascending, OrderByMode::Descending, OrderByMode::Ascending});

  // clang-format off
  const auto expected_lqp =
  ProjectionNode::make(expression_vector(int_float_a, int_float_b),
    SortNode::make(expression_vector(int_float_a, add_(int_float_a, int_float_b), int_float_b), order_by_modes,
      ProjectionNode::make(expression_vector(add_(int_float_a, int_float_b), int_float_a, int_float_b),
        stored_table_node_int_float)));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SQLTranslatorTest, InArray) {
  const auto actual_lqp = compile_query("SELECT * FROM int_float WHERE a + 7 IN (1+2,3,4)");

  // clang-format off
  const auto expected_lqp =
  PredicateNode::make(in_(add_(int_float_a, 7), list_(add_(1, 2), 3, 4)),
    stored_table_node_int_float);
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SQLTranslatorTest, NotInArray) {
  const auto actual_lqp = compile_query("SELECT * FROM int_float WHERE a + 7 NOT IN (1+2,3,4)");

  // clang-format off
  const auto expected_lqp =
  PredicateNode::make(not_in_(add_(int_float_a, 7), list_(add_(1, 2), 3, 4)),
    stored_table_node_int_float);
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SQLTranslatorTest, InSelect) {
  const auto actual_lqp = compile_query("SELECT * FROM int_float WHERE a + 7 IN (SELECT * FROM int_float2)");

  // clang-format off
  const auto subquery_lqp = stored_table_node_int_float2;
  const auto subquery = lqp_subquery_(subquery_lqp);

  // clang-format off
  const auto expected_lqp =
  PredicateNode::make(in_(add_(int_float_a, 7), subquery),
    stored_table_node_int_float);
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SQLTranslatorTest, InCorrelatedSubquery) {
  const auto actual_lqp = compile_query(
      "SELECT * FROM int_float WHERE a IN (SELECT * FROM int_float2 WHERE int_float.b * int_float.a * int_float.a > "
      "b)");

  // clang-format off
  const auto parameter_a = correlated_parameter_(ParameterID{1}, int_float_a);
  const auto parameter_b = correlated_parameter_(ParameterID{0}, int_float_b);

  const auto b_times_a_times_a = mul_(mul_(parameter_b, parameter_a), parameter_a);

  const auto subquery_lqp =
  PredicateNode::make(greater_than_(b_times_a_times_a, int_float2_b),
      stored_table_node_int_float2);

  const auto subquery = lqp_subquery_(subquery_lqp, std::make_pair(ParameterID{1}, int_float_a),
                                  std::make_pair(ParameterID{0}, int_float_b));

  // clang-format off
  const auto expected_lqp =
    PredicateNode::make(in_(int_float_a, subquery),
      stored_table_node_int_float);
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SQLTranslatorTest, JoinSimple) {
  const auto actual_lqp_a = compile_query("SELECT * FROM int_float JOIN int_float2 ON int_float2.a > int_float.a");
  const auto actual_lqp_b = compile_query("SELECT * FROM int_float LEFT JOIN int_float2 ON int_float2.a > int_float.a");
  const auto actual_lqp_c =
      compile_query("SELECT * FROM int_float RIGHT JOIN int_float2 ON int_float2.a > int_float.a");
  const auto actual_lqp_d =
      compile_query("SELECT * FROM int_float FULL OUTER JOIN int_float2 ON int_float2.a > int_float.a");

  const auto a_gt_a = greater_than_(int_float2_a, int_float_a);
  const auto node_a = stored_table_node_int_float;
  const auto node_b = stored_table_node_int_float2;

  const auto expected_lqp_a = JoinNode::make(JoinMode::Inner, a_gt_a, node_a, node_b);
  const auto expected_lqp_b = JoinNode::make(JoinMode::Left, a_gt_a, node_a, node_b);
  const auto expected_lqp_c = JoinNode::make(JoinMode::Right, a_gt_a, node_a, node_b);
  const auto expected_lqp_d = JoinNode::make(JoinMode::FullOuter, a_gt_a, node_a, node_b);

  EXPECT_LQP_EQ(actual_lqp_a, expected_lqp_a);
  EXPECT_LQP_EQ(actual_lqp_b, expected_lqp_b);
  EXPECT_LQP_EQ(actual_lqp_c, expected_lqp_c);
  EXPECT_LQP_EQ(actual_lqp_d, expected_lqp_d);
}

TEST_F(SQLTranslatorTest, JoinCrossSelectStar) {
  const auto actual_lqp = compile_query("SELECT * FROM int_float, int_float2 AS t, int_float5 WHERE t.a < 2");

  // clang-format off
  const auto expected_lqp =
  PredicateNode::make(less_than_(int_float2_a, 2),
    JoinNode::make(JoinMode::Cross,
      JoinNode::make(JoinMode::Cross,
        stored_table_node_int_float,
        stored_table_node_int_float2),
    stored_table_node_int_float5));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SQLTranslatorTest, JoinCrossSelectElements) {
  const auto actual_lqp =
      compile_query("SELECT int_float5.d, t.* FROM int_float, int_float2 AS t, int_float5 WHERE t.a < 2");

  // clang-format off
  const auto expected_lqp =
  ProjectionNode::make(expression_vector(int_float5_d, int_float2_a, int_float2_b),
    PredicateNode::make(less_than_(int_float2_a, 2),
      JoinNode::make(JoinMode::Cross,
        JoinNode::make(JoinMode::Cross,
          stored_table_node_int_float,
          stored_table_node_int_float2),
      stored_table_node_int_float5)));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SQLTranslatorTest, JoinLeftRightFullOuter) {
  const auto actual_lqp_left = compile_query("SELECT * FROM int_float AS a LEFT JOIN int_float2 AS b ON a.a = b.a;");

  // clang-format off
  const auto expected_lqp_left =
  JoinNode::make(JoinMode::Left, equals_(int_float_a, int_float2_a),
    stored_table_node_int_float,
    stored_table_node_int_float2);
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp_left, expected_lqp_left);

  const auto actual_lqp_right = compile_query("SELECT * FROM int_float AS a RIGHT JOIN int_float2 AS b ON a.a = b.a;");

  // clang-format off
  const auto expected_lqp_right =
  JoinNode::make(JoinMode::Right, equals_(int_float_a, int_float2_a),
    stored_table_node_int_float,
    stored_table_node_int_float2);
  // clang-format on
  EXPECT_LQP_EQ(actual_lqp_right, expected_lqp_right);

  const auto actual_lqp_full = compile_query("SELECT * FROM int_float AS a FULL JOIN int_float2 AS b ON a.a = b.a;");

  // clang-format off
  const auto expected_lqp_full_outer =
  JoinNode::make(JoinMode::FullOuter, equals_(int_float_a, int_float2_a),
    stored_table_node_int_float,
    stored_table_node_int_float2);
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp_full, expected_lqp_full_outer);
}

TEST_F(SQLTranslatorTest, JoinSemiOuterPredicatesForNullSupplyingSide) {
  // Test that predicates in the JOIN condition that reference only the null-supplying side are pushed down

  const auto actual_lqp_left = compile_query(
      "SELECT"
      "  * "
      "FROM "
      "  int_float AS a LEFT JOIN int_float2 AS b "
      "    ON b.a > 5 AND a.a = b.a "
      "WHERE b.b < 2;");

  // clang-format off

  const auto expected_lqp_left =
  PredicateNode::make(less_than_(int_float2_b, 2),
      JoinNode::make(JoinMode::Left, equals_(int_float_a, int_float2_a),
        stored_table_node_int_float,
        PredicateNode::make(greater_than_(int_float2_a, 5),
          stored_table_node_int_float2)));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp_left, expected_lqp_left);

  const auto actual_lqp_right = compile_query(
      "SELECT"
      "  * "
      "FROM "
      "  int_float AS a RIGHT JOIN int_float2 AS b "
      "    ON a.a > 5 AND a.a = b.a "
      "WHERE b.b < 2;");

  // clang-format off

  const auto expected_lqp_right =
  PredicateNode::make(less_than_(int_float2_b, 2),
      JoinNode::make(JoinMode::Right, equals_(int_float_a, int_float2_a),
        PredicateNode::make(greater_than_(int_float_a, 5),
          stored_table_node_int_float),
        stored_table_node_int_float2));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp_right, expected_lqp_right);
}

TEST_F(SQLTranslatorTest, JoinOuterPredicatesForNullPreservingSide) {
  // See #1436

  EXPECT_THROW(compile_query("SELECT * FROM int_float AS a LEFT JOIN int_float2 AS b ON a.a > 5 AND a.a = b.a"),
               InvalidInputException);

  EXPECT_THROW(compile_query("SELECT * FROM int_float AS a RIGHT JOIN int_float2 AS b ON b.a > 5 AND a.a = b.a"),
               InvalidInputException);

  EXPECT_THROW(compile_query("SELECT * FROM int_float AS a FULL JOIN int_float2 AS b ON a.a > 5 AND a.a = b.a"),
               InvalidInputException);

  EXPECT_THROW(compile_query("SELECT * FROM int_float AS a FULL JOIN int_float2 AS b ON b.a > 5 AND a.a = b.a"),
               InvalidInputException);
}

TEST_F(SQLTranslatorTest, JoinNaturalSimple) {
  // Also test that columns can be referenced after a natural join

  const auto actual_lqp = compile_query(
      "SELECT "
      "  * "
      "FROM "
      "  int_float AS a NATURAL JOIN int_float2 AS b "
      "WHERE "
      "  a.b > 10 AND a.a > 5");

  // clang-format off
  const auto expected_lqp =
  PredicateNode::make(greater_than_(int_float_b, 10),
    PredicateNode::make(greater_than_(int_float_a, 5),
      ProjectionNode::make(expression_vector(int_float_a, int_float_b),
        PredicateNode::make(equals_(int_float_b, int_float2_b),
          JoinNode::make(JoinMode::Inner, equals_(int_float_a, int_float2_a),
            stored_table_node_int_float,
            stored_table_node_int_float2)))));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SQLTranslatorTest, JoinNaturalColumnAlias) {
  // Test that the Natural join can work with column aliases and that the output columns have the correct name

  const auto actual_lqp = compile_query(
      "SELECT "
      "  * "
      "FROM "
      "  int_float AS a NATURAL JOIN (SELECT a AS d, b AS a, c FROM int_int_int) AS b");

  const auto aliases = std::vector<std::string>{{"a", "b", "d", "c"}};
  const auto subquery_aliases = std::vector<std::string>{{"d", "a", "c"}};

  // clang-format off
  const auto expected_lqp =
  AliasNode::make(expression_vector(int_float_a, int_float_b, int_int_int_a, int_int_int_c), aliases,
    ProjectionNode::make(expression_vector(int_float_a, int_float_b, int_int_int_a, int_int_int_c),
      JoinNode::make(JoinMode::Inner, equals_(int_float_a, int_int_int_b),
        stored_table_node_int_float,
        AliasNode::make(expression_vector(int_int_int_a, int_int_int_b, int_int_int_c), subquery_aliases,
          stored_table_node_int_int_int))));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SQLTranslatorTest, JoinInnerComplexPredicateA) {
  const auto actual_lqp = compile_query(
      "SELECT * FROM int_float JOIN int_float2 ON int_float.a + int_float2.a = int_float2.b * int_float.a;");

  // clang-format off
  const auto a_plus_a = add_(int_float_a, int_float2_a);
  const auto b_times_a = mul_(int_float2_b, int_float_a);
  const auto expected_lqp =
  PredicateNode::make(equals_(a_plus_a, b_times_a),
    JoinNode::make(JoinMode::Cross,
      stored_table_node_int_float,
      stored_table_node_int_float2));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SQLTranslatorTest, JoinInnerComplexPredicateB) {
  const auto actual_lqp =
      compile_query("SELECT * FROM int_float AS m1 JOIN int_float AS m2 ON m1.a * 3 = m2.a - 5 OR m1.a > 20;");

  // clang-format off
  const auto a_times_3 = mul_(int_float_a, 3);
  const auto a_minus_5 = sub_(int_float_a, 5);

  const auto join_cross = JoinNode::make(JoinMode::Cross, stored_table_node_int_float, stored_table_node_int_float);
  const auto join_predicate = or_(equals_(mul_(int_float_a, 3), sub_(int_float_a, 5)), greater_than_(int_float_a, 20));

  const auto expected_lqp =
  PredicateNode::make(join_predicate,
    JoinNode::make(JoinMode::Cross,
      stored_table_node_int_float,
      stored_table_node_int_float));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SQLTranslatorTest, FromColumnAliasingSimple) {
  const auto actual_lqp_a = compile_query("SELECT t.x FROM int_float AS t (x, y) WHERE x = t.y");
  const auto actual_lqp_b = compile_query("SELECT t.x FROM (SELECT * FROM int_float) AS t (x, y) WHERE x = t.y");

  // clang-format off
  const auto expected_lqp =
  AliasNode::make(expression_vector(int_float_a), std::vector<std::string>({"x", }),
    ProjectionNode::make(expression_vector(int_float_a),
      PredicateNode::make(equals_(int_float_a, int_float_b),
        stored_table_node_int_float)));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp_a, expected_lqp);
  EXPECT_LQP_EQ(actual_lqp_b, expected_lqp);
}

TEST_F(SQLTranslatorTest, FromColumnAliasingColumnsSwitchNames) {
  // Tricky: Columns "switch names". a becomes b and b becomes a

  const auto actual_lqp_a = compile_query("SELECT * FROM int_float AS t (b, a) WHERE b = t.a");
  const auto actual_lqp_b = compile_query("SELECT * FROM (SELECT * FROM int_float) AS t (b, a) WHERE b = t.a");

  // clang-format off
  const auto expected_lqp =
  AliasNode::make(expression_vector(int_float_a, int_float_b), std::vector<std::string>({"b", "a"}),
    PredicateNode::make(equals_(int_float_a, int_float_b),
      stored_table_node_int_float));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp_a, expected_lqp);
  EXPECT_LQP_EQ(actual_lqp_b, expected_lqp);
}

TEST_F(SQLTranslatorTest, FromColumnAliasingTablesSwitchNames) {
  // Tricky: Tables "switch names". int_float becomes int_float2 and int_float2 becomes int_float

  const auto actual_lqp_a = compile_query(
      "SELECT int_float.y, int_float2.* "
      "FROM int_float AS int_float2 (a, b), int_float2 AS int_float(x,y) "
      "WHERE int_float.x = int_float2.b");
  const auto actual_lqp_b = compile_query(
      "SELECT int_float.y, int_float2.* "
      "FROM (SELECT * FROM int_float) AS int_float2 (a, b), (SELECT * FROM int_float2) AS int_float(x,y) "
      "WHERE int_float.x = int_float2.b");

  // clang-format off
  const auto expected_lqp =
  AliasNode::make(expression_vector(int_float2_b, int_float_a, int_float_b), std::vector<std::string>({"y", "a", "b"}),
    ProjectionNode::make(expression_vector(int_float2_b, int_float_a, int_float_b),
      PredicateNode::make(equals_(int_float2_a, int_float_b),
        JoinNode::make(JoinMode::Cross,
          stored_table_node_int_float,
          stored_table_node_int_float2))));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp_a, expected_lqp);
  EXPECT_LQP_EQ(actual_lqp_b, expected_lqp);
}

TEST_F(SQLTranslatorTest, SameColumnForDifferentTableNames) {
  const auto actual_lqp = compile_query("SELECT R.a, S.a FROM int_float AS R, int_float AS S");

  // clang-format off
  const auto expected_lqp =
  ProjectionNode::make(expression_vector(int_float_a, int_float_a),
    JoinNode::make(JoinMode::Cross,
      stored_table_node_int_float,
      stored_table_node_int_float));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SQLTranslatorTest, LimitLiteral) {
  // Most common case: LIMIT to a fixed number
  const auto actual_lqp = compile_query("SELECT * FROM int_float LIMIT 1;");
  const auto expected_lqp = LimitNode::make(value_(1), stored_table_node_int_float);
  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SQLTranslatorTest, LimitExpression) {
  // Uncommon: LIMIT to the result of an Expression (which has to be uncorrelated)
  const auto actual_lqp = compile_query("SELECT * FROM int_float LIMIT 3 + (SELECT MIN(b) FROM int_float2);");

  // clang-format off
  const auto subquery =
  AggregateNode::make(expression_vector(), expression_vector(min_(int_float2_b)),
    stored_table_node_int_float2);

  const auto expected_lqp =
  LimitNode::make(add_(3, lqp_subquery_(subquery)),
    stored_table_node_int_float);
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SQLTranslatorTest, Extract) {
  std::vector<DatetimeComponent> components{DatetimeComponent::Year,   DatetimeComponent::Month,
                                            DatetimeComponent::Day,    DatetimeComponent::Hour,
                                            DatetimeComponent::Minute, DatetimeComponent::Second};

  std::shared_ptr<opossum::AbstractLQPNode> actual_lqp;
  std::shared_ptr<opossum::AbstractLQPNode> expected_lqp;
  ProjectionNode::make(expression_vector(extract_(DatetimeComponent::Year, "1993-08-01")), DummyTableNode::make());

  for (const auto& component : components) {
    std::stringstream query_str;
    query_str << "SELECT EXTRACT(" << component << " FROM '1993-08-01');";

    actual_lqp = compile_query(query_str.str());
    expected_lqp = ProjectionNode::make(expression_vector(extract_(component, "1993-08-01")), DummyTableNode::make());

    EXPECT_LQP_EQ(actual_lqp, expected_lqp);
  }
}

TEST_F(SQLTranslatorTest, ValuePlaceholders) {
  const auto actual_lqp = compile_query("SELECT a + ?, ? FROM int_float WHERE a > ?");

  // clang-format off
  const auto expected_lqp =
  ProjectionNode::make(expression_vector(add_(int_float_a, placeholder_(ParameterID{0})),
                                         placeholder_(ParameterID{1})),
    PredicateNode::make(greater_than_(int_float_a, placeholder_(ParameterID{2})),
      stored_table_node_int_float));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SQLTranslatorTest, ValuePlaceholdersInSubselect) {
  // clang-format off
  const auto [actual_lqp, parameter_ids_of_value_placeholders] = compile_prepared_query(
      "SELECT ? + (SELECT a + ? "
                  "FROM int_float2) "
      "FROM (SELECT a "
            "FROM int_float WHERE ? > (SELECT a + ? "
                                      "FROM int_string)) s1");

  ASSERT_EQ(parameter_ids_of_value_placeholders.size(), 4u);
  EXPECT_EQ(parameter_ids_of_value_placeholders.at(0), ParameterID{2});
  EXPECT_EQ(parameter_ids_of_value_placeholders.at(1), ParameterID{3});
  EXPECT_EQ(parameter_ids_of_value_placeholders.at(2), ParameterID{0});
  EXPECT_EQ(parameter_ids_of_value_placeholders.at(3), ParameterID{1});

  const auto placeholder_0 = placeholder_(ParameterID{2});
  const auto placeholder_1 = placeholder_(ParameterID{3});
  const auto placeholder_2 = placeholder_(ParameterID{0});
  const auto placeholder_3 = placeholder_(ParameterID{1});

  const auto subquery_a_lqp =
  ProjectionNode::make(expression_vector(add_(int_float2_a, placeholder_1)),
                       stored_table_node_int_float2);
  const auto subquery_a = lqp_subquery_(subquery_a_lqp);
  const auto subquery_b_lqp =
  ProjectionNode::make(expression_vector(add_(int_string_a, placeholder_3)),
    stored_table_node_int_string);
  const auto subquery_b = lqp_subquery_(subquery_b_lqp);

  const auto expected_lqp =
  ProjectionNode::make(expression_vector(add_(placeholder_0, subquery_a)),
    ProjectionNode::make(expression_vector(int_float_a),
      PredicateNode::make(greater_than_(placeholder_2, subquery_b),
        stored_table_node_int_float)));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SQLTranslatorTest, ParameterIDAllocationSimple) {
  /**
   * Test that ParameterIDs are correctly allocated to correlated Parameters
   */

  const auto query = "SELECT (SELECT (SELECT int_float2.a + int_float.b) FROM int_float2) FROM int_float";

  hsql::SQLParserResult parser_result;
  hsql::SQLParser::parseSQLString(query, &parser_result);
  Assert(parser_result.isValid(), create_sql_parser_error_message(query, parser_result));

  SQLTranslator sql_translator{UseMvcc::No};

  const auto actual_lqp = sql_translator.translate_parser_result(parser_result).at(0);

  // clang-format off
  const auto parameter_int_float_b = correlated_parameter_(ParameterID{1}, int_float_b);
  const auto parameter_int_float2_a = correlated_parameter_(ParameterID{0}, int_float2_a);

  // "(SELECT int_float2.a + int_float.b)"
  const auto expected_sub_subquery_lqp =
  ProjectionNode::make(expression_vector(add_(parameter_int_float2_a, parameter_int_float_b)),
     DummyTableNode::make());
  const auto sub_subquery = lqp_subquery_(expected_sub_subquery_lqp,
                                      std::make_pair(ParameterID{0}, int_float2_a));


  // "(SELECT (SELECT int_float2.a + int_float.b) FROM int_float2)"
  const auto expected_subquery_lqp =
  ProjectionNode::make(expression_vector(sub_subquery),
    stored_table_node_int_float2);
  const auto expected_subquery = lqp_subquery_(expected_subquery_lqp, std::make_pair(ParameterID{1}, int_float_b));

  const auto expected_lqp =
  ProjectionNode::make(expression_vector(expected_subquery),
    stored_table_node_int_float);
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SQLTranslatorTest, ParameterIDAllocation) {
  /**
   * Test that ParameterIDs are correctly allocated to ValuePlaceholders and External Parameters
   */
  // clang-format off
  const auto query =
      "SELECT ?, (SELECT ? + MAX(b) + (SELECT int_float2.a + ? + int_float2.b) "
                 "FROM int_float2) "
      "FROM (SELECT a + ? AS k "
            "FROM int_float) s1 WHERE k > (SELECT ? "
                                          "FROM int_string)";

  // NOLINTNEXTLINE
  const auto [actual_lqp, parameter_ids_of_value_placeholders] = compile_prepared_query(query);

  ASSERT_EQ(parameter_ids_of_value_placeholders.size(), 5u);
  EXPECT_EQ(parameter_ids_of_value_placeholders.at(0), ParameterID{1});
  EXPECT_EQ(parameter_ids_of_value_placeholders.at(1), ParameterID{2});
  EXPECT_EQ(parameter_ids_of_value_placeholders.at(2), ParameterID{4});
  EXPECT_EQ(parameter_ids_of_value_placeholders.at(3), ParameterID{0});
  EXPECT_EQ(parameter_ids_of_value_placeholders.at(4), ParameterID{6});

  const auto placeholder_0 = placeholder_(ParameterID{1});
  const auto placeholder_1 = placeholder_(ParameterID{2});
  const auto placeholder_2 = placeholder_(ParameterID{4});
  const auto placeholder_3 = placeholder_(ParameterID{0});
  const auto placeholder_4 = placeholder_(ParameterID{6});

  const auto parameter_int_float2_a = correlated_parameter_(ParameterID{3}, int_float2_a);
  const auto parameter_int_float2_b = correlated_parameter_(ParameterID{5}, int_float2_b);

  // SELECT int_float2.a + ? + int_float2.b
  const auto subquery_a_lqp =
  ProjectionNode::make(expression_vector(add_(add_(parameter_int_float2_a, placeholder_2), parameter_int_float2_b)),
    DummyTableNode::make());
  const auto subquery_a = lqp_subquery_(subquery_a_lqp, std::make_pair(ParameterID{5}, int_float2_b),
    std::make_pair(ParameterID{3}, int_float2_a));

  // (SELECT ? + MAX(b) + (subquery_a) FROM int_float2)
  const auto subquery_b_lqp =
  ProjectionNode::make(expression_vector(add_(add_(placeholder_1, max_(int_float2_b)), subquery_a)),
    AggregateNode::make(expression_vector(), expression_vector(max_(int_float2_b)),
      stored_table_node_int_float2));
  const auto subquery_b = lqp_subquery_(subquery_b_lqp);

  // SELECT ? FROM int_string
  const auto subquery_c_lqp = ProjectionNode::make(expression_vector(placeholder_4), stored_table_node_int_string);
  const auto subquery_c = lqp_subquery_(subquery_c_lqp);

  const auto expected_lqp =
  ProjectionNode::make(expression_vector(placeholder_0, subquery_b),
    PredicateNode::make(greater_than_(add_(int_float_a, placeholder_3), subquery_c),
      AliasNode::make(expression_vector(add_(int_float_a, placeholder_3)), std::vector<std::string>{"k"},
        ProjectionNode::make(expression_vector(add_(int_float_a, placeholder_3)),
          stored_table_node_int_float))));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SQLTranslatorTest, UseMvcc) {
  const auto query = "SELECT * FROM int_float, int_float2 WHERE int_float.a = int_float2.b";

  hsql::SQLParserResult parser_result;
  hsql::SQLParser::parseSQLString(query, &parser_result);
  Assert(parser_result.isValid(), create_sql_parser_error_message(query, parser_result));

  const auto lqp_a = SQLTranslator{UseMvcc::No}.translate_parser_result(parser_result).at(0);
  const auto lqp_b = SQLTranslator{UseMvcc::Yes}.translate_parser_result(parser_result).at(0);

  EXPECT_FALSE(lqp_is_validated(lqp_a));
  EXPECT_TRUE(lqp_is_validated(lqp_b));
}

TEST_F(SQLTranslatorTest, Substr) {
  const auto actual_lqp_a = compile_query("SELECT SUBSTR('Hello', 3, 2 + 3)");
  const auto actual_lqp_b = compile_query("SELECT substr('Hello', 3, 2 + 3)");
  const auto actual_lqp_c = compile_query("SELECT substring('Hello', 3, 2 + 3)");

  // clang-format off
  const auto expected_lqp =
  ProjectionNode::make(expression_vector(substr_("Hello", 3, add_(2, 3))),
    DummyTableNode::make());
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp_a, expected_lqp);
  EXPECT_LQP_EQ(actual_lqp_b, expected_lqp);
  EXPECT_LQP_EQ(actual_lqp_c, expected_lqp);
}

TEST_F(SQLTranslatorTest, Exists) {
  const auto actual_lqp = compile_query("SELECT EXISTS(SELECT * FROM int_float);");

  // clang-format off
  const auto expected_lqp =
  ProjectionNode::make(expression_vector(exists_(lqp_subquery_(stored_table_node_int_float))),
    DummyTableNode::make());
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SQLTranslatorTest, NotExists) {
  const auto actual_lqp = compile_query("SELECT NOT EXISTS(SELECT * FROM int_float);");

  // clang-format off
  const auto expected_lqp =
  ProjectionNode::make(expression_vector(not_exists_(lqp_subquery_(stored_table_node_int_float))),
    DummyTableNode::make());
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SQLTranslatorTest, ExistsCorrelated) {
  const auto actual_lqp =
      compile_query("SELECT EXISTS(SELECT * FROM int_float WHERE int_float.a > int_float2.b) FROM int_float2");

  // clang-format off
  const auto subquery_lqp =
  PredicateNode::make(greater_than_(int_float_a, correlated_parameter_(ParameterID{0}, int_float2_b)),
    stored_table_node_int_float);
  const auto subquery = lqp_subquery_(subquery_lqp, std::make_pair(ParameterID{0}, int_float2_b));

  const auto expected_lqp =
  ProjectionNode::make(expression_vector(exists_(subquery)),
    stored_table_node_int_float2);
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SQLTranslatorTest, UnaryMinus) {
  const auto actual_lqp = compile_query("SELECT -a FROM int_float");

  // clang-format off
  const auto expected_lqp =
  ProjectionNode::make(expression_vector(unary_minus_(int_float_a)),
    stored_table_node_int_float);
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SQLTranslatorTest, ShowTables) {
  const auto actual_lqp = compile_query("SHOW TABLES");

  const auto expected_lqp = StoredTableNode::make(MetaTableManager::META_PREFIX + "tables");

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SQLTranslatorTest, ShowColumns) {
  const auto actual_lqp = compile_query("SHOW COLUMNS int_float");

  // clang-format off
  const auto stored_table_node = StoredTableNode::make(MetaTableManager::META_PREFIX + "columns");
  const auto table_name_column = stored_table_node->get_column("table");
  const auto expected_lqp =
      PredicateNode::make(equals_(table_name_column, "int_float"),
        stored_table_node);
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SQLTranslatorTest, DMLOnMetatables) {
  EXPECT_THROW(compile_query("UPDATE meta_tables SET table_name = 'foo';"), InvalidInputException);
  EXPECT_THROW(compile_query("DELETE FROM meta_tables;"), InvalidInputException);
  EXPECT_THROW(compile_query("INSERT INTO meta_tables SELECT * FROM meta_tables;"), InvalidInputException);
}

TEST_F(SQLTranslatorTest, InsertValues) {
  const auto actual_lqp = compile_query("INSERT INTO int_float VALUES (10, 12.5);");

  // clang-format off
  const auto expected_lqp =
  InsertNode::make("int_float",
   ProjectionNode::make(expression_vector(10, cast_(12.5, DataType::Float)),
     DummyTableNode::make()));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SQLTranslatorTest, InsertValuesColumnReorder) {
  const auto actual_lqp = compile_query("INSERT INTO int_float (b, a) VALUES (12.5, 10);");

  // clang-format off
  const auto expected_lqp =
  InsertNode::make("int_float",
    ProjectionNode::make(expression_vector(10, cast_(12.5, DataType::Float)),
        DummyTableNode::make()));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SQLTranslatorTest, InsertValuesColumnSubset) {
  const auto actual_lqp = compile_query("INSERT INTO int_float (b) VALUES (12.5);");

  // clang-format off
  const auto expected_lqp =
  InsertNode::make("int_float",
    ProjectionNode::make(expression_vector(cast_(null_(), DataType::Int), cast_(12.5, DataType::Float)),
      DummyTableNode::make()));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SQLTranslatorTest, InsertNull) {
  const auto actual_lqp = compile_query("INSERT INTO int_float (b, a) VALUES (12.5, NULL);");

  // clang-format off
  const auto expected_lqp =
  InsertNode::make("int_float",
    ProjectionNode::make(expression_vector(cast_(null_(), DataType::Int), cast_(12.5, DataType::Float)),
      DummyTableNode::make()));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SQLTranslatorTest, InsertSubquery) {
  const auto actual_lqp = compile_query("INSERT INTO int_float SELECT a, b FROM int_float2 WHERE a > 5;");

  // clang-format off
  const auto expected_lqp =
  InsertNode::make("int_float",
    PredicateNode::make(greater_than_(int_float2_a, 5),
      stored_table_node_int_float2));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SQLTranslatorTest, InsertConvertibleType) {
  const auto actual_lqp = compile_query("INSERT INTO int_float VALUES (5.5, 12)");

  // clang-format off
  const auto expected_lqp =
  InsertNode::make("int_float",
    ProjectionNode::make(expression_vector(cast_(5.5, DataType::Int), cast_(12, DataType::Float)),
      DummyTableNode::make()));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SQLTranslatorTest, DeleteWithoutMVCC) {
  EXPECT_THROW(compile_query("DELETE FROM int_float;"), std::logic_error);
}

TEST_F(SQLTranslatorTest, DeleteSimple) {
  const auto actual_lqp = compile_query("DELETE FROM int_float", UseMvcc::Yes);

  // clang-format off
  const auto expected_lqp =
  DeleteNode::make(
    ValidateNode::make(
      StoredTableNode::make("int_float")));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SQLTranslatorTest, DeleteConditional) {
  const auto actual_lqp = compile_query("DELETE FROM int_float WHERE a > 5", UseMvcc::Yes);

  // clang-format off
  const auto expected_lqp =
  DeleteNode::make(
    PredicateNode::make(greater_than_(int_float_a, 5),
      ValidateNode::make(
        stored_table_node_int_float)));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SQLTranslatorTest, UpdateWithoutMVCC) {
  EXPECT_THROW(compile_query("UPDATE int_float SET b = 3.2 WHERE a > 1;"), std::logic_error);
}

TEST_F(SQLTranslatorTest, UpdateUnconditional) {
  const auto actual_lqp = compile_query("UPDATE int_float SET b = b + 1", UseMvcc::Yes);

  // clang-format off
  const auto row_subquery_lqp =
  ValidateNode::make(
    stored_table_node_int_float);

  const auto expected_lqp =
  UpdateNode::make("int_float",
    row_subquery_lqp,
    ProjectionNode::make(expression_vector(int_float_a, add_(int_float_b, value_(1))),
      row_subquery_lqp));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SQLTranslatorTest, UpdateConditional) {
  const auto actual_lqp = compile_query("UPDATE int_float SET b = 3.2 WHERE a > 1;", UseMvcc::Yes);

  // clang-format off
  const auto row_subquery_lqp =
  PredicateNode::make(greater_than_(int_float_a, 1),
    ValidateNode::make(
      stored_table_node_int_float));

  const auto expected_lqp =
  UpdateNode::make("int_float",
    row_subquery_lqp,
    ProjectionNode::make(expression_vector(int_float_a, cast_(3.2, DataType::Float)),
      row_subquery_lqp));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SQLTranslatorTest, UpdateCast) {
  const auto actual_lqp = compile_query("UPDATE int_float SET a = b, b = 3 WHERE a > 1;", UseMvcc::Yes);

  // clang-format off
  const auto row_subquery_lqp =
  PredicateNode::make(greater_than_(int_float_a, 1),
    ValidateNode::make(
      stored_table_node_int_float));

  const auto expected_lqp =
  UpdateNode::make("int_float",
    row_subquery_lqp,
    ProjectionNode::make(expression_vector(cast_(int_float_b, DataType::Int), cast_(3, DataType::Float)),
      row_subquery_lqp));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SQLTranslatorTest, CreateView) {
  const auto query = "CREATE VIEW my_first_view AS SELECT a, b, a + b, a*b AS t FROM int_float WHERE a = 'b';";
  const auto result_node = compile_query(query);

  // clang-format off
  const auto select_list_expressions = expression_vector(int_float_a, int_float_b, add_(int_float_a, int_float_b), mul_(int_float_a, int_float_b));  // NOLINT

  const auto view_lqp =
  AliasNode::make(select_list_expressions, std::vector<std::string>({"a", "b", "a + b", "t"}),
    ProjectionNode::make(select_list_expressions,
      PredicateNode::make(equals_(int_float_a, "b"),
         stored_table_node_int_float)));

  const auto view_columns = std::unordered_map<ColumnID, std::string>({
                                                                      {ColumnID{0}, "a"},
                                                                      {ColumnID{1}, "b"},
                                                                      {ColumnID{3}, "t"},
                                                                      });
  // clang-format on

  const auto view = std::make_shared<LQPView>(view_lqp, view_columns);

  const auto expected_lqp = CreateViewNode::make("my_first_view", view, false);

  EXPECT_LQP_EQ(result_node, expected_lqp);
}

TEST_F(SQLTranslatorTest, CreateAliasView) {
  const auto actual_lqp = compile_query("CREATE VIEW my_second_view (c, d) AS SELECT * FROM int_float WHERE a = 'b';");

  // clang-format off
  const auto view_columns = std::unordered_map<ColumnID, std::string>({
                                                                      {ColumnID{0}, "c"},
                                                                      {ColumnID{1}, "d"}
                                                                      });

  const auto view_lqp = PredicateNode::make(equals_(int_float_a, "b"), stored_table_node_int_float);
  // clang-format on

  const auto view = std::make_shared<LQPView>(view_lqp, view_columns);

  const auto expected_lqp = CreateViewNode::make("my_second_view", view, false);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SQLTranslatorTest, CreateViewIfNotExists) {
  const auto query = "CREATE VIEW IF NOT EXISTS my_first_view AS SELECT b, a FROM int_float WHERE a = 'b';";
  const auto result_node = compile_query(query);

  const auto select_list_expressions = expression_vector(int_float_b, int_float_a);

  // clang-format off
  const auto view_lqp =
  ProjectionNode::make(select_list_expressions,
    PredicateNode::make(equals_(int_float_a, "b"),
      stored_table_node_int_float));
  // clang-format on

  const auto view_columns = std::unordered_map<ColumnID, std::string>({{ColumnID{0}, "b"}, {ColumnID{1}, "a"}});

  const auto view = std::make_shared<LQPView>(view_lqp, view_columns);

  const auto expected_lqp = CreateViewNode::make("my_first_view", view, true);

  EXPECT_LQP_EQ(result_node, expected_lqp);
}

TEST_F(SQLTranslatorTest, DropView) {
  const auto query = "DROP VIEW my_third_view";
  auto result_node = compile_query(query);

  const auto lqp = DropViewNode::make("my_third_view", false);

  EXPECT_LQP_EQ(lqp, result_node);
}

TEST_F(SQLTranslatorTest, DropViewIfExists) {
  const auto query = "DROP VIEW IF EXISTS my_third_view";
  auto result_node = compile_query(query);

  const auto lqp = DropViewNode::make("my_third_view", true);

  EXPECT_LQP_EQ(lqp, result_node);
}

TEST_F(SQLTranslatorTest, CreateTable) {
  const auto actual_lqp = compile_query(
      "CREATE TABLE a_table (a_int INTEGER, a_long LONG, a_float FLOAT, a_double DOUBLE NULL, a_string VARCHAR(10) NOT "
      "NULL)");

  const auto column_definitions = TableColumnDefinitions{{"a_int", DataType::Int, false},
                                                         {"a_long", DataType::Long, false},
                                                         {"a_float", DataType::Float, false},
                                                         {"a_double", DataType::Double, true},
                                                         {"a_string", DataType::String, false}};

  const auto static_table_node = StaticTableNode::make(Table::create_dummy_table(column_definitions));
  const auto expected_lqp = CreateTableNode::make("a_table", false, static_table_node);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SQLTranslatorTest, CreateTableIfNotExists) {
  const auto actual_lqp = compile_query(
      "CREATE TABLE IF NOT EXISTS a_table (a_int INTEGER, a_long LONG, a_float FLOAT, a_double DOUBLE NULL, a_string "
      "VARCHAR(10) NOT NULL)");

  const auto column_definitions = TableColumnDefinitions{{"a_int", DataType::Int, false},
                                                         {"a_long", DataType::Long, false},
                                                         {"a_float", DataType::Float, false},
                                                         {"a_double", DataType::Double, true},
                                                         {"a_string", DataType::String, false}};

  const auto static_table_node = StaticTableNode::make(Table::create_dummy_table(column_definitions));
  const auto expected_lqp = CreateTableNode::make("a_table", true, static_table_node);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SQLTranslatorTest, CreateTableAsSelect) {
  const auto actual_lqp = compile_query("CREATE TABLE a_table AS SELECT * FROM int_float");

  const auto stored_table_node = StoredTableNode::make("int_float");
  const auto expected_lqp = CreateTableNode::make("a_table", false, stored_table_node);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SQLTranslatorTest, DropTable) {
  const auto actual_lqp = compile_query("DROP TABLE a_table");

  const auto expected_lqp = DropTableNode::make("a_table", false);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SQLTranslatorTest, DropTableIfExists) {
  const auto actual_lqp = compile_query("DROP TABLE IF EXISTS a_table");

  const auto expected_lqp = DropTableNode::make("a_table", true);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SQLTranslatorTest, PrepareWithoutParameters) {
  const auto actual_lqp = compile_query("PREPARE some_prepared_plan FROM 'SELECT a AS x FROM int_float'");

  // clang-format off
  const auto statement_lqp =
  AliasNode::make(expression_vector(int_float_a), std::vector<std::string>{"x"},
    ProjectionNode::make(expression_vector(int_float_a), stored_table_node_int_float));
  // clang-format on

  const auto prepared_plan = std::make_shared<PreparedPlan>(statement_lqp, std::vector<ParameterID>{});

  const auto expected_lqp = CreatePreparedPlanNode::make("some_prepared_plan", prepared_plan);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SQLTranslatorTest, PrepareWithParameters) {
  const auto actual_lqp = compile_query(
      "PREPARE some_prepared_plan FROM 'SELECT * FROM int_float "
      "WHERE a > ? AND b < ?'");

  // clang-format off
  const auto statement_lqp =
  PredicateNode::make(greater_than_(int_float_a, placeholder_(ParameterID{0})),
    PredicateNode::make(less_than_(int_float_b, placeholder_(ParameterID{1})),
      stored_table_node_int_float));
  // clang-format on

  const auto prepared_plan =
      std::make_shared<PreparedPlan>(statement_lqp, std::vector<ParameterID>{ParameterID{0}, ParameterID{1}});

  const auto expected_lqp = CreatePreparedPlanNode::make("some_prepared_plan", prepared_plan);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SQLTranslatorTest, PrepareWithParametersAndCorrelatedSubquery) {
  // Correlated subqueries and prepared statement's parameters both use the ParameterID system, so let's test that they
  // cooperate

  const auto actual_lqp = compile_query(
      "PREPARE some_prepared_plan FROM 'SELECT * FROM int_float WHERE a > ? AND"
      " a < (SELECT MIN(a) FROM int_string WHERE int_float.a = int_string.a) AND"
      " b < ?'");

  // clang-format off
  const auto correlated_parameter = correlated_parameter_(ParameterID{1}, int_float_a);

  const auto subquery_lqp =
  AggregateNode::make(expression_vector(), expression_vector(min_(int_string_a)),
    PredicateNode::make(equals_(correlated_parameter, int_string_a),
      stored_table_node_int_string));

  const auto subquery = lqp_subquery_(subquery_lqp, std::make_pair(ParameterID{1}, int_float_a));

  const auto statement_lqp = PredicateNode::make(greater_than_(int_float_a, placeholder_(ParameterID{0})),
  PredicateNode::make(less_than_(int_float_a, subquery),
    PredicateNode::make(less_than_(int_float_b, placeholder_(ParameterID{2})),
       stored_table_node_int_float)));
  // clang-format on

  const auto prepared_plan =
      std::make_shared<PreparedPlan>(statement_lqp, std::vector<ParameterID>{ParameterID{0}, ParameterID{2}});

  const auto expected_lqp = CreatePreparedPlanNode::make("some_prepared_plan", prepared_plan);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SQLTranslatorTest, Execute) {
  // clang-format off
  const auto uncorrelated_parameter = placeholder_(ParameterID{3});
  const auto correlated_parameter = correlated_parameter_(ParameterID{2}, int_float_a);

  const auto prepared_subquery_lqp = AggregateNode::make(expression_vector(), expression_vector(min_(int_float_a)),
    PredicateNode::make(equals_(uncorrelated_parameter, correlated_parameter),
      stored_table_node_int_float));

  const auto prepared_subquery = lqp_subquery_(prepared_subquery_lqp, std::make_pair(ParameterID{1}, int_string_a));

  const auto prepared_plan_lqp =
  PredicateNode::make(greater_than_(int_string_a, placeholder_(ParameterID{1})),
    PredicateNode::make(less_than_(int_string_b, placeholder_(ParameterID{0})),
      PredicateNode::make(equals_(int_string_a, prepared_subquery), stored_table_node_int_string)));
  // clang-format on

  const auto prepared_plan = std::make_shared<PreparedPlan>(
      prepared_plan_lqp, std::vector<ParameterID>{ParameterID{0}, ParameterID{1}, ParameterID{3}});

  Hyrise::get().storage_manager.add_prepared_plan("some_prepared_plan", prepared_plan);

  const auto actual_lqp = compile_query("EXECUTE some_prepared_plan ('Hello', 1, 42)");

  // clang-format off
  const auto execute_subquery_lqp =
      AggregateNode::make(expression_vector(), expression_vector(min_(int_float_a)),
                          PredicateNode::make(equals_(42, correlated_parameter), stored_table_node_int_float));

  const auto execute_subquery = lqp_subquery_(execute_subquery_lqp, std::make_pair(ParameterID{1}, int_string_a));

  const auto expected_lqp =
  PredicateNode::make(greater_than_(int_string_a, 1),
    PredicateNode::make(less_than_(int_string_b, "Hello"),
      PredicateNode::make(equals_(int_string_a, execute_subquery),
        stored_table_node_int_string)));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SQLTranslatorTest, ExecuteWithoutParams) {
  const auto prepared_lqp =
      AggregateNode::make(expression_vector(), expression_vector(min_(int_float_a)), stored_table_node_int_float);

  const auto prepared_plan = std::make_shared<PreparedPlan>(prepared_lqp, std::vector<ParameterID>{});

  Hyrise::get().storage_manager.add_prepared_plan("another_prepared_plan", prepared_plan);

  const auto actual_lqp = compile_query("EXECUTE another_prepared_plan ()");

  EXPECT_LQP_EQ(actual_lqp, prepared_lqp);
}

TEST_F(SQLTranslatorTest, OperatorPrecedence) {
  /**
   * Though the operator precedence is handled by the sql-parser, do some checks here as well that it works as expected.
   * SQLite is our reference: https://www.sqlite.org/lang_expr.html
   * For operators with the same precedence, we evaluate left-to-right
   */

  // clang-format off
  EXPECT_LQP_EQ(compile_query("SELECT 1 + 2 * 3 / -4"), ProjectionNode::make(expression_vector(add_(1, div_(mul_(2, 3), -4))), DummyTableNode::make()));  // NOLINT
  EXPECT_LQP_EQ(compile_query("SELECT 1 + 2 * 3 / 4"), ProjectionNode::make(expression_vector(add_(1, div_(mul_(2, 3), 4))), DummyTableNode::make()));  // NOLINT
  EXPECT_LQP_EQ(compile_query("SELECT 3 + 5 % 3"), ProjectionNode::make(expression_vector(add_(3, mod_(5, 3))), DummyTableNode::make()));  // NOLINT
  EXPECT_LQP_EQ(compile_query("SELECT 3 + 5 > 4 / 2"), ProjectionNode::make(expression_vector(greater_than_(add_(3, 5), div_(4, 2))), DummyTableNode::make()));  // NOLINT
  EXPECT_LQP_EQ(compile_query("SELECT 5 < 3 == 2 < 1"), ProjectionNode::make(expression_vector(equals_(less_than_(5, 3), less_than_(2, 1))), DummyTableNode::make()));  // NOLINT
  EXPECT_LQP_EQ(compile_query("SELECT 1 OR 2 AND 3 OR 4"), ProjectionNode::make(expression_vector(or_(or_(1, and_(2, 3)), 4)), DummyTableNode::make()));  // NOLINT
  // clang-format on
}

TEST_F(SQLTranslatorTest, CatchInputErrors) {
  EXPECT_THROW(compile_query("SELECT no_such_table.* FROM int_float;"), InvalidInputException);
  EXPECT_THROW(compile_query("SELECT no_such_function(5+3);"), InvalidInputException);
  EXPECT_THROW(compile_query("SELECT no_such_column FROM int_float;"), InvalidInputException);
  EXPECT_THROW(compile_query("SELECT * FROM no_such_table;"), InvalidInputException);
  EXPECT_THROW(compile_query("SELECT b, SUM(b) AS s FROM table_a GROUP BY a;"), InvalidInputException);
  EXPECT_THROW(compile_query("SELECT * FROM int_float GROUP BY a;"), InvalidInputException);
  EXPECT_THROW(compile_query("SELECT t1.*, t2.*, SUM(t2.b) FROM int_float t1, int_float t2 GROUP BY t1.a, t1.b, t2.a"),
               InvalidInputException);
  EXPECT_THROW(compile_query("SELECT * FROM table_a JOIN table_b ON a = b;"), InvalidInputException);
  EXPECT_THROW(compile_query("SELECT * FROM table_a JOIN table_b ON table_a.a = table_b.a AND a = 3;"),
               InvalidInputException);  // NOLINT
  EXPECT_THROW(compile_query("SELECT * FROM int_float WHERE 3 + 4;"), InvalidInputException);
  EXPECT_THROW(compile_query("SELECT a AS b FROM int_float WHERE b > 5"), InvalidInputException);
  EXPECT_THROW(compile_query("SELECT a AS b FROM int_float GROUP BY int_float.b"), InvalidInputException);
  EXPECT_THROW(compile_query("SELECT a AS b, b AS a FROM int_float WHERE a > 5"), InvalidInputException);
  EXPECT_THROW(compile_query("INSERT INTO int_float VALUES (1, 2, 3, 4)"), InvalidInputException);
  EXPECT_THROW(compile_query("SELECT a, SUM(b) FROM int_float GROUP BY a HAVING b > 10;"), InvalidInputException);
  EXPECT_THROW(compile_query("SELECT * FROM int_float LIMIT 1 OFFSET 1;"), InvalidInputException);
  EXPECT_THROW(compile_query("INSERT INTO no_such_table (a) VALUES (1);"), InvalidInputException);
  EXPECT_THROW(compile_query("DELETE FROM no_such_table"), InvalidInputException);
  EXPECT_THROW(compile_query("DELETE FROM no_such_table WHERE a = 1"), InvalidInputException);
  EXPECT_THROW(compile_query("UPDATE no_such_table SET a = 1"), InvalidInputException);
  EXPECT_THROW(compile_query("UPDATE no_such_table SET a = 1 WHERE a = 1"), InvalidInputException);
  EXPECT_THROW(compile_query("WITH q AS (SELECT * FROM int_float), q AS (SELECT b FROM q) SELECT * FROM q;"),
               InvalidInputException);
}

TEST_F(SQLTranslatorTest, WithClauseSingleQuerySimple) {
  const auto actual_lqp = compile_query(
      "WITH "
      "wq AS (SELECT a, b FROM int_int_int) "
      "SELECT * FROM wq WHERE a > 123;");

  // clang-format off
  const auto wq_lqp =
    ProjectionNode::make(expression_vector(int_int_int_a, int_int_int_b),
      stored_table_node_int_int_int);

  const auto expected_lqp =
    PredicateNode::make(greater_than_(int_int_int_a, value_(123)),
      wq_lqp);
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SQLTranslatorTest, WithClauseSingleQueryAlias) {
  const auto actual_lqp = compile_query(
      "WITH "
      "wq AS (SELECT a AS x FROM int_int_int) "
      "SELECT * FROM wq WHERE x > 123;");

  // clang-format off
  const auto aliases = std::vector<std::string>{"x"};
  const auto expressions = expression_vector(int_int_int_a);
  const auto wq_lqp =
    AliasNode::make(expressions, aliases,
      ProjectionNode::make(expression_vector(int_int_int_a),
        stored_table_node_int_int_int));

  const auto expected_lqp =
    AliasNode::make(expressions, aliases,
      PredicateNode::make(greater_than_(int_int_int_a, value_(123)),
        wq_lqp));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SQLTranslatorTest, WithClauseSingleQueryAliasWhere) {
  const auto actual_lqp = compile_query(
      "WITH "
      "wq AS (SELECT a AS x FROM int_int_int WHERE a > 123) "
      "SELECT x AS z FROM wq;");

  // clang-format off
  const auto alias_x = std::vector<std::string>{"x"};
  const auto wq_lqp =
    AliasNode::make(expression_vector(int_int_int_a), alias_x,
      ProjectionNode::make(expression_vector(int_int_int_a),
        PredicateNode::make(greater_than_(int_int_int_a, value_(123)),
          stored_table_node_int_int_int)));

  const auto alias_z = std::vector<std::string>{"z"};
  const auto expected_lqp =
    AliasNode::make(expression_vector(int_int_int_a), alias_z,
      wq_lqp);
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SQLTranslatorTest, WithClauseSingleQueryAggregateGroupBy) {
  const auto actual_lqp = compile_query(
      "WITH "
      "wq AS (SELECT a, SUM(b) FROM int_int_int GROUP BY a) "
      "SELECT * FROM wq;");

  // clang-format off
  const auto wq_lqp =
    AggregateNode::make(expression_vector(int_int_int_a), expression_vector(sum_(int_int_int_b)),
      stored_table_node_int_int_int);

  const auto expected_lqp = wq_lqp;
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SQLTranslatorTest, WithClauseSingleQueryAggregateGroupByAlias) {
  const auto actual_lqp = compile_query(
      "WITH "
      "wq AS (SELECT a, SUM(b) AS sum FROM int_int_int GROUP BY a) "
      "SELECT * FROM wq WHERE sum > 10;");

  // clang-format off
  const auto sum_b = sum_(int_int_int_b);
  const auto select_list_expressions = expression_vector(int_int_int_a, sum_b);
  const auto aliases = std::vector<std::string>{"a", "sum"};
  const auto wq_lqp =
    AliasNode::make(select_list_expressions, aliases,
      AggregateNode::make(expression_vector(int_int_int_a), expression_vector(sum_b),
        stored_table_node_int_int_int));

  // Redundant AliasNode due to the SQLTranslator architecture. Doesn't look nice, but not really an issue.
  const auto expected_lqp =
    AliasNode::make(select_list_expressions, aliases,
      PredicateNode::make(greater_than_(sum_b, value_(10)),
        wq_lqp));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SQLTranslatorTest, WithClauseDoubleQuery) {
  const auto actual_lqp = compile_query(
      "WITH "
      "wq1 AS (SELECT a AS a1, b AS b1 FROM int_float), "
      "wq2 AS (SELECT a AS a2, b AS b2 FROM int_float2) "
      "SELECT * FROM wq1 JOIN wq2 ON a1 = a2;");
  // clang-format off
  const auto expressions_wq1 = expression_vector(int_float_a, int_float_b);
  const auto aliases_wq1 = std::vector<std::string>{"a1", "b1"};
  const auto wq1_lqp =
    AliasNode::make(expressions_wq1, aliases_wq1,
      stored_table_node_int_float);

  const auto expressions_wq2 = expression_vector(int_float2_a, int_float2_b);
  const auto aliases_wq2 = std::vector<std::string>{"a2", "b2"};
  const auto wq2_lqp =
    AliasNode::make(expressions_wq2, aliases_wq2,
      stored_table_node_int_float2);

  const auto expressions_join = expression_vector(int_float_a, int_float_b, int_float2_a, int_float2_b);
  const auto aliases_join = std::vector<std::string>({"a1", "b1", "a2", "b2"});
  const auto a1_equals_a2 = equals_(int_float_a, int_float2_a);
  const auto expected_lqp =
    AliasNode::make(expressions_join, aliases_join,
      JoinNode::make(JoinMode::Inner, a1_equals_a2, wq1_lqp, wq2_lqp));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SQLTranslatorTest, WithClauseConsecutiveQueriesSimple) {
  const auto actual_lqp = compile_query(
      "WITH "
      "wq1 AS (SELECT a, b FROM int_int_int), "
      "wq2 AS (SELECT b FROM wq1) "
      "SELECT * FROM wq2;");

  // clang-format off
  const auto wq1_lqp =
    ProjectionNode::make(expression_vector(int_int_int_a, int_int_int_b),
      stored_table_node_int_int_int);
  const auto wq2_lqp =
    ProjectionNode::make(expression_vector(int_int_int_b),
      wq1_lqp);

  const auto expected_lqp = wq2_lqp;
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SQLTranslatorTest, WithClauseConsecutiveQueriesWhereAlias) {
  const auto actual_lqp = compile_query(
      "WITH "
      "wq1 AS (SELECT a, b FROM int_int_int WHERE a > 9), "
      "wq2 AS (SELECT b AS z FROM wq1 WHERE b >= 10) "
      "SELECT * FROM wq2;");

  // clang-format off
  const auto wq1_lqp =
    ProjectionNode::make(expression_vector(int_int_int_a, int_int_int_b),
      PredicateNode::make(greater_than_(int_int_int_a, value_(9)),
        stored_table_node_int_int_int));

  const auto alias_z = std::vector<std::string>{"z"};
  const auto wq2_lqp =
    AliasNode::make(expression_vector(int_int_int_b), alias_z,
      ProjectionNode::make(expression_vector(int_int_int_b),
        PredicateNode::make(greater_than_equals_(int_int_int_b, value_(10)),
          wq1_lqp)));

  // Redundant AliasNode due to the SQLTranslator architecture. Doesn't look nice, but not really an issue.
  const auto expected_lqp =
    AliasNode::make(expression_vector(int_int_int_b), alias_z,
      wq2_lqp);
  // clang-format on
  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SQLTranslatorTest, WithClausePlaceholders) {
  // clang-format off
  const auto [actual_lqp, parameter_ids_of_value_placeholders] = compile_prepared_query(
      "WITH "
      "wq AS (SELECT ?, a, b "
             "FROM int_int_int WHERE a > ?) "
      "SELECT ?, a "
      "FROM (SELECT * FROM wq WHERE b = ?) AS t WHERE a = ?;");

  ASSERT_EQ(parameter_ids_of_value_placeholders.size(), 5u);
  EXPECT_EQ(parameter_ids_of_value_placeholders.at(0), ParameterID{0});
  EXPECT_EQ(parameter_ids_of_value_placeholders.at(1), ParameterID{1});
  EXPECT_EQ(parameter_ids_of_value_placeholders.at(2), ParameterID{3});
  EXPECT_EQ(parameter_ids_of_value_placeholders.at(3), ParameterID{2});
  EXPECT_EQ(parameter_ids_of_value_placeholders.at(4), ParameterID{4});

  const auto placeholder_0 = placeholder_(ParameterID{0});
  const auto placeholder_1 = placeholder_(ParameterID{1});
  const auto wq_lqp =
    ProjectionNode::make(expression_vector(placeholder_0, int_int_int_a, int_int_int_b),
      PredicateNode::make(greater_than_(int_int_int_a, placeholder_1),
        stored_table_node_int_int_int));

  const auto placeholder_2 = placeholder_(ParameterID{3});
  const auto placeholder_3 = placeholder_(ParameterID{2});
  const auto placeholder_4 = placeholder_(ParameterID{4});
  const auto expected_lqp =
    ProjectionNode::make(expression_vector(placeholder_2, int_int_int_a),
      PredicateNode::make(equals_(int_int_int_a, placeholder_4),
        PredicateNode::make(equals_(int_int_int_b, placeholder_3),
          wq_lqp)));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SQLTranslatorTest, WithClauseTableMasking) {
  // Check StorageManager for existance of table int_float
  const auto pre_condition_lqp_actual = compile_query("SELECT * FROM int_float;");
  const auto pre_condition_lqp_expected = stored_table_node_int_float;
  EXPECT_LQP_EQ(pre_condition_lqp_actual, pre_condition_lqp_expected);

  // Mask StorageManager's int_float table via WITH clause
  const auto actual_lqp = compile_query(
      "WITH "
      "int_float AS (SELECT a, b FROM int_int_int) "
      "SELECT * FROM int_float;");

  // clang-format off
  const auto expected_lqp =
    ProjectionNode::make(expression_vector(int_int_int_a, int_int_int_b),
      stored_table_node_int_int_int);
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

}  // namespace opossum
