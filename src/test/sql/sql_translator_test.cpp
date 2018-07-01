#include "gtest/gtest.h"

#include "constant_mappings.hpp"
#include "expression/arithmetic_expression.hpp"
#include "expression/abstract_expression.hpp"
#include "expression/binary_predicate_expression.hpp"
#include "expression/expression_factory.hpp"
#include "expression/expression_utils.hpp"
#include "expression/case_expression.hpp"
#include "expression/value_expression.hpp"
#include "expression/arithmetic_expression.hpp"
#include "expression/lqp_column_expression.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/aggregate_node.hpp"
#include "logical_query_plan/create_view_node.hpp"
#include "logical_query_plan/drop_view_node.hpp"
#include "logical_query_plan/alias_node.hpp"
#include "logical_query_plan/delete_node.hpp"
#include "logical_query_plan/dummy_table_node.hpp"
#include "logical_query_plan/lqp_column_reference.hpp"
#include "logical_query_plan/insert_node.hpp"
#include "logical_query_plan/update_node.hpp"
#include "logical_query_plan/update_node.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/limit_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/projection_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "logical_query_plan/show_tables_node.hpp"
#include "logical_query_plan/show_columns_node.hpp"
#include "logical_query_plan/sort_node.hpp"
#include "logical_query_plan/union_node.hpp"
#include "storage/storage_manager.hpp"
#include "sql/sql_translator.hpp"
#include "utils/load_table.hpp"
#include "testing_assert.hpp"
#include "tpch/tpch_queries.hpp"

using namespace opossum::expression_factory;  // NOLINT
using namespace std::string_literals;  // NOLINT

namespace {
void load_test_tables() {
  opossum::StorageManager::get().add_table("int_float", opossum::load_table("src/test/tables/int_float.tbl"));
  opossum::StorageManager::get().add_table("int_string", opossum::load_table("src/test/tables/int_string.tbl"));
  opossum::StorageManager::get().add_table("int_float2", opossum::load_table("src/test/tables/int_float2.tbl"));
  opossum::StorageManager::get().add_table("int_float5", opossum::load_table("src/test/tables/int_float5.tbl"));
  opossum::StorageManager::get().add_table("int_int_int", opossum::load_table("src/test/tables/int_int_int.tbl"));
}

std::shared_ptr<opossum::AbstractLQPNode> compile_query(const std::string& query) {
  const auto lqps = opossum::SQLTranslator{}.translate_sql(query);
  Assert(lqps.size() == 1, "Expected just one LQP");
  return lqps.at(0);
}
}  // namespace

namespace opossum {

class SQLTranslatorTest : public ::testing::Test {
 public:
  void SetUp() override {
    load_test_tables();
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

  void TearDown() override {
    StorageManager::reset();
  }

  std::shared_ptr<StoredTableNode> stored_table_node_int_float;
  std::shared_ptr<StoredTableNode> stored_table_node_int_string;
  std::shared_ptr<StoredTableNode> stored_table_node_int_float2;
  std::shared_ptr<StoredTableNode> stored_table_node_int_float5;
  std::shared_ptr<StoredTableNode> stored_table_node_int_int_int;
  LQPColumnReference int_float_a, int_float_b, int_string_a, int_string_b, int_float5_a, int_float5_d, int_float2_a, int_float2_b, int_int_int_a, int_int_int_b, int_int_int_c;
};

TEST_F(SQLTranslatorTest, NoFromClause) {
  const auto actual_lqp = compile_query("SELECT 1 + 2;");

  // clang-format off
  const auto expected_lqp =
  ProjectionNode::make(expression_vector(add(value(1), value(2))),
    DummyTableNode::make());
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SQLTranslatorTest, ExpressionStringTest) {
  const auto actual_lqp = compile_query("SELECT * FROM int_float WHERE a = 'b'");

  // clang-format off
  const auto expected_lqp =
  PredicateNode::make(equals(int_float_a, "b"s),
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
  const auto actual_lqp_no_table = compile_query("SELECT * FROM int_float WHERE a + b > 10");
  const auto actual_lqp_table = compile_query("SELECT int_float.* FROM int_float WHERE a + b > 10;");

  // clang-format off
  const auto expected_lqp =
  ProjectionNode::make(expression_vector(int_float_a, int_float_b),
    PredicateNode::make(greater_than(add(int_float_a, int_float_b), 10),
      ProjectionNode::make(expression_vector(add(int_float_a, int_float_b), int_float_a, int_float_b), stored_table_node_int_float)
    ));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp_no_table, expected_lqp);
  EXPECT_LQP_EQ(actual_lqp_table, expected_lqp);
}

TEST_F(SQLTranslatorTest, SimpleArithmeticExpression) {
  const auto actual_lqp = compile_query("SELECT a * b FROM int_float;");

  const auto expected_lqp = ProjectionNode::make(expression_vector(mul(int_float_a, int_float_b)), stored_table_node_int_float);

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
  const auto a_plus_b_times_3 = mul(add(int_float_a, int_float_b), 3);

  const auto expression = case_(equals(a_plus_b_times_3, 123), "Hello",
                                case_(equals(a_plus_b_times_3, 1234), "World",
                                      "Nope"));
  // clang-format on

  const auto expected_lqp = ProjectionNode::make(expression_vector(expression), stored_table_node_int_float);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SQLTranslatorTest, CaseExpressionSimpleNoElse) {
  const auto actual_lqp = compile_query("SELECT"
                                        "    CASE (a + b) * 3"
                                        "       WHEN 123 THEN 'Hello'"
                                        "       WHEN 1234 THEN 'World'"
                                        "    END"
                                        " FROM int_float;");

  // clang-format off
  const auto a_plus_b_times_3 = mul(add(int_float_a, int_float_b), 3);

  const auto expression = case_(equals(a_plus_b_times_3, 123), "Hello",
                                case_(equals(a_plus_b_times_3, 1234), "World",
                                      null()));
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
  const auto expressions = expression_vector(int_float_a, int_float_b, add(int_float_b, int_float_a));

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

  const auto a_times_b = mul(int_float_a, int_float_b);
  const auto b_plus_a = add(int_float_b, int_float_a);

  // clang-format off
  const auto expected_lqp =
  ProjectionNode::make(expression_vector(int_float_a),
    PredicateNode::make(greater_than_equals(a_times_b, b_plus_a),
      ProjectionNode::make(expression_vector(a_times_b, b_plus_a, int_float_a, int_float_b), stored_table_node_int_float
  )));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SQLTranslatorTest, WhereWithLike) {
  const auto actual_lqp_a = compile_query("SELECT * FROM int_string WHERE b LIKE '%test1%';");
  const auto actual_lqp_b = compile_query("SELECT * FROM int_string WHERE b NOT LIKE '%test1%';");
  const auto actual_lqp_c = compile_query("SELECT * FROM int_string WHERE b NOT LIKE CONCAT('%test1', '%');");

  // clang-format off
  const auto expected_lqp_a = PredicateNode::make(like(int_string_b, "%test1%"), stored_table_node_int_string);
  const auto expected_lqp_b = PredicateNode::make(not_like(int_string_b, "%test1%"), stored_table_node_int_string);
  const auto expected_lqp_c =
  ProjectionNode::make(expression_vector(int_string_a, int_string_b),
    PredicateNode::make(not_like(int_string_b, concat("%test1", "%")),
      ProjectionNode::make(expression_vector(concat("%test1", "%"), int_string_a, int_string_b),
        stored_table_node_int_string)));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp_a, expected_lqp_a);
  EXPECT_LQP_EQ(actual_lqp_b, expected_lqp_b);
  EXPECT_LQP_EQ(actual_lqp_c, expected_lqp_c);
}

TEST_F(SQLTranslatorTest, WhereWithLogical) {
  const auto actual_lqp = compile_query("SELECT a FROM int_float WHERE 5 >= b + a OR (a > 2 AND b > 2);");

  const auto b_plus_a = add(int_float_b, int_float_a);

  // clang-format off
  const auto predicate = or_(greater_than_equals(5, add(int_float_b, int_float_a)),
                             and_(greater_than(int_float_a, 2), greater_than(int_float_b, 2)));

  const auto expected_lqp =
  ProjectionNode::make(expression_vector(int_float_a),
    PredicateNode::make(not_equals(predicate, 0),
      ProjectionNode::make(expression_vector(predicate, int_float_a, int_float_b),
        stored_table_node_int_float
  )));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SQLTranslatorTest, WhereWithBetween) {
  const auto actual_lqp = compile_query("SELECT a FROM int_float WHERE a BETWEEN b and 5;");

  const auto a_times_b = mul(int_float_a, int_float_b);
  const auto b_plus_a = add(int_float_b, int_float_a);

  // clang-format off
  const auto expected_lqp =
  ProjectionNode::make(expression_vector(int_float_a),
    PredicateNode::make(between(int_float_a, int_float_b, 5),
      stored_table_node_int_float
  ));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SQLTranslatorTest, WhereIsNull) {
  const auto actual_lqp = compile_query("SELECT b FROM int_float WHERE a + b IS NULL;");

  // clang-format off
  const auto expected_lqp =
  ProjectionNode::make(expression_vector(int_float_b),
    PredicateNode::make(is_null(add(int_float_a, int_float_b)),
      ProjectionNode::make(expression_vector(add(int_float_a, int_float_b), int_float_a, int_float_b),
      stored_table_node_int_float
  )));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SQLTranslatorTest, WhereIsNotNull) {
  const auto actual_lqp = compile_query("SELECT b FROM int_float WHERE a IS NOT NULL;");

  // clang-format off
  const auto expected_lqp =
  ProjectionNode::make(expression_vector(int_float_b),
    PredicateNode::make(is_not_null(int_float_a),
      stored_table_node_int_float
  ));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SQLTranslatorTest, WhereExists) {
  const auto actual_lqp = compile_query("SELECT * FROM int_float WHERE EXISTS(SELECT * FROM int_float2 WHERE int_float.a = int_float2.a);");

  // clang-format off
  const auto parameter_int_float_a = parameter(ParameterID{0}, int_float_a);
  const auto sub_select_lqp =
  PredicateNode::make(equals(parameter_int_float_a, int_float2_a), stored_table_node_int_float2);
  const auto sub_select = select(sub_select_lqp, std::make_pair(ParameterID{0}, int_float_a));

  const auto expected_lqp =
  ProjectionNode::make(expression_vector(int_float_a, int_float_b),
    PredicateNode::make(not_equals(exists(sub_select), 0),
      ProjectionNode::make(expression_vector(exists(sub_select), int_float_a, int_float_b),
        stored_table_node_int_float
  )));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SQLTranslatorTest, WhereWithCorrelatedSelect) {
  const auto actual_lqp = compile_query("SELECT * FROM int_float WHERE a > (SELECT MIN(a + int_float.b) FROM int_float2);");

  const auto parameter_b = parameter(ParameterID{0}, int_float_b);

  // clang-format off
  const auto sub_select_lqp =
  AggregateNode::make(expression_vector(), expression_vector(min(add(int_float2_a, parameter_b))),
    ProjectionNode::make(expression_vector(int_float2_a, int_float2_b, add(int_float2_a, parameter_b)),
      stored_table_node_int_float2
  ));
  const auto sub_select = select(sub_select_lqp, std::make_pair(ParameterID{0}, int_float_b));

  const auto expected_lqp =
  ProjectionNode::make(expression_vector(int_float_a, int_float_b),
    PredicateNode::make(greater_than(int_float_a, sub_select),
      ProjectionNode::make(expression_vector(sub_select, int_float_a, int_float_b),
        stored_table_node_int_float
  )));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SQLTranslatorTest, WhereNotPredicate) {
  const auto actual_lqp_a = compile_query("SELECT * FROM int_float WHERE NOT (a = b);");
  const auto actual_lqp_b = compile_query("SELECT * FROM int_float WHERE NOT (a != b);");
  const auto actual_lqp_c = compile_query("SELECT * FROM int_float WHERE NOT (a > b);");
  const auto actual_lqp_d = compile_query("SELECT * FROM int_float WHERE NOT (a < b);");
  const auto actual_lqp_e = compile_query("SELECT * FROM int_float WHERE NOT (a >= b);");
  const auto actual_lqp_f = compile_query("SELECT * FROM int_float WHERE NOT (a <= b);");
  const auto actual_lqp_g = compile_query("SELECT * FROM int_float WHERE NOT (a IS NULL);");
  const auto actual_lqp_h = compile_query("SELECT * FROM int_float WHERE NOT (a IS NOT NULL);");

  // clang-format off
  const auto expected_lqp_a = PredicateNode::make(not_equals(int_float_a, int_float_b), stored_table_node_int_float);
  const auto expected_lqp_b = PredicateNode::make(equals(int_float_a, int_float_b), stored_table_node_int_float);
  const auto expected_lqp_c = PredicateNode::make(less_than_equals(int_float_a, int_float_b), stored_table_node_int_float);  // NOLINT
  const auto expected_lqp_d = PredicateNode::make(greater_than_equals(int_float_a, int_float_b), stored_table_node_int_float);  // NOLINT
  const auto expected_lqp_e = PredicateNode::make(less_than(int_float_a, int_float_b), stored_table_node_int_float);
  const auto expected_lqp_f = PredicateNode::make(greater_than(int_float_a, int_float_b), stored_table_node_int_float);
  const auto expected_lqp_g = PredicateNode::make(is_not_null(int_float_a), stored_table_node_int_float);
  const auto expected_lqp_h = PredicateNode::make(is_null(int_float_a), stored_table_node_int_float);
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

TEST_F(SQLTranslatorTest, WhereNotFallback) {
  /**
   * If we can't inverse a predicate to apply NOT, we translate the NOT expression from
   * "NOT <some_expression>" to "<some_expression> == 0"
   */

  const auto actual_lqp = compile_query("SELECT * FROM int_float WHERE NOT (a IN (1, 2));");

  // clang-format off
  const auto expected_lqp =
  ProjectionNode::make(expression_vector(int_float_a, int_float_b),
    PredicateNode::make(equals(in(int_float_a, list(1, 2)), 0),
        ProjectionNode::make(expression_vector(in(int_float_a, list(1, 2)), int_float_a, int_float_b),
      stored_table_node_int_float)));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SQLTranslatorTest, AggregateWithGroupBy) {
  const auto actual_lqp = compile_query("SELECT SUM(a * 3) * b FROM int_float GROUP BY b");

  const auto a_times_3 = mul(int_float_a, 3);

  // clang-format off
  const auto expected_lqp =
  ProjectionNode::make(expression_vector(mul(sum(a_times_3), int_float_b)),
    AggregateNode::make(expression_vector(int_float_b), expression_vector(sum(a_times_3)),
      ProjectionNode::make(expression_vector(int_float_a, int_float_b, a_times_3),
      stored_table_node_int_float
  )));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SQLTranslatorTest, AggregateCount) {
  const auto actual_lqp_count_a = compile_query("SELECT b, COUNT(a) FROM int_float GROUP BY b");
  // clang-format off
  const auto expected_lqp_a =
  AggregateNode::make(expression_vector(int_float_b), expression_vector(count(int_float_a)),
    stored_table_node_int_float
  );
  // clang-format on
  EXPECT_LQP_EQ(actual_lqp_count_a, expected_lqp_a);


  const auto actual_lqp_count_star = compile_query("SELECT b, COUNT(*) FROM int_float GROUP BY b");
  // clang-format off
  const auto expected_lqp_star =
  AggregateNode::make(expression_vector(int_float_b), expression_vector(count_star()),
    stored_table_node_int_float
  );
  // clang-format on
  EXPECT_LQP_EQ(actual_lqp_count_star, expected_lqp_star);


  const auto actual_lqp_count_distinct_a_plus_b = compile_query("SELECT a, b, COUNT(DISTINCT a + b) FROM int_float GROUP BY a, b");
  // clang-format off
  const auto expected_lqp_count_distinct_a_plus_b =
  AggregateNode::make(expression_vector(int_float_a, int_float_b), expression_vector(count_distinct(add(int_float_a, int_float_b))),
    ProjectionNode::make(expression_vector(int_float_a, int_float_b, add(int_float_a, int_float_b)),
      stored_table_node_int_float
  ));
  // clang-format on
  EXPECT_LQP_EQ(actual_lqp_count_distinct_a_plus_b, expected_lqp_count_distinct_a_plus_b);
}

TEST_F(SQLTranslatorTest, GroupByOnly) {
  const auto actual_lqp = compile_query("SELECT * FROM int_float GROUP BY b + 3, a / b, b");

  const auto b_plus_3 = add(int_float_b, 3);
  const auto a_divided_by_b = div_(int_float_a, int_float_b);
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

  const auto sum_a_plus_b = sum(add(int_float_a, int_float_b));
  const auto b_plus_3 = add(int_float_b, 3);

  const auto aliases = std::vector<std::string>({"b", "y", "SUM(a + b)"});
  const auto select_list_expressions = expression_vector(int_float_b, b_plus_3, sum(add(int_float_a, int_float_b)));

  // clang-format off
  const auto expected_lqp =
  AliasNode::make(select_list_expressions, aliases,
    ProjectionNode::make(select_list_expressions,
      AggregateNode::make(expression_vector(b_plus_3, int_float_b), expression_vector(sum_a_plus_b),
        ProjectionNode::make(expression_vector(int_float_a, int_float_b, add(int_float_a, int_float_b), b_plus_3),
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
  ProjectionNode::make(expression_vector(add(min(int_float_a), 3)),
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

  const auto expressions = expression_vector(add(int_float_a, int_float_b), int_float_a, int_float_b);
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

//TEST_F(SQLTranslatorTest, SubSelectSelectList) {
//  // "d" is from the outer query
//  const auto actual_lqp_a = compile_query("SELECT (SELECT MIN(a + d) FROM int_float), a FROM int_float5 AS f");
//
//  const auto a_plus_d = add(int_float_a, external(int_float5_d, 0));
//
//  // clang-format off
//  const auto sub_select_lqp =
//  AggregateNode::make(expression_vector(), expression_vector(min(a_plus_d)),
//    ProjectionNode::make(expression_vector(int_float_a, int_float_b, a_plus_d), stored_table_node_int_float)
//  );
//  // clang-format on
//
//  const auto select_expressions = expression_vector(select(sub_select_lqp, expression_vector(int_float5_d)), int_float5_a);
//
//  // clang-format off
//  const auto expected_lqp = ProjectionNode::make(select_expressions, stored_table_node_int_float5);
//  // clang-format on
//
//  EXPECT_LQP_EQ(actual_lqp_a, expected_lqp);
//}

TEST_F(SQLTranslatorTest, OrderByTest) {
  const auto actual_lqp = compile_query("SELECT * FROM int_float ORDER BY a, a+b DESC, b ASC");

  const auto order_by_modes = std::vector<OrderByMode>({OrderByMode::Ascending, OrderByMode::Descending, OrderByMode::Ascending});

  // clang-format off
  const auto expected_lqp =
  ProjectionNode::make(expression_vector(int_float_a, int_float_b),
    SortNode::make(expression_vector(int_float_a, add(int_float_a, int_float_b), int_float_b), order_by_modes,
      ProjectionNode::make(expression_vector(add(int_float_a, int_float_b), int_float_a, int_float_b),
        stored_table_node_int_float
  )));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp)
}

TEST_F(SQLTranslatorTest, InArray) {
  const auto actual_lqp = compile_query("SELECT * FROM int_float WHERE a + 7 IN (1+2,3,4)");

  const auto a_plus_7_in = in(add(int_float_a, 7), list(add(1,2), 3, 4));

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

TEST_F(SQLTranslatorTest, InSelect) {
  const auto actual_lqp = compile_query("SELECT * FROM int_float WHERE a + 7 IN (SELECT * FROM int_float2)");

  // clang-format off
  const auto sub_select_lqp = stored_table_node_int_float2;
  const auto sub_select = select(sub_select_lqp);

  const auto a_plus_7_in = in(add(int_float_a, 7), sub_select);

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

TEST_F(SQLTranslatorTest, InCorrelatedSelect) {
  const auto actual_lqp = compile_query("SELECT * FROM int_float WHERE a IN (SELECT * FROM int_float2 WHERE int_float.b * int_float.a * int_float.a > b)");

  // clang-format off
  const auto parameter_a = parameter(ParameterID{1}, int_float_a);
  const auto parameter_b = parameter(ParameterID{0}, int_float_b);

  const auto b_times_a_times_a = mul(mul(parameter_b, parameter_a), parameter_a);

  const auto sub_select_lqp =
  ProjectionNode::make(expression_vector(int_float2_a, int_float2_b),
    PredicateNode::make(greater_than(b_times_a_times_a, int_float2_b),
      ProjectionNode::make(expression_vector(b_times_a_times_a, int_float2_a, int_float2_b),
        stored_table_node_int_float2
  )));

  const auto sub_select = select(sub_select_lqp, std::make_pair(ParameterID{1}, int_float_a), std::make_pair(ParameterID{0}, int_float_b));

  const auto a_in_sub_select = in(int_float_a, sub_select);

  // clang-format off
  const auto expected_lqp =
  ProjectionNode::make(expression_vector(int_float_a, int_float_b),
    PredicateNode::make(not_equals(a_in_sub_select, 0),
      ProjectionNode::make(expression_vector(a_in_sub_select, int_float_a, int_float_b),
         stored_table_node_int_float
  )));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SQLTranslatorTest, JoinInnerSimple) {
  const auto actual_lqp = compile_query("SELECT * FROM int_float JOIN int_float2 ON int_float2.a > int_float.a");

  // clang-format off
  const auto expected_lqp =
  JoinNode::make(JoinMode::Inner, greater_than(int_float2_a, int_float_a),
    stored_table_node_int_float,
    stored_table_node_int_float2
  );
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SQLTranslatorTest, JoinCrossSelectStar) {
  const auto actual_lqp = compile_query("SELECT * FROM int_float, int_float2 AS t, int_float5 WHERE t.a < 2");

  // clang-format off
  const auto expected_lqp =
  PredicateNode::make(less_than(int_float2_a, 2),
    JoinNode::make(JoinMode::Cross,
      JoinNode::make(JoinMode::Cross,
        stored_table_node_int_float,
        stored_table_node_int_float2),
    stored_table_node_int_float5
  ));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SQLTranslatorTest, JoinCrossSelectElements) {
  const auto actual_lqp = compile_query("SELECT int_float5.d, t.* FROM int_float, int_float2 AS t, int_float5 WHERE t.a < 2");

  // clang-format off
  const auto expected_lqp =
  ProjectionNode::make(expression_vector(int_float5_d, int_float2_a, int_float2_b),
    PredicateNode::make(less_than(int_float2_a, 2),
      JoinNode::make(JoinMode::Cross,
        JoinNode::make(JoinMode::Cross,
          stored_table_node_int_float,
          stored_table_node_int_float2),
      stored_table_node_int_float5
  )));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SQLTranslatorTest, JoinLeftOuter) {
  // Test local predicates in outer joins - those on the preserving side are discarded, those on the null supplying side
  // are performed before the join

  const auto actual_lqp = compile_query("SELECT"
                                        "  * "
                                        "FROM "
                                        "  int_float AS a LEFT JOIN int_float2 AS b "
                                        "    ON a.a = b.a AND b.a > 5 AND b.b <= 13 AND a.a < 3 "
                                        "WHERE b.b < 2;");

  // clang-format off
  const auto expected_lqp =
  PredicateNode::make(less_than(int_float2_b, 2),
    JoinNode::make(JoinMode::Left, equals(int_float_a, int_float2_a),
      stored_table_node_int_float,
      PredicateNode::make(greater_than(int_float2_a, 5),
        PredicateNode::make(less_than_equals(int_float2_b, 13), stored_table_node_int_float2))
  ));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SQLTranslatorTest, JoinNaturalSimple) {
  // Also test that columns can be referenced after a natural join

  const auto actual_lqp = compile_query("SELECT "
                                        "  * "
                                        "FROM "
                                        "  int_float AS a NATURAL JOIN int_float2 AS b "
                                        "WHERE "
                                        "  a.b > 10 AND a.a > 5");

  // clang-format off
  const auto expected_lqp =
  PredicateNode::make(greater_than(int_float_b, 10),
    PredicateNode::make(greater_than(int_float_a, 5),
      ProjectionNode::make(expression_vector(int_float_a, int_float_b),
        PredicateNode::make(equals(int_float_b, int_float2_b),
          JoinNode::make(JoinMode::Inner, equals(int_float_a, int_float2_a),
            stored_table_node_int_float,
            stored_table_node_int_float2
  )))));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SQLTranslatorTest, JoinNaturalColumnAlias) {
  // Test that the Natural join can work with column aliases and that the output columns have the correct name

  const auto actual_lqp = compile_query("SELECT "
                                        "  * "
                                        "FROM "
                                        "  int_float AS a NATURAL JOIN (SELECT a AS d, b AS a, c FROM int_int_int) AS b");

  const auto aliases = std::vector<std::string>{{"a", "b", "d", "c"}};
  const auto sub_select_aliases = std::vector<std::string>{{"d", "a", "c"}};

  // clang-format off
  const auto expected_lqp =
  AliasNode::make(expression_vector(int_float_a, int_float_b, int_int_int_a, int_int_int_c), aliases,
    ProjectionNode::make(expression_vector(int_float_a, int_float_b, int_int_int_a, int_int_int_c),
      JoinNode::make(JoinMode::Inner, equals(int_float_a, int_int_int_b),
        stored_table_node_int_float,
        AliasNode::make(expression_vector(int_int_int_a, int_int_int_b, int_int_int_c), sub_select_aliases,
          stored_table_node_int_int_int)
  )));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SQLTranslatorTest, JoinInnerComplexPredicate) {
  const auto actual_lqp = compile_query("SELECT * FROM int_float JOIN int_float2 ON int_float.a + int_float2.a = int_float2.b * int_float.a;");

  // clang-format off
  const auto a_plus_a = add(int_float_a, int_float2_a);
  const auto b_times_a = mul(int_float2_b, int_float_a);
  const auto expected_lqp =
  ProjectionNode::make(expression_vector(int_float_a, int_float_b, int_float2_a, int_float2_b),
    PredicateNode::make(equals(a_plus_a, b_times_a),
      ProjectionNode::make(expression_vector(a_plus_a, b_times_a, int_float_a, int_float_b, int_float2_a, int_float2_b),
        JoinNode::make(JoinMode::Cross,
          stored_table_node_int_float,
          stored_table_node_int_float2)
  )));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SQLTranslatorTest, JoinInnerComplexLogicalPredicate) {
  const auto actual_lqp = compile_query("SELECT * FROM int_float AS m1 JOIN int_float AS m2 ON m1.a * 3 = m2.a - 5 OR m1.a > 20;");

  // clang-format off
  const auto a_times_3 = mul(int_float_a, 3);
  const auto a_minus_5 = sub(int_float_a, 5);

  const auto join_cross = JoinNode::make(JoinMode::Cross, stored_table_node_int_float, stored_table_node_int_float);
  const auto join_predicate = or_(equals(mul(int_float_a, 3), sub(int_float_a, 5)), greater_than(int_float_a, 20));

  const auto expected_lqp =
  ProjectionNode::make(expression_vector(int_float_a, int_float_b, int_float_a, int_float_b),
    PredicateNode::make(not_equals(join_predicate, 0),
      ProjectionNode::make(expression_vector(join_predicate, int_float_a, int_float_b, int_float_a, int_float_b),
        JoinNode::make(JoinMode::Cross,
          stored_table_node_int_float,
          stored_table_node_int_float
  ))));
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
      PredicateNode::make(equals(int_float_a, int_float_b),
        stored_table_node_int_float
  )));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp_a, expected_lqp);
  EXPECT_LQP_EQ(actual_lqp_b, expected_lqp);
}

TEST_F(SQLTranslatorTest, FromColumnAliasingTablesSwitchNames) {
  // Tricky: Tables "switch names". int_float becomes int_float2 and int_float2 becomes int_float

  const auto actual_lqp_a = compile_query("SELECT int_float.y, int_float2.* "
                                          "FROM int_float AS int_float2 (a, b), int_float2 AS int_float(x,y) "
                                          "WHERE int_float.x = int_float2.b");
  const auto actual_lqp_b = compile_query("SELECT int_float.y, int_float2.* "
                                          "FROM (SELECT * FROM int_float) AS int_float2 (a, b), (SELECT * FROM int_float2) AS int_float(x,y) "
                                          "WHERE int_float.x = int_float2.b");

  // clang-format off
  const auto expected_lqp =
  AliasNode::make(expression_vector(int_float2_b, int_float_a, int_float_b), std::vector<std::string>({"y", "a", "b"}),
    ProjectionNode::make(expression_vector(int_float2_b, int_float_a, int_float_b),
      PredicateNode::make(equals(int_float2_a, int_float_b),
        JoinNode::make(JoinMode::Cross,
          stored_table_node_int_float,
          stored_table_node_int_float2
  ))));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp_a, expected_lqp);
  EXPECT_LQP_EQ(actual_lqp_b, expected_lqp);
}

TEST_F(SQLTranslatorTest, LimitLiteral) {
  // Most common case: LIMIT to a fixed number
  const auto actual_lqp = compile_query("SELECT * FROM int_float LIMIT 1;");
  const auto expected_lqp = LimitNode::make(value(static_cast<int64_t>(1)), stored_table_node_int_float);
  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

// TODO(anybody) Disabled because SQLParser doesn't support Expressions in LIMIT clause
TEST_F(SQLTranslatorTest, DISABLED_LimitExpression) {
  // Uncommon: LIMIT to the result of an Expression (which has to be uncorrelated
  const auto actual_lqp = compile_query("SELECT int_float.a AS x FROM int_float LIMIT 3 + (SELECT MIN(b) FROM int_float2);");

  // clang-format off
  const auto sub_select =
  AggregateNode::make(expression_vector(), expression_vector(min(int_float2_b)),
                      stored_table_node_int_float2);

  const auto expected_lqp =
  LimitNode::make(add(3, select(sub_select)),
                  stored_table_node_int_float);
  // clang-format on
  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SQLTranslatorTest, Extract) {
  const auto actual_lqp = compile_query("SELECT EXTRACT(MONTH FROM '1993-08-01');");

  // clang-format off
  const auto expected_lqp =
  ProjectionNode::make(expression_vector(extract(DatetimeComponent::Month, "1993-08-01")), DummyTableNode::make());
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SQLTranslatorTest, ValuePlaceholders) {
  const auto actual_lqp = compile_query("SELECT a + ?, ? FROM int_float WHERE a > ?");

  // clang-format off
  const auto expected_lqp =
  ProjectionNode::make(expression_vector(add(int_float_a, parameter(ParameterID{1})), parameter(ParameterID{2})),
    PredicateNode::make(greater_than(int_float_a, parameter(ParameterID{0})),
      stored_table_node_int_float
  ));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SQLTranslatorTest, ParameterIDAllocationSimple) {
  /**
   * Test that ParameterIDs are correctly allocated to ValuePlaceholders and External Parameters
   */
  SQLTranslator sql_translator;

  const auto actual_lqp = sql_translator.translate_sql("SELECT (SELECT (SELECT int_float2.a + int_float.b) FROM int_float2) FROM int_float").at(0);

  actual_lqp->print();

  // clang-format off
  const auto parameter_int_float_b = parameter(ParameterID{1}, int_float_b);
  const auto parameter_int_float2_a = parameter(ParameterID{0}, int_float2_a);

  // "(SELECT int_float2.a + int_float.b)"
  const auto expected_sub_sub_select_lqp =
  ProjectionNode::make(expression_vector(add(parameter_int_float2_a, parameter_int_float_b)),
     DummyTableNode::make()
  );
  const auto sub_sub_select = select(expected_sub_sub_select_lqp,
                                     std::make_pair(ParameterID{0}, int_float2_a));


  // "(SELECT (SELECT int_float2.a + int_float.b) FROM int_float2)"
  const auto expected_sub_select_lqp =
  ProjectionNode::make(expression_vector(sub_sub_select),
    stored_table_node_int_float2
  );
  const auto expected_sub_select = select(expected_sub_select_lqp, std::make_pair(ParameterID{1}, int_float_b));

  const auto expected_lqp =
  ProjectionNode::make(expression_vector(expected_sub_select),
    stored_table_node_int_float
  );
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SQLTranslatorTest, ParameterIDAllocation) {
  /**
   * Test that ParameterIDs are correctly allocated to ValuePlaceholders and External Parameters
   */
  SQLTranslator sql_translator;

  const auto actual_lqp = sql_translator.translate_sql("SELECT ?, "
                                        "  (SELECT MIN(b) + int_float.a FROM int_float2), "
                                        "  (SELECT MAX(b) + int_float.b + (SELECT int_float2.a + int_float.b) FROM int_float2)"
                                        "FROM int_float WHERE a > ?").at(0);

  // clang-format off
  const auto parameter_int_float_a = parameter(ParameterID{2}, int_float_a);
  const auto parameter_int_float_b = parameter(ParameterID{3}, int_float_b);
  const auto parameter_int_float2_a = parameter(ParameterID{4}, int_float2_a);

  // "(SELECT MIN(b) + int_float.a FROM int_float2)"
  const auto expected_sub_select_lqp_a =
  ProjectionNode::make(expression_vector(add(min(int_float2_b), parameter_int_float_a)),
    AggregateNode::make(expression_vector(), expression_vector(min(int_float2_b)),
      stored_table_node_int_float2
  ));
  const auto expected_sub_select_a = select(expected_sub_select_lqp_a, std::make_pair(ParameterID{2}, int_float_a));

  // "(SELECT int_float2.a + int_float.b)"
  const auto expected_sub_sub_select_lqp =
  ProjectionNode::make(expression_vector(add(parameter_int_float2_a, parameter_int_float_b)),
    DummyTableNode::make()
  );
  const auto sub_sub_select = select(expected_sub_sub_select_lqp,
                                     std::make_pair(ParameterID{4}, int_float2_a));


  // "(SELECT MAX(b) + int_float.b + (SELECT int_float2.a + int_float.b) FROM int_float2)"
  const auto expected_sub_select_lqp_b =
  ProjectionNode::make(expression_vector(add(add(max(int_float2_b), parameter_int_float_b), sub_sub_select)),
    AggregateNode::make(expression_vector(), expression_vector(max(int_float2_b)),
      stored_table_node_int_float2
  ));
  const auto expected_sub_select_b = select(expected_sub_select_lqp_b, std::make_pair(ParameterID{3}, int_float_b));

  const auto expected_lqp =
  ProjectionNode::make(expression_vector(parameter(ParameterID{1}),
                                         expected_sub_select_a,
                                         expected_sub_select_b),
    PredicateNode::make(greater_than(int_float_a, parameter(ParameterID{0})),
      stored_table_node_int_float
  ));
  // clang-format on

  EXPECT_EQ(sql_translator.value_placeholders().size(), 2u);
  EXPECT_EQ(sql_translator.value_placeholders().at(ValuePlaceholderID{0}), ParameterID{1});
  EXPECT_EQ(sql_translator.value_placeholders().at(ValuePlaceholderID{1}), ParameterID{0});

  const auto actual_projection_node = std::dynamic_pointer_cast<ProjectionNode>(actual_lqp);
  ASSERT_TRUE(actual_projection_node);

  const auto actual_sub_select_a = std::dynamic_pointer_cast<LQPSelectExpression>(actual_projection_node->expressions.at(1));
  ASSERT_TRUE(actual_sub_select_a);

  const auto actual_sub_select_b = std::dynamic_pointer_cast<LQPSelectExpression>(actual_projection_node->expressions.at(2));
  ASSERT_TRUE(actual_sub_select_b);

//  const auto node_mapping =
//
//  EXPECT_TRUE(expression_equal_to_expression_in_different_lqp(*actual_sub_select_a, *expected_sub_select_a));
//  EXPECT_TRUE(actual_sub_select_b->deep_equals(*expected_sub_select_b));

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}


TEST_F(SQLTranslatorTest, UseMvcc) {
  const auto lqp_a = SQLTranslator{UseMvcc::No}.translate_sql("SELECT * FROM int_float, int_float2 WHERE int_float.a = int_float2.b").at(0);
  const auto lqp_b = SQLTranslator{UseMvcc::Yes}.translate_sql("SELECT * FROM int_float, int_float2 WHERE int_float.a = int_float2.b").at(0);

  EXPECT_FALSE(lqp_is_validated(lqp_a));
  EXPECT_TRUE(lqp_is_validated(lqp_b));
}

TEST_F(SQLTranslatorTest, Substr) {
  const auto actual_lqp_a = compile_query("SELECT SUBSTR('Hello', 3, 2 + 3)");
  const auto actual_lqp_b = compile_query("SELECT substr('Hello', 3, 2 + 3)");

  // clang-format off
  const auto expected_lqp =
  ProjectionNode::make(expression_vector(substr("Hello", 3, add(2, 3))),
    DummyTableNode::make()
  );
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp_a, expected_lqp);
  EXPECT_LQP_EQ(actual_lqp_b, expected_lqp);
}

TEST_F(SQLTranslatorTest, Exists) {
  const auto actual_lqp = compile_query("SELECT EXISTS(SELECT * FROM int_float);");

  // clang-format off
  const auto expected_lqp =
  ProjectionNode::make(expression_vector(exists(select(stored_table_node_int_float))),
    DummyTableNode::make()
  );
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SQLTranslatorTest, NotExists) {
  const auto actual_lqp = compile_query("SELECT NOT EXISTS(SELECT * FROM int_float);");

  // clang-format off
  const auto expected_lqp =
  ProjectionNode::make(expression_vector(equals(exists(select(stored_table_node_int_float)), 0)),
    DummyTableNode::make()
  );
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SQLTranslatorTest, ExistsCorrelated) {
  const auto actual_lqp = compile_query("SELECT EXISTS(SELECT * FROM int_float WHERE int_float.a > int_float2.b) FROM int_float2");

  // clang-format off
  const auto sub_select_lqp =
  PredicateNode::make(greater_than(int_float_a, parameter(ParameterID{0}, int_float2_b)),
    stored_table_node_int_float);
  const auto sub_select = select(sub_select_lqp, std::make_pair(ParameterID{0}, int_float2_b));

  const auto expected_lqp =
  ProjectionNode::make(expression_vector(exists(sub_select)),
    stored_table_node_int_float2
  );
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SQLTranslatorTest, Negate) {
  const auto actual_lqp = compile_query("SELECT -a FROM int_float");

  // clang-format off
  const auto expected_lqp =
  ProjectionNode::make(expression_vector(negate(int_float_a)),
    stored_table_node_int_float
  );
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SQLTranslatorTest, ShowTables) {
  const auto actual_lqp = compile_query("SHOW TABLES");
  const auto expected_lqp = ShowTablesNode::make();
  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SQLTranslatorTest, ShowColumns) {
  const auto actual_lqp = compile_query("SHOW COLUMNS int_float");
  const auto expected_lqp = ShowColumnsNode::make("int_float");
  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

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
TEST_F(SQLTranslatorTest, InsertValues) {
  const auto actual_lqp = compile_query("INSERT INTO int_float VALUES (10, 12.5);");

  // clang-format off
  const auto expected_lqp =
  InsertNode::make("int_float",
                   ProjectionNode::make(expression_vector(10, 12.5f),
                                        DummyTableNode::make()
                   ));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SQLTranslatorTest, InsertValuesColumnReorder) {
  const auto actual_lqp = compile_query("INSERT INTO int_float (b, a) VALUES (12.5, 10);");

  // clang-format off
  const auto expected_lqp =
  InsertNode::make("int_float",
                   ProjectionNode::make(expression_vector(10, 12.5f),
                                        ProjectionNode::make(expression_vector(12.5f, 10),
                                                             DummyTableNode::make()
                                        )));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SQLTranslatorTest, InsertValuesColumnSubset) {
  const auto actual_lqp = compile_query("INSERT INTO int_float (b) VALUES (12.5);");

  // clang-format off
  const auto expected_lqp =
  InsertNode::make("int_float",
                   ProjectionNode::make(expression_vector(null(), 12.5f),
                                        ProjectionNode::make(expression_vector(12.5f),
                                                             DummyTableNode::make()
                                        )));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SQLTranslatorTest, InsertSubquery) {
  const auto actual_lqp = compile_query("INSERT INTO int_float SELECT a, b FROM int_float2 WHERE a > 5;");

  // clang-format off
  const auto expected_lqp =
  InsertNode::make("int_float",
                   PredicateNode::make(greater_than(int_float2_a, 5),
                                       stored_table_node_int_float2
                   ));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SQLTranslatorTest, DeleteSimple) {
  const auto actual_lqp = compile_query("DELETE FROM int_float");

  // clang-format off
  const auto expected_lqp =
  DeleteNode::make("int_float",
                   StoredTableNode::make("int_float")
  );
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}


TEST_F(SQLTranslatorTest, DeleteConditional) {
  const auto actual_lqp = compile_query("DELETE FROM int_float WHERE a > 5");

  // clang-format off
  const auto expected_lqp =
  DeleteNode::make("int_float",
                   PredicateNode::make(greater_than(int_float_a, 5),
                                       stored_table_node_int_float
                   ));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SQLTranslatorTest, UpdateConditional) {
  const auto actual_lqp = compile_query("UPDATE int_float SET b = 3.2 WHERE a > 1;");

  // clang-format off
  const auto expected_lqp =
  UpdateNode::make("int_float", expression_vector(int_float_a, 3.2f),
                   PredicateNode::make(greater_than(int_float_a, 1),
                                       stored_table_node_int_float
                   ));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}
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
//
TEST_F(SQLTranslatorTest, CreateView) {
  const auto query = "CREATE VIEW my_first_view AS SELECT a, b, a + b, a*b AS t FROM int_float WHERE a = 'b';";
  const auto result_node = compile_query(query);

  // clang-format off
  const auto select_list_expressions = expression_vector(int_float_a, int_float_b, add(int_float_a, int_float_b), mul(int_float_a, int_float_b));  // NOLINT

  const auto view_lqp =
  AliasNode::make(select_list_expressions, std::vector<std::string>({"a", "b", "a + b", "t"}),
                  ProjectionNode::make(select_list_expressions,
                                       PredicateNode::make(equals(int_float_a, "b"),
                                                           stored_table_node_int_float)));

  const auto view_columns = std::unordered_map<ColumnID, std::string>({
                                                                      {ColumnID{0}, "a"},
                                                                      {ColumnID{1}, "b"},
                                                                      {ColumnID{3}, "t"},
                                                                      });
  // clang-format on

  const auto view = std::make_shared<View>(view_lqp, view_columns);

  const auto lqp = std::make_shared<CreateViewNode>("my_first_view", view);

  EXPECT_LQP_EQ(lqp, result_node);
}

TEST_F(SQLTranslatorTest, CreateAliasView) {
  const auto actual_lqp = compile_query("CREATE VIEW my_second_view (c, d) AS SELECT * FROM int_float WHERE a = 'b';");

  // clang-format off
  const auto view_columns = std::unordered_map<ColumnID, std::string>({
                                                                      {ColumnID{0}, "c"},
                                                                      {ColumnID{1}, "d"},
                                                                      });

  const auto view_lqp = PredicateNode::make(equals(int_float_a, "b"), stored_table_node_int_float);
  // clang-format on

  const auto view = std::make_shared<View>(view_lqp, view_columns);

  const auto expected_lqp = std::make_shared<CreateViewNode>("my_second_view", view);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SQLTranslatorTest, DropView) {
  const auto query = "DROP VIEW my_third_view";
  auto result_node = compile_query(query);

  const auto lqp = DropViewNode::make("my_third_view");

  EXPECT_LQP_EQ(lqp, result_node);
}

}  // namespace opossum
