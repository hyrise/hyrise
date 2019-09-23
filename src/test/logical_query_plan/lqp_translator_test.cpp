#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "base_test.hpp"
#include "expression/aggregate_expression.hpp"
#include "expression/arithmetic_expression.hpp"
#include "expression/expression_functional.hpp"
#include "expression/expression_utils.hpp"
#include "expression/lqp_column_expression.hpp"
#include "expression/pqp_column_expression.hpp"
#include "expression/pqp_subquery_expression.hpp"
#include "hyrise.hpp"
#include "logical_query_plan/aggregate_node.hpp"
#include "logical_query_plan/create_prepared_plan_node.hpp"
#include "logical_query_plan/create_table_node.hpp"
#include "logical_query_plan/drop_table_node.hpp"
#include "logical_query_plan/dummy_table_node.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/limit_node.hpp"
#include "logical_query_plan/lqp_translator.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/projection_node.hpp"
#include "logical_query_plan/sort_node.hpp"
#include "logical_query_plan/static_table_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "logical_query_plan/union_node.hpp"
#include "operators/aggregate_hash.hpp"
#include "operators/get_table.hpp"
#include "operators/index_scan.hpp"
#include "operators/join_hash.hpp"
#include "operators/join_nested_loop.hpp"
#include "operators/join_sort_merge.hpp"
#include "operators/limit.hpp"
#include "operators/maintenance/create_prepared_plan.hpp"
#include "operators/maintenance/create_table.hpp"
#include "operators/maintenance/drop_table.hpp"
#include "operators/product.hpp"
#include "operators/projection.hpp"
#include "operators/sort.hpp"
#include "operators/table_scan.hpp"
#include "operators/table_wrapper.hpp"
#include "operators/union_all.hpp"
#include "operators/union_positions.hpp"
#include "storage/chunk_encoder.hpp"
#include "storage/index/group_key/group_key_index.hpp"
#include "storage/prepared_plan.hpp"
#include "storage/table.hpp"
#include "utils/load_table.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

class LQPTranslatorTest : public BaseTest {
 public:
  void SetUp() override {
    table_int_float = load_table("resources/test_data/tbl/int_float.tbl");
    table_int_string = load_table("resources/test_data/tbl/int_string.tbl");
    table_int_float2 = load_table("resources/test_data/tbl/int_float2.tbl");
    table_int_float5 = load_table("resources/test_data/tbl/int_float5.tbl");
    table_alias_name = load_table("resources/test_data/tbl/table_alias_name.tbl");

    Hyrise::get().storage_manager.add_table("table_int_float", table_int_float);
    Hyrise::get().storage_manager.add_table("table_int_string", table_int_string);
    Hyrise::get().storage_manager.add_table("table_int_float2", table_int_float2);
    Hyrise::get().storage_manager.add_table("table_int_float5", table_int_float5);
    Hyrise::get().storage_manager.add_table("table_alias_name", table_alias_name);
    Hyrise::get().storage_manager.add_table("int_float_chunked",
                                            load_table("resources/test_data/tbl/int_float.tbl", 1));
    ChunkEncoder::encode_all_chunks(Hyrise::get().storage_manager.get_table("int_float_chunked"));

    int_float_node = StoredTableNode::make("table_int_float");
    int_float_a = int_float_node->get_column("a");
    int_float_b = int_float_node->get_column("b");
    int_float_a_expression = std::make_shared<LQPColumnExpression>(int_float_a);
    int_float_b_expression = std::make_shared<LQPColumnExpression>(int_float_b);

    int_string_node = StoredTableNode::make("table_int_string");
    int_string_a = int_string_node->get_column("a");
    int_string_b = int_string_node->get_column("b");

    int_float2_node = StoredTableNode::make("table_int_float2");
    int_float2_a = int_float2_node->get_column("a");
    int_float2_b = int_float2_node->get_column("b");

    int_float5_node = StoredTableNode::make("table_int_float5");
    int_float5_a = int_float5_node->get_column("a");
    int_float5_d = int_float5_node->get_column("d");
  }

  std::shared_ptr<Table> table_int_float, table_int_float2, table_int_float5, table_alias_name, table_int_string;
  std::shared_ptr<StoredTableNode> int_float_node, int_string_node, int_float2_node, int_float5_node;
  LQPColumnReference int_float_a, int_float_b, int_string_a, int_string_b, int_float2_a, int_float2_b, int_float5_a,
      int_float5_d;
  std::shared_ptr<AbstractExpression> int_float_a_expression, int_float_b_expression;
};

TEST_F(LQPTranslatorTest, StoredTableNode) {
  /**
   * Build LQP and translate to PQP
   *
   * LQP resembles:
   *    SELECT a FROM table_int_float;
   */
  const auto pqp = LQPTranslator{}.translate_node(int_float_node);

  /**
   * Check PQP
   */
  const auto get_table_op = std::dynamic_pointer_cast<GetTable>(pqp);
  ASSERT_TRUE(get_table_op);
  EXPECT_EQ(get_table_op->table_name(), "table_int_float");
}

TEST_F(LQPTranslatorTest, ArithmeticExpression) {
  /**
   * Build LQP and translate to PQP
   *
   * LQP resembles:
   *   SELECT a + b FROM table_int_float;
   */
  const auto a_plus_b_lqp = std::make_shared<ArithmeticExpression>(ArithmeticOperator::Addition, int_float_a_expression,
                                                                   int_float_b_expression);
  const auto projection_expressions = std::vector<std::shared_ptr<AbstractExpression>>({a_plus_b_lqp});
  const auto projection_node = ProjectionNode::make(projection_expressions, int_float_node);
  const auto pqp = LQPTranslator{}.translate_node(projection_node);

  /**
   * Check PQP
   */
  const auto projection_op = std::dynamic_pointer_cast<Projection>(pqp);
  ASSERT_TRUE(projection_op);
  ASSERT_EQ(projection_op->expressions.size(), 1u);
  const auto a_plus_b_pqp = std::dynamic_pointer_cast<ArithmeticExpression>(projection_op->expressions.at(0));
  ASSERT_TRUE(a_plus_b_pqp);
  EXPECT_EQ(a_plus_b_pqp->arithmetic_operator, ArithmeticOperator::Addition);

  const auto a_pqp = std::dynamic_pointer_cast<PQPColumnExpression>(a_plus_b_pqp->left_operand());
  ASSERT_TRUE(a_pqp);
  EXPECT_EQ(a_pqp->column_id, ColumnID{0});

  const auto b_pqp = std::dynamic_pointer_cast<PQPColumnExpression>(a_plus_b_pqp->right_operand());
  ASSERT_TRUE(b_pqp);
  EXPECT_EQ(b_pqp->column_id, ColumnID{1});

  const auto get_table_op = std::dynamic_pointer_cast<const GetTable>(pqp->input_left());
  ASSERT_TRUE(get_table_op);
  EXPECT_EQ(get_table_op->table_name(), "table_int_float");
}

TEST_F(LQPTranslatorTest, PredicateNodeSimpleBinary) {
  /**
   * Build LQP and translate to PQP
   *
   * LQP resembles:
   *   SELECT * FROM int_float WHERE 5 > b;
   */
  const auto predicate_node = PredicateNode::make(greater_than_(5, int_float_b), int_float_node);
  const auto pqp = LQPTranslator{}.translate_node(predicate_node);

  /**
   * Check PQP
   */
  const auto table_scan_op = std::dynamic_pointer_cast<TableScan>(pqp);
  const auto b = PQPColumnExpression::from_table(*table_int_float, ColumnID{1});
  ASSERT_TRUE(table_scan_op);
  EXPECT_EQ(*table_scan_op->predicate(), *greater_than_(5, b));

  const auto get_table_op = std::dynamic_pointer_cast<const GetTable>(pqp->input_left());
  ASSERT_TRUE(get_table_op);
  EXPECT_EQ(get_table_op->table_name(), "table_int_float");
}

TEST_F(LQPTranslatorTest, PredicateNodeLike) {
  /**
   * Build LQP and translate to PQP
   *
   * LQP resembles:
   *   SELECT * FROM int_string WHERE b LIKE 'hello%';
   */
  const auto lqp = PredicateNode::make(like_(int_string_b, "hello%"), int_string_node);
  const auto pqp = LQPTranslator{}.translate_node(lqp);

  /**
   * Check PQP
   */
  const auto table_scan_op = std::dynamic_pointer_cast<TableScan>(pqp);
  const auto b = PQPColumnExpression::from_table(*table_int_string, ColumnID{1});
  ASSERT_TRUE(table_scan_op);
  EXPECT_EQ(*table_scan_op->predicate(), *like_(b, "hello%"));

  const auto get_table_op = std::dynamic_pointer_cast<const GetTable>(pqp->input_left());
  ASSERT_TRUE(get_table_op);
  EXPECT_EQ(get_table_op->table_name(), "table_int_string");
}

TEST_F(LQPTranslatorTest, PredicateNodeUnary) {
  /**
   * Build LQP and translate to PQP
   *
   * LQP resembles:
   *   SELECT * FROM int_float WHERE b IS NOT NULL;
   */
  const auto predicate_node = PredicateNode::make(is_not_null_(int_float_b), int_float_node);
  const auto pqp = LQPTranslator{}.translate_node(predicate_node);

  /**
   * Check PQP
   */
  const auto table_scan_op = std::dynamic_pointer_cast<TableScan>(pqp);
  const auto b = PQPColumnExpression::from_table(*table_int_float, ColumnID{1});
  ASSERT_TRUE(table_scan_op);
  EXPECT_EQ(*table_scan_op->predicate(), *is_not_null_(b));

  const auto get_table_op = std::dynamic_pointer_cast<const GetTable>(pqp->input_left());
  ASSERT_TRUE(get_table_op);
  EXPECT_EQ(get_table_op->table_name(), "table_int_float");
}

TEST_F(LQPTranslatorTest, PredicateNodeBetween) {
  /**
   * Build LQP and translate to PQP
   *
   * LQP resembles:
   *   SELECT * FROM int_float WHERE 5 BETWEEN a AND b;
   */
  const auto predicate_node = PredicateNode::make(between_inclusive_(5, int_float_a, int_float_b), int_float_node);
  const auto pqp = LQPTranslator{}.translate_node(predicate_node);

  /**
   * Check PQP
   */
  const auto a = PQPColumnExpression::from_table(*table_int_float, "a");
  const auto b = PQPColumnExpression::from_table(*table_int_float, "b");

  const auto between_scan_op = std::dynamic_pointer_cast<const TableScan>(pqp);
  ASSERT_TRUE(between_scan_op);
  EXPECT_EQ(*between_scan_op->predicate(), *between_inclusive_(5, a, b));

  const auto get_table_op = std::dynamic_pointer_cast<const GetTable>(pqp->input_left());
  ASSERT_TRUE(get_table_op);
  EXPECT_EQ(get_table_op->table_name(), "table_int_float");
}

TEST_F(LQPTranslatorTest, SubqueryExpressionCorrelated) {
  /**
   * Build LQP and translate to PQP
   *
   * LQP resembles:
   *   SELECT (SELECT MIN(a + int_float5.d + int_float5.a) FROM int_float), a FROM int_float5;
   */
  const auto parameter_a = correlated_parameter_(ParameterID{0}, int_float5_a);
  const auto parameter_d = correlated_parameter_(ParameterID{1}, int_float5_d);

  const auto a_plus_a_plus_d = add_(int_float_a, add_(parameter_a, parameter_d));

  // clang-format off
  const auto subquery_lqp =
  AggregateNode::make(expression_vector(), expression_vector(min_(a_plus_a_plus_d)),
    ProjectionNode::make(expression_vector(a_plus_a_plus_d),
      int_float_node));

  const auto subquery = lqp_subquery_(subquery_lqp, std::make_pair(ParameterID{0}, int_float5_a),
                                 std::make_pair(ParameterID{1}, int_float5_d));

  const auto lqp =
  ProjectionNode::make(expression_vector(subquery, int_float5_a), int_float5_node);
  // clang-format on

  const auto pqp = LQPTranslator{}.translate_node(lqp);

  ASSERT_EQ(pqp->type(), OperatorType::Projection);
  ASSERT_TRUE(pqp->input_left());
  ASSERT_EQ(pqp->input_left()->type(), OperatorType::GetTable);

  const auto projection = std::static_pointer_cast<const Projection>(pqp);
  ASSERT_EQ(projection->expressions.size(), 2u);

  const auto expression_a = std::dynamic_pointer_cast<PQPSubqueryExpression>(projection->expressions.at(0));
  ASSERT_TRUE(expression_a);
  ASSERT_EQ(expression_a->parameters.size(), 2u);
  ASSERT_EQ(expression_a->parameters.at(0).first, ParameterID{0});
  ASSERT_EQ(expression_a->parameters.at(0).second, ColumnID{0});
  ASSERT_EQ(expression_a->parameters.at(1).first, ParameterID{1});
  ASSERT_EQ(expression_a->parameters.at(1).second, ColumnID{1});

  ASSERT_EQ(expression_a->pqp->type(), OperatorType::Aggregate);

  const auto expression_b = std::dynamic_pointer_cast<PQPColumnExpression>(projection->expressions.at(1));
  ASSERT_TRUE(expression_b);
}

TEST_F(LQPTranslatorTest, Sort) {
  /**
   * Build LQP and translate to PQP
   *
   * LQP resembles:
   *   SELECT a, b FROM int_float ORDER BY a, a + b DESC, b ASC
   */

  const auto order_by_modes =
      std::vector<OrderByMode>({OrderByMode::Ascending, OrderByMode::Descending, OrderByMode::AscendingNullsLast});

  // clang-format off
  const auto lqp =
  ProjectionNode::make(expression_vector(int_float_a, int_float_b),
    SortNode::make(expression_vector(int_float_a, add_(int_float_a, int_float_b), int_float_b), order_by_modes,
      ProjectionNode::make(expression_vector(add_(int_float_a, int_float_b), int_float_a, int_float_b),
        int_float_node)));

  // clang-format on
  const auto pqp = LQPTranslator{}.translate_node(lqp);

  /**
   * Check PQP
   */
  const auto projection_a = std::dynamic_pointer_cast<const Projection>(pqp);
  ASSERT_TRUE(projection_a);

  const auto sort_a = std::dynamic_pointer_cast<const Sort>(pqp->input_left());
  ASSERT_TRUE(sort_a);
  EXPECT_EQ(sort_a->column_id(), ColumnID{1});
  EXPECT_EQ(sort_a->order_by_mode(), OrderByMode::Ascending);

  const auto sort_a_plus_b = std::dynamic_pointer_cast<const Sort>(sort_a->input_left());
  ASSERT_TRUE(sort_a_plus_b);
  EXPECT_EQ(sort_a_plus_b->column_id(), ColumnID{0});
  EXPECT_EQ(sort_a_plus_b->order_by_mode(), OrderByMode::Descending);

  const auto sort_b = std::dynamic_pointer_cast<const Sort>(sort_a_plus_b->input_left());
  ASSERT_TRUE(sort_b);
  EXPECT_EQ(sort_b->column_id(), ColumnID{2});
  EXPECT_EQ(sort_b->order_by_mode(), OrderByMode::AscendingNullsLast);

  const auto projection_b = std::dynamic_pointer_cast<const Projection>(sort_b->input_left());
  ASSERT_TRUE(projection_b);

  const auto get_table = std::dynamic_pointer_cast<const GetTable>(projection_b->input_left());
  ASSERT_TRUE(get_table);
}

TEST_F(LQPTranslatorTest, LimitLiteral) {
  /**
   * Build LQP and translate to PQP
   *
   * LQP resembles:
   *   SELECT * FROM int_float LIMIT 1337
   */
  const auto lqp = LimitNode::make(value_(static_cast<int64_t>(1337)), int_float_node);
  const auto pqp = LQPTranslator{}.translate_node(lqp);

  /**
   * Check PQP
   */
  const auto limit = std::dynamic_pointer_cast<Limit>(pqp);
  ASSERT_TRUE(limit);
  const auto value_expression = std::dynamic_pointer_cast<ValueExpression>(limit->row_count_expression());
  ASSERT_TRUE(value_expression);
  ASSERT_EQ(value_expression->value, AllTypeVariant(static_cast<int64_t>(1337)));

  const auto get_table = std::dynamic_pointer_cast<const GetTable>(limit->input_left());
  ASSERT_TRUE(get_table);
  EXPECT_EQ(get_table->table_name(), "table_int_float");
}

TEST_F(LQPTranslatorTest, PredicateNodeUnaryScan) {
  /**
   * Build LQP and translate to PQP
   */
  auto predicate_node = PredicateNode::make(equals_(int_float_b, 42), int_float_node);
  const auto op = LQPTranslator{}.translate_node(predicate_node);

  /**
   * Check PQP
   */
  const auto table_scan_op = std::dynamic_pointer_cast<TableScan>(op);
  const auto b = PQPColumnExpression::from_table(*table_int_float, "b");
  ASSERT_TRUE(table_scan_op);
  EXPECT_EQ(*table_scan_op->predicate(), *equals_(b, 42));
}

TEST_F(LQPTranslatorTest, PredicateNodeBetweenScan) {
  /**
   * Build LQP and translate to PQP
   */
  auto predicate_node = PredicateNode::make(between_inclusive_(int_float_a, 42, 1337), int_float_node);
  const auto op = LQPTranslator{}.translate_node(predicate_node);

  /**
   * Check PQP
   */
  const auto table_scan_op = std::dynamic_pointer_cast<TableScan>(op);
  ASSERT_TRUE(table_scan_op);

  const auto a = PQPColumnExpression::from_table(*table_int_float, "a");
  EXPECT_EQ(*table_scan_op->predicate(), *between_inclusive_(a, 42, 1337));
}

TEST_F(LQPTranslatorTest, PredicateNodeIndexScan) {
  /**
   * Build LQP and translate to PQP
   */
  const auto stored_table_node = StoredTableNode::make("int_float_chunked");

  const auto table = Hyrise::get().storage_manager.get_table("int_float_chunked");
  std::vector<ColumnID> index_column_ids = {ColumnID{1}};
  std::vector<ChunkID> index_chunk_ids = {ChunkID{0}, ChunkID{2}};
  table->get_chunk(index_chunk_ids[0])->create_index<GroupKeyIndex>(index_column_ids);
  table->get_chunk(index_chunk_ids[1])->create_index<GroupKeyIndex>(index_column_ids);

  auto predicate_node = PredicateNode::make(equals_(stored_table_node->get_column("b"), 42));
  predicate_node->set_left_input(stored_table_node);
  predicate_node->scan_type = ScanType::IndexScan;
  const auto op = LQPTranslator{}.translate_node(predicate_node);

  /**
   * Check PQP
   */
  const auto union_op = std::dynamic_pointer_cast<UnionAll>(op);
  ASSERT_TRUE(union_op);

  const auto index_scan_op = std::dynamic_pointer_cast<const IndexScan>(op->input_left());
  ASSERT_TRUE(index_scan_op);
  EXPECT_EQ(index_scan_op->included_chunk_ids, index_chunk_ids);

  const auto table_scan_op = std::dynamic_pointer_cast<const TableScan>(op->input_right());
  const auto b = PQPColumnExpression::from_table(*table, "b");
  ASSERT_TRUE(table_scan_op);
  EXPECT_EQ(table_scan_op->excluded_chunk_ids, index_chunk_ids);
  EXPECT_EQ(*table_scan_op->predicate(), *equals_(b, 42));
}

TEST_F(LQPTranslatorTest, PredicateNodeBinaryIndexScan) {
  /**
   * Build LQP and translate to PQP
   */
  const auto stored_table_node = StoredTableNode::make("int_float_chunked");

  const auto table = Hyrise::get().storage_manager.get_table("int_float_chunked");
  std::vector<ColumnID> index_column_ids = {ColumnID{1}};
  std::vector<ChunkID> index_chunk_ids = {ChunkID{0}, ChunkID{2}};
  table->get_chunk(index_chunk_ids[0])->create_index<GroupKeyIndex>(index_column_ids);
  table->get_chunk(index_chunk_ids[1])->create_index<GroupKeyIndex>(index_column_ids);

  auto predicate_node = PredicateNode::make(between_inclusive_(stored_table_node->get_column("b"), 42, 1337));
  predicate_node->set_left_input(stored_table_node);
  predicate_node->scan_type = ScanType::IndexScan;
  const auto op = LQPTranslator{}.translate_node(predicate_node);

  /**
   * Check PQP
   */
  const auto union_op = std::dynamic_pointer_cast<UnionAll>(op);
  ASSERT_TRUE(union_op);

  const auto index_scan_op = std::dynamic_pointer_cast<const IndexScan>(op->input_left());
  ASSERT_TRUE(index_scan_op);
  EXPECT_EQ(index_scan_op->included_chunk_ids, index_chunk_ids);
  EXPECT_EQ(index_scan_op->input_left()->type(), OperatorType::GetTable);

  const auto b = PQPColumnExpression::from_table(*table, "b");
  const auto table_scan_op = std::dynamic_pointer_cast<const TableScan>(op->input_right());
  ASSERT_TRUE(table_scan_op);
  EXPECT_EQ(table_scan_op->excluded_chunk_ids, index_chunk_ids);
  EXPECT_EQ(*table_scan_op->predicate(), *between_inclusive_(b, 42, 1337));
}

TEST_F(LQPTranslatorTest, PredicateNodeIndexScanFailsWhenNotApplicable) {
  if (!HYRISE_DEBUG) GTEST_SKIP();

  /**
   * Build LQP and translate to PQP
   */
  const auto stored_table_node = StoredTableNode::make("int_float_chunked");

  const auto table = Hyrise::get().storage_manager.get_table("int_float_chunked");
  std::vector<ColumnID> index_column_ids = {ColumnID{1}};
  std::vector<ChunkID> index_chunk_ids = {ChunkID{0}, ChunkID{2}};
  table->get_chunk(index_chunk_ids[0])->create_index<GroupKeyIndex>(index_column_ids);
  table->get_chunk(index_chunk_ids[1])->create_index<GroupKeyIndex>(index_column_ids);

  auto predicate_node = PredicateNode::make(equals_(stored_table_node->get_column("b"), 42));
  predicate_node->set_left_input(stored_table_node);
  auto predicate_node2 = PredicateNode::make(less_than_(stored_table_node->get_column("a"), 42));
  predicate_node2->set_left_input(predicate_node);

  // The optimizer should not set this ScanType in this situation
  predicate_node2->scan_type = ScanType::IndexScan;
  EXPECT_THROW(LQPTranslator{}.translate_node(predicate_node2), std::logic_error);
}

TEST_F(LQPTranslatorTest, ProjectionNode) {
  /**
   * Build LQP and translate to PQP
   */
  auto projection_node = ProjectionNode::make(expression_vector(int_float_a), int_float_node);
  const auto op = LQPTranslator{}.translate_node(projection_node);

  /**
   * Check PQP
   */
  const auto projection_op = std::dynamic_pointer_cast<Projection>(op);
  ASSERT_TRUE(projection_op);
  EXPECT_EQ(projection_op->expressions.size(), 1u);
  EXPECT_EQ(*projection_op->expressions[0], *PQPColumnExpression::from_table(*table_int_float, "a"));
}

TEST_F(LQPTranslatorTest, JoinNodeToJoinHash) {
  /**
   * Build LQP and translate to PQP
   */
  auto join_node = JoinNode::make(JoinMode::Inner, equals_(int_float2_b, int_float_b), int_float_node, int_float2_node);
  const auto op = LQPTranslator{}.translate_node(join_node);

  /**
   * Check PQP - for a inner-equi join, JoinHash should be used.
   */
  const auto join_op = std::dynamic_pointer_cast<JoinHash>(op);
  ASSERT_TRUE(join_op);
  EXPECT_EQ(join_op->primary_predicate().column_ids, ColumnIDPair(ColumnID{1}, ColumnID{1}));
  EXPECT_EQ(join_op->primary_predicate().predicate_condition, PredicateCondition::Equals);
  EXPECT_EQ(join_op->mode(), JoinMode::Inner);
}

TEST_F(LQPTranslatorTest, JoinNodeToJoinSortMerge) {
  /**
   * Build LQP and translate to PQP
   */
  auto join_node =
      JoinNode::make(JoinMode::Inner, less_than_(int_float_b, int_float2_b), int_float_node, int_float2_node);
  const auto op = LQPTranslator{}.translate_node(join_node);

  /**
   * Check PQP - JoinHash doesn't support non-equi joins, thus we fall back to JoinSortMerge
   */
  const auto join_op = std::dynamic_pointer_cast<JoinSortMerge>(op);
  ASSERT_TRUE(join_op);
  EXPECT_EQ(join_op->primary_predicate().column_ids, ColumnIDPair(ColumnID{1}, ColumnID{1}));
  EXPECT_EQ(join_op->primary_predicate().predicate_condition, PredicateCondition::LessThan);
  EXPECT_EQ(join_op->mode(), JoinMode::Inner);
}

TEST_F(LQPTranslatorTest, JoinNodeToJoinNestedLoop) {
  /**
   * Build LQP and translate to PQP
   */
  auto join_node =
      JoinNode::make(JoinMode::Inner, less_than_(int_float_a, int_float2_b), int_float_node, int_float2_node);
  const auto op = LQPTranslator{}.translate_node(join_node);

  /**
   * Check PQP - Neither JoinHash nor JoinSortMerge support non-equi joins on different column types. So we fall back to
   * JoinNestedLoop.
   */
  const auto join_op = std::dynamic_pointer_cast<JoinNestedLoop>(op);
  ASSERT_TRUE(join_op);
  EXPECT_EQ(join_op->primary_predicate().column_ids, ColumnIDPair(ColumnID{0}, ColumnID{1}));
  EXPECT_EQ(join_op->primary_predicate().predicate_condition, PredicateCondition::LessThan);
  EXPECT_EQ(join_op->mode(), JoinMode::Inner);
}

TEST_F(LQPTranslatorTest, AggregateNodeSimple) {
  /**
   * Build LQP and translate to PQP
   */
  // clang-format off
  const auto lqp =
  AggregateNode::make(expression_vector(int_float_a), expression_vector(sum_(add_(int_float_b, int_float_a))),
    ProjectionNode::make(expression_vector(int_float_b, int_float_a, add_(int_float_b, int_float_a)),
      int_float_node));
  // clang-format on
  const auto op = LQPTranslator{}.translate_node(lqp);

  /**
   * Check PQP
   */
  const auto aggregate_op = std::dynamic_pointer_cast<AggregateHash>(op);
  ASSERT_TRUE(aggregate_op);
  ASSERT_EQ(aggregate_op->aggregates().size(), 1u);
  ASSERT_EQ(aggregate_op->groupby_column_ids().size(), 1u);
  EXPECT_EQ(aggregate_op->groupby_column_ids().at(0), ColumnID{1});

  const auto aggregate_definition = aggregate_op->aggregates()[0];
  EXPECT_EQ(aggregate_definition.column, ColumnID{2});
  EXPECT_EQ(aggregate_definition.function, AggregateFunction::Sum);
}

TEST_F(LQPTranslatorTest, JoinAndPredicates) {
  /**
   * Build LQP and translate to PQP
   */
  auto predicate_node_left = PredicateNode::make(equals_(int_float_a, 42), int_float_node);
  auto predicate_node_right = PredicateNode::make(greater_than_(int_float2_b, 30.0), int_float2_node);

  auto join_node = JoinNode::make(JoinMode::Inner, equals_(int_float_a, int_float2_a));
  join_node->set_left_input(predicate_node_left);
  join_node->set_right_input(predicate_node_right);

  const auto op = LQPTranslator{}.translate_node(join_node);

  /**
   * Check PQP
   */
  const auto a = PQPColumnExpression::from_table(*table_int_float, "a");
  const auto b = PQPColumnExpression::from_table(*table_int_float2, "b");

  const auto join_op = std::dynamic_pointer_cast<const JoinHash>(op);
  ASSERT_TRUE(join_op);

  const auto predicate_op_left = std::dynamic_pointer_cast<const TableScan>(join_op->input_left());
  ASSERT_TRUE(predicate_op_left);
  ASSERT_EQ(*predicate_op_left->predicate(), *equals_(a, 42));

  const auto predicate_op_right = std::dynamic_pointer_cast<const TableScan>(join_op->input_right());
  ASSERT_TRUE(predicate_op_right);
  ASSERT_EQ(*predicate_op_right->predicate(), *greater_than_(b, 30.0));

  const auto get_table_op_left = std::dynamic_pointer_cast<const GetTable>(predicate_op_left->input_left());
  ASSERT_TRUE(get_table_op_left);
  EXPECT_EQ(get_table_op_left->table_name(), "table_int_float");

  const auto get_table_op_right = std::dynamic_pointer_cast<const GetTable>(predicate_op_right->input_left());
  ASSERT_TRUE(get_table_op_right);
  EXPECT_EQ(get_table_op_right->table_name(), "table_int_float2");
}

TEST_F(LQPTranslatorTest, LimitNode) {
  /**
   * Build LQP and translate to PQP
   */
  const auto stored_table_node = StoredTableNode::make("table_int_float");

  auto limit_node = LimitNode::make(value_(2));
  limit_node->set_left_input(stored_table_node);

  /**
   * Check PQP
   */
  const auto op = LQPTranslator{}.translate_node(limit_node);
  const auto limit_op = std::dynamic_pointer_cast<Limit>(op);
  ASSERT_TRUE(limit_op);
  EXPECT_EQ(*limit_op->row_count_expression(), *value_(2));
}

TEST_F(LQPTranslatorTest, DiamondShapeSimple) {
  /**
   * Test that
   *
   *    _____union____
   *   /              \
   *  predicate_a     predicate_b
   *  \                /
   *   \__predicate_c_/
   *          |
   *     table_int_float2
   *
   * has a diamond shape in the PQP as well. If it wouldn't have it might look like this:
   *
   *    _____union____
   *   /              \
   *  predicate_a     predicate_b
   *      |             |
   *  predicate_c(1)  predicate_c(2)
   *      |             |
   * table_int_float2 table_int_float2
   *
   * which is still semantically correct, but would mean predicate_c gets executed twice
   */

  auto predicate_node_a = PredicateNode::make(equals_(int_float2_a, 3));
  auto predicate_node_b = PredicateNode::make(equals_(int_float2_a, 4));
  auto predicate_node_c = PredicateNode::make(equals_(int_float2_b, 5));
  auto union_node = UnionNode::make(UnionMode::Positions);
  const auto& lqp = union_node;

  union_node->set_left_input(predicate_node_a);
  union_node->set_right_input(predicate_node_b);
  predicate_node_a->set_left_input(predicate_node_c);
  predicate_node_b->set_left_input(predicate_node_c);
  predicate_node_c->set_left_input(int_float2_node);

  const auto pqp = LQPTranslator{}.translate_node(lqp);

  ASSERT_NE(pqp, nullptr);
  ASSERT_NE(pqp->input_left(), nullptr);
  ASSERT_NE(pqp->input_right(), nullptr);
  ASSERT_NE(pqp->input_left()->input_left(), nullptr);
  ASSERT_NE(pqp->input_right()->input_left(), nullptr);
  EXPECT_EQ(pqp->input_left()->input_left(), pqp->input_right()->input_left());
  EXPECT_EQ(pqp->input_left()->input_left()->input_left(), pqp->input_right()->input_left()->input_left());
}

TEST_F(LQPTranslatorTest, ReusingPQPSelfJoin) {
  /**
   * Test that LQP:
   *
   *               Projection b, b
   *                      |
   *           ______Cross Join______
   *          /                      \
   *     Predicate                Predicate
   *     b = 456.7f               b = 457.7f
   *         |                        |
   *     Predicate                Predicate
   *     a = 12345                a = 12345
   *         |                        |
   *    StoredTable              StoredTable
   *  table_int_float2         table_int_float2
   *
   * is translated to PQP:
   *
   *           Projection b, b
   *                  |
   *       ________Product_______
   *      /                      \
   *  TableScan               TableScan
   *  b = 456.7f              b = 457.7f
   *     \________TableScan______/
   *              a = 12345
   *                  |
   *              GetTable
   *           table_int_float2
   *    
   */

  auto int_float_node_1 = StoredTableNode::make("table_int_float2");
  auto int_float_a_1 = int_float_node_1->get_column("a");
  auto int_float_b_1 = int_float_node_1->get_column("b");

  auto int_float_node_2 = StoredTableNode::make("table_int_float2");
  auto int_float_a_2 = int_float_node_2->get_column("a");
  auto int_float_b_2 = int_float_node_2->get_column("b");

  // clang-format off
  const auto lqp =
  ProjectionNode::make(expression_vector(int_float_b_1, int_float_b_2),
    JoinNode::make(JoinMode::Cross,
      PredicateNode::make(equals_(int_float_b_1, 456.7f),
        PredicateNode::make(equals_(int_float_a_1, 12345),
          int_float_node_1)),
      PredicateNode::make(equals_(int_float_b_2, 457.7f),
        PredicateNode::make(equals_(int_float_a_2, 12345),
          int_float_node_2))));
  // clang-format on

  const auto pqp = LQPTranslator{}.translate_node(lqp);

  const auto projection = std::dynamic_pointer_cast<const Projection>(pqp);
  ASSERT_NE(projection, nullptr);

  const auto product = std::dynamic_pointer_cast<const Product>(projection->input_left());
  ASSERT_NE(product, nullptr);

  const auto table_scan_b_1 = std::dynamic_pointer_cast<const TableScan>(product->input_left());
  const auto table_scan_b_2 = std::dynamic_pointer_cast<const TableScan>(product->input_right());
  ASSERT_NE(table_scan_b_1, nullptr);
  ASSERT_NE(table_scan_b_2, nullptr);
  ASSERT_NE(table_scan_b_1, table_scan_b_2);

  const auto table_scan_a_1 = std::dynamic_pointer_cast<const TableScan>(table_scan_b_1->input_left());
  const auto table_scan_a_2 = std::dynamic_pointer_cast<const TableScan>(table_scan_b_2->input_left());
  ASSERT_NE(table_scan_a_1, nullptr);
  ASSERT_NE(table_scan_a_2, nullptr);
  ASSERT_EQ(table_scan_a_1, table_scan_a_2);

  const auto get_table = std::dynamic_pointer_cast<const GetTable>(table_scan_a_1->input_left());
  ASSERT_NE(get_table, nullptr);

  ASSERT_EQ(get_table->input_left(), nullptr);
}

TEST_F(LQPTranslatorTest, ReuseInputExpressions) {
  // If the result of a (sub)expression is available in an input column, the expression should not be redundantly
  // evaluated

  // clang-format off
  const auto lqp =
  PredicateNode::make(greater_than_(add_(add_(int_float_a, int_float_b), 3), 2),
    ProjectionNode::make(expression_vector(add_(add_(int_float_a, int_float_b), 3)),
      ProjectionNode::make(expression_vector(5, add_(int_float_a, int_float_b)),
        int_float_node)));
  // clang-format on

  const auto pqp = LQPTranslator{}.translate_node(lqp);

  ASSERT_NE(pqp, nullptr);
  ASSERT_NE(pqp->input_left(), nullptr);
  ASSERT_NE(pqp->input_left()->input_left(), nullptr);

  const auto table_scan = std::dynamic_pointer_cast<const TableScan>(pqp);
  const auto projection_a = std::dynamic_pointer_cast<const Projection>(pqp->input_left());
  const auto projection_b = std::dynamic_pointer_cast<const Projection>(pqp->input_left()->input_left());

  ASSERT_NE(table_scan, nullptr);
  ASSERT_NE(projection_a, nullptr);
  ASSERT_NE(projection_b, nullptr);

  const auto a_plus_b_in_temporary_column = pqp_column_(ColumnID{1}, DataType::Float, false, "a + b");
  const auto scan_column_expression =
      std::dynamic_pointer_cast<PQPColumnExpression>(table_scan->predicate()->arguments.at(0));

  ASSERT_TRUE(scan_column_expression);
  EXPECT_EQ(scan_column_expression->column_id, ColumnID{0});
  EXPECT_EQ(*projection_a->expressions.at(0), *add_(a_plus_b_in_temporary_column, 3));
}

TEST_F(LQPTranslatorTest, ReuseSubqueryExpression) {
  // Test that sub query expressions whose result is available in an output column of the input operator are not
  // evaluated redundantly

  // clang-format off
  const auto subquery =
  ProjectionNode::make(expression_vector(add_(1, 2)),
    DummyTableNode::make());

  const auto subquery_a = lqp_subquery_(subquery);
  const auto subquery_b = lqp_subquery_(subquery);

  const auto lqp =
  ProjectionNode::make(expression_vector(add_(subquery_a, 3)),
    ProjectionNode::make(expression_vector(5, subquery_b),
      int_float_node));
  // clang-format on

  const auto pqp = LQPTranslator{}.translate_node(lqp);

  ASSERT_NE(pqp, nullptr);
  ASSERT_NE(pqp->input_left(), nullptr);

  const auto projection_a = std::dynamic_pointer_cast<const Projection>(pqp);
  const auto projection_b = std::dynamic_pointer_cast<const Projection>(pqp->input_left());

  ASSERT_NE(projection_a, nullptr);
  ASSERT_NE(projection_b, nullptr);

  // As subquery columns without an explicit alias get the LQP/PQP address as their name, we need to retrieve it first.
  const auto column_name = subquery_a->as_column_name();
  const auto subquery_in_temporary_column = pqp_column_(ColumnID{1}, DataType::Int, false, column_name);

  EXPECT_EQ(*projection_a->expressions.at(0), *add_(subquery_in_temporary_column, 3));
}

TEST_F(LQPTranslatorTest, CreateTable) {
  auto column_definitions = TableColumnDefinitions{};
  column_definitions.emplace_back("a", DataType::Int, false);
  column_definitions.emplace_back("b", DataType::Float, true);

  const auto lqp =
      CreateTableNode::make("t", false, StaticTableNode::make(Table::create_dummy_table(column_definitions)));

  const auto pqp = LQPTranslator{}.translate_node(lqp);

  EXPECT_EQ(pqp->type(), OperatorType::CreateTable);

  const auto create_table = std::dynamic_pointer_cast<CreateTable>(pqp);
  EXPECT_EQ(create_table->table_name, "t");

  // CreateTable input must be executed to enable access to column definitions
  create_table->mutable_input_left()->execute();
  EXPECT_EQ(create_table->column_definitions(), column_definitions);
}

TEST_F(LQPTranslatorTest, StaticTable) {
  auto column_definitions = TableColumnDefinitions{};
  column_definitions.emplace_back("a", DataType::Int, false);
  column_definitions.emplace_back("b", DataType::Float, true);

  const auto dummy_table = Table::create_dummy_table(column_definitions);

  const auto lqp = StaticTableNode::make(dummy_table);

  const auto pqp = LQPTranslator{}.translate_node(lqp);

  EXPECT_EQ(pqp->type(), OperatorType::TableWrapper);

  const auto table_wrapper = std::dynamic_pointer_cast<TableWrapper>(pqp);
  EXPECT_EQ(table_wrapper->table, dummy_table);
}

TEST_F(LQPTranslatorTest, DropTable) {
  const auto lqp = DropTableNode::make("t", false);

  const auto pqp = LQPTranslator{}.translate_node(lqp);

  EXPECT_EQ(pqp->type(), OperatorType::DropTable);
  EXPECT_EQ(pqp->input_left(), nullptr);

  const auto drop_table = std::dynamic_pointer_cast<DropTable>(pqp);
  EXPECT_EQ(drop_table->table_name, "t");
}

TEST_F(LQPTranslatorTest, CreatePreparedPlan) {
  const auto prepared_plan = std::make_shared<PreparedPlan>(DummyTableNode::make(), std::vector<ParameterID>{});
  const auto lqp = CreatePreparedPlanNode::make("p", prepared_plan);

  const auto pqp = LQPTranslator{}.translate_node(lqp);

  EXPECT_EQ(pqp->type(), OperatorType::CreatePreparedPlan);
  EXPECT_EQ(pqp->input_left(), nullptr);

  const auto prepare = std::dynamic_pointer_cast<CreatePreparedPlan>(pqp);
  EXPECT_EQ(prepare->prepared_plan(), prepared_plan);
}

}  // namespace opossum
