#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "gtest/gtest.h"

#include "expression/aggregate_expression.hpp"
#include "expression/arithmetic_expression.hpp"
#include "expression/expression_functional.hpp"
#include "expression/expression_utils.hpp"
#include "expression/lqp_column_expression.hpp"
#include "expression/pqp_column_expression.hpp"
#include "expression/pqp_select_expression.hpp"
#include "logical_query_plan/aggregate_node.hpp"
#include "logical_query_plan/dummy_table_node.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/limit_node.hpp"
#include "logical_query_plan/lqp_translator.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/projection_node.hpp"
#include "logical_query_plan/show_columns_node.hpp"
#include "logical_query_plan/show_tables_node.hpp"
#include "logical_query_plan/sort_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "logical_query_plan/union_node.hpp"
#include "operators/aggregate.hpp"
#include "operators/get_table.hpp"
#include "operators/index_scan.hpp"
#include "operators/join_hash.hpp"
#include "operators/join_sort_merge.hpp"
#include "operators/limit.hpp"
#include "operators/maintenance/show_columns.hpp"
#include "operators/maintenance/show_tables.hpp"
#include "operators/product.hpp"
#include "operators/projection.hpp"
#include "operators/sort.hpp"
#include "operators/table_scan.hpp"
#include "operators/union_positions.hpp"
#include "storage/chunk_encoder.hpp"
#include "storage/index/group_key/group_key_index.hpp"
#include "storage/storage_manager.hpp"
#include "utils/load_table.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

class LQPTranslatorTest : public ::testing::Test {
 public:
  void SetUp() override {
    table_int_float = load_table("src/test/tables/int_float.tbl");
    table_int_string = load_table("src/test/tables/int_string.tbl");
    table_int_float2 = load_table("src/test/tables/int_float2.tbl");
    table_int_float5 = load_table("src/test/tables/int_float5.tbl");
    table_alias_name = load_table("src/test/tables/table_alias_name.tbl");

    StorageManager::get().add_table("table_int_float", table_int_float);
    StorageManager::get().add_table("table_int_string", table_int_string);
    StorageManager::get().add_table("table_int_float2", table_int_float2);
    StorageManager::get().add_table("table_int_float5", table_int_float5);
    StorageManager::get().add_table("table_alias_name", table_alias_name);
    StorageManager::get().add_table("int_float_chunked", load_table("src/test/tables/int_float.tbl", 1));
    ChunkEncoder::encode_all_chunks(StorageManager::get().get_table("int_float_chunked"));

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

  void TearDown() override { StorageManager::reset(); }

  const std::vector<ChunkID> get_included_chunk_ids(const std::shared_ptr<const IndexScan>& index_scan) {
    return index_scan->_included_chunk_ids;
  }

  const std::vector<ChunkID> get_excluded_chunk_ids(const std::shared_ptr<const TableScan>& table_scan) {
    return table_scan->_excluded_chunk_ids;
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
  ASSERT_TRUE(table_scan_op);
  EXPECT_EQ(table_scan_op->predicate().column_id, ColumnID{1});
  EXPECT_EQ(table_scan_op->predicate().predicate_condition, PredicateCondition::LessThan);
  EXPECT_EQ(table_scan_op->predicate().value, AllParameterVariant(5));

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
  ASSERT_TRUE(table_scan_op);
  EXPECT_EQ(table_scan_op->predicate().column_id, ColumnID{1});
  EXPECT_EQ(table_scan_op->predicate().predicate_condition, PredicateCondition::Like);
  EXPECT_EQ(table_scan_op->predicate().value, AllParameterVariant("hello%"));

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
  ASSERT_TRUE(table_scan_op);
  EXPECT_EQ(table_scan_op->predicate().column_id, ColumnID{1});
  EXPECT_EQ(table_scan_op->predicate().predicate_condition, PredicateCondition::IsNotNull);
  EXPECT_TRUE(is_variant(table_scan_op->predicate().value));

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
  const auto predicate_node = PredicateNode::make(between_(5, int_float_a, int_float_b), int_float_node);
  const auto pqp = LQPTranslator{}.translate_node(predicate_node);

  /**
   * Check PQP
   */
  const auto upper_bound_scan_op = std::dynamic_pointer_cast<const TableScan>(pqp);
  ASSERT_TRUE(upper_bound_scan_op);
  EXPECT_EQ(upper_bound_scan_op->predicate().column_id, ColumnID{1});
  EXPECT_EQ(upper_bound_scan_op->predicate().predicate_condition, PredicateCondition::GreaterThanEquals);
  EXPECT_EQ(upper_bound_scan_op->predicate().value, AllParameterVariant(5));

  const auto lower_bound_scan_op = std::dynamic_pointer_cast<const TableScan>(pqp->input_left());
  ASSERT_TRUE(lower_bound_scan_op);
  EXPECT_EQ(lower_bound_scan_op->predicate().column_id, ColumnID{0});
  EXPECT_EQ(lower_bound_scan_op->predicate().predicate_condition, PredicateCondition::LessThanEquals);
  EXPECT_EQ(lower_bound_scan_op->predicate().value, AllParameterVariant(5));

  const auto get_table_op = std::dynamic_pointer_cast<const GetTable>(pqp->input_left()->input_left());
  ASSERT_TRUE(get_table_op);
  EXPECT_EQ(get_table_op->table_name(), "table_int_float");
}

TEST_F(LQPTranslatorTest, SelectExpressionCorrelated) {
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
  const auto subselect_lqp =
  AggregateNode::make(expression_vector(), expression_vector(min_(a_plus_a_plus_d)),
    ProjectionNode::make(expression_vector(a_plus_a_plus_d),
      int_float_node));

  const auto subselect = lqp_select_(subselect_lqp, std::make_pair(ParameterID{0}, int_float5_a),
                                 std::make_pair(ParameterID{1}, int_float5_d));

  const auto lqp =
  ProjectionNode::make(expression_vector(subselect, int_float5_a), int_float5_node);
  // clang-format on

  const auto pqp = LQPTranslator{}.translate_node(lqp);

  ASSERT_EQ(pqp->type(), OperatorType::Projection);
  ASSERT_TRUE(pqp->input_left());
  ASSERT_EQ(pqp->input_left()->type(), OperatorType::GetTable);

  const auto projection = std::static_pointer_cast<const Projection>(pqp);
  ASSERT_EQ(projection->expressions.size(), 2u);

  const auto expression_a = std::dynamic_pointer_cast<PQPSelectExpression>(projection->expressions.at(0));
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

TEST_F(LQPTranslatorTest, JoinNonEqui) {
  /**
   * Build LQP and translate to PQP
   *
   * LQP resembles:
   *   SELECT * FROM int_float2 JOIN int_float ON int_float.a < int_float2.b
   */
  // clang-format off
  const auto lqp =
  JoinNode::make(JoinMode::Inner, less_than_(int_float_a, int_float2_b),
    int_float2_node, int_float_node);
  // clang-format on
  const auto pqp = LQPTranslator{}.translate_node(lqp);

  /**
   * Check PQP
   */
  const auto join_sort_merge = std::dynamic_pointer_cast<JoinSortMerge>(pqp);
  ASSERT_TRUE(join_sort_merge);
  EXPECT_EQ(join_sort_merge->column_ids().first, ColumnID{1});
  EXPECT_EQ(join_sort_merge->column_ids().second, ColumnID{0});
  EXPECT_EQ(join_sort_merge->predicate_condition(), PredicateCondition::GreaterThan);

  const auto get_table_int_float2 = std::dynamic_pointer_cast<const GetTable>(join_sort_merge->input_left());
  ASSERT_TRUE(get_table_int_float2);
  EXPECT_EQ(get_table_int_float2->table_name(), "table_int_float2");

  const auto get_table_int_float = std::dynamic_pointer_cast<const GetTable>(join_sort_merge->input_right());
  ASSERT_TRUE(get_table_int_float);
  EXPECT_EQ(get_table_int_float->table_name(), "table_int_float");
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
  ASSERT_TRUE(table_scan_op);
  EXPECT_EQ(table_scan_op->predicate().column_id, ColumnID{1} /* "b" */);
  EXPECT_EQ(table_scan_op->predicate().predicate_condition, PredicateCondition::Equals);
  EXPECT_EQ(table_scan_op->predicate().value, AllParameterVariant(42));
}

TEST_F(LQPTranslatorTest, PredicateNodeSupportedBetweenScan) {
  /**
   * Build LQP and translate to PQP
   */
  auto predicate_node = PredicateNode::make(between_(int_float_a, 42, 1337), int_float_node);
  const auto op = LQPTranslator{}.translate_node(predicate_node);

  /**
   * Check PQP
   */
  const auto table_scan_op = std::dynamic_pointer_cast<TableScan>(op);
  ASSERT_TRUE(table_scan_op);
  EXPECT_EQ(table_scan_op->predicate().column_id, ColumnID{0} /* "a" */);
  EXPECT_EQ(table_scan_op->predicate().predicate_condition, PredicateCondition::Between);
  EXPECT_EQ(table_scan_op->predicate().value, AllParameterVariant(42));
  EXPECT_EQ(*table_scan_op->predicate().value2, AllParameterVariant(1337));
}

TEST_F(LQPTranslatorTest, PredicateNodeNonSupportedBetweenScan) {
  /**
   * Build LQP and translate to PQP
   */

  // Because the BETWEEN TableScan does not handle varying types (yet), two scans should be created
  auto predicate_node = PredicateNode::make(between_(int_float_a, 42, 1337.5), int_float_node);
  const auto op = LQPTranslator{}.translate_node(predicate_node);

  /**
   * Check PQP
   */
  const auto table_scan_op2 = std::dynamic_pointer_cast<TableScan>(op);
  ASSERT_TRUE(table_scan_op2);
  EXPECT_EQ(table_scan_op2->predicate().column_id, ColumnID{0} /* "a" */);
  EXPECT_EQ(table_scan_op2->predicate().predicate_condition, PredicateCondition::LessThanEquals);
  EXPECT_EQ(table_scan_op2->predicate().value, AllParameterVariant(1337.5));

  const auto table_scan_op = std::dynamic_pointer_cast<const TableScan>(table_scan_op2->input_left());
  ASSERT_TRUE(table_scan_op);
  EXPECT_EQ(table_scan_op->predicate().column_id, ColumnID{0} /* "a" */);
  EXPECT_EQ(table_scan_op->predicate().predicate_condition, PredicateCondition::GreaterThanEquals);
  EXPECT_EQ(table_scan_op->predicate().value, AllParameterVariant(42));
}

TEST_F(LQPTranslatorTest, PredicateNodeIndexScan) {
  /**
   * Build LQP and translate to PQP
   */
  const auto stored_table_node = StoredTableNode::make("int_float_chunked");

  const auto table = StorageManager::get().get_table("int_float_chunked");
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
  const auto union_op = std::dynamic_pointer_cast<UnionPositions>(op);
  ASSERT_TRUE(union_op);

  const auto index_scan_op = std::dynamic_pointer_cast<const IndexScan>(op->input_left());
  ASSERT_TRUE(index_scan_op);
  EXPECT_EQ(get_included_chunk_ids(index_scan_op), index_chunk_ids);

  const auto table_scan_op = std::dynamic_pointer_cast<const TableScan>(op->input_right());
  ASSERT_TRUE(table_scan_op);
  EXPECT_EQ(get_excluded_chunk_ids(table_scan_op), index_chunk_ids);
  EXPECT_EQ(table_scan_op->predicate().column_id, ColumnID{1} /* "a" */);
  EXPECT_EQ(table_scan_op->predicate().predicate_condition, PredicateCondition::Equals);
  EXPECT_EQ(table_scan_op->predicate().value, AllParameterVariant(42));
}

TEST_F(LQPTranslatorTest, PredicateNodeBinaryIndexScan) {
  /**
   * Build LQP and translate to PQP
   */
  const auto stored_table_node = StoredTableNode::make("int_float_chunked");

  const auto table = StorageManager::get().get_table("int_float_chunked");
  std::vector<ColumnID> index_column_ids = {ColumnID{1}};
  std::vector<ChunkID> index_chunk_ids = {ChunkID{0}, ChunkID{2}};
  table->get_chunk(index_chunk_ids[0])->create_index<GroupKeyIndex>(index_column_ids);
  table->get_chunk(index_chunk_ids[1])->create_index<GroupKeyIndex>(index_column_ids);

  auto predicate_node = PredicateNode::make(between_(stored_table_node->get_column("b"), 42, 1337));
  predicate_node->set_left_input(stored_table_node);
  predicate_node->scan_type = ScanType::IndexScan;
  const auto op = LQPTranslator{}.translate_node(predicate_node);

  /**
   * Check PQP
   */
  const auto union_op = std::dynamic_pointer_cast<UnionPositions>(op);
  ASSERT_TRUE(union_op);

  const auto index_scan_op = std::dynamic_pointer_cast<const IndexScan>(op->input_left());
  ASSERT_TRUE(index_scan_op);
  EXPECT_EQ(get_included_chunk_ids(index_scan_op), index_chunk_ids);
  EXPECT_EQ(index_scan_op->input_left()->type(), OperatorType::GetTable);

  const auto table_scan_op = std::dynamic_pointer_cast<const TableScan>(op->input_right());
  ASSERT_TRUE(table_scan_op);
  EXPECT_EQ(get_excluded_chunk_ids(table_scan_op), index_chunk_ids);
  EXPECT_EQ(table_scan_op->predicate().column_id, ColumnID{1} /* "a" */);
  EXPECT_EQ(table_scan_op->predicate().predicate_condition, PredicateCondition::LessThanEquals);
  EXPECT_EQ(table_scan_op->predicate().value, AllParameterVariant(1337));

  const auto table_scan_op2 = std::dynamic_pointer_cast<const TableScan>(table_scan_op->input_left());
  ASSERT_TRUE(table_scan_op2);
  EXPECT_EQ(get_excluded_chunk_ids(table_scan_op2), index_chunk_ids);
  EXPECT_EQ(table_scan_op2->predicate().column_id, ColumnID{1} /* "a" */);
  EXPECT_EQ(table_scan_op2->predicate().predicate_condition, PredicateCondition::GreaterThanEquals);
  EXPECT_EQ(table_scan_op2->predicate().value, AllParameterVariant(42));
}

TEST_F(LQPTranslatorTest, PredicateNodeIndexScanFailsWhenNotApplicable) {
  if (!IS_DEBUG) return;
  /**
   * Build LQP and translate to PQP
   */
  const auto stored_table_node = StoredTableNode::make("int_float_chunked");

  const auto table = StorageManager::get().get_table("int_float_chunked");
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

TEST_F(LQPTranslatorTest, JoinNode) {
  /**
   * Build LQP and translate to PQP
   */
  auto join_node = JoinNode::make(JoinMode::Outer, equals_(int_float_b, int_float2_a), int_float_node, int_float2_node);
  const auto op = LQPTranslator{}.translate_node(join_node);

  /**
   * Check PQP
   */
  const auto join_op = std::dynamic_pointer_cast<JoinSortMerge>(op);
  ASSERT_TRUE(join_op);
  EXPECT_EQ(join_op->column_ids(), ColumnIDPair(ColumnID{1}, ColumnID{0}));
  EXPECT_EQ(join_op->predicate_condition(), PredicateCondition::Equals);
  EXPECT_EQ(join_op->mode(), JoinMode::Outer);
}

TEST_F(LQPTranslatorTest, ShowTablesNode) {
  /**
   * Build LQP and translate to PQP
   */
  const auto show_tables_node = ShowTablesNode::make();
  const auto op = LQPTranslator{}.translate_node(show_tables_node);

  /**
   * Check PQP
   */
  const auto show_tables_op = std::dynamic_pointer_cast<ShowTables>(op);
  ASSERT_TRUE(show_tables_op);
  EXPECT_EQ(show_tables_op->name(), "ShowTables");
}

TEST_F(LQPTranslatorTest, ShowColumnsNode) {
  /**
   * Build LQP and translate to PQP
   */
  const auto show_column_node = ShowColumnsNode::make("table_a");
  const auto op = LQPTranslator{}.translate_node(show_column_node);

  /**
   * Check PQP
   */
  const auto show_columns_op = std::dynamic_pointer_cast<ShowColumns>(op);
  ASSERT_TRUE(show_columns_op);
  EXPECT_EQ(show_columns_op->name(), "ShowColumns");
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
  const auto aggregate_op = std::dynamic_pointer_cast<Aggregate>(op);
  ASSERT_TRUE(aggregate_op);
  ASSERT_EQ(aggregate_op->aggregates().size(), 1u);
  ASSERT_EQ(aggregate_op->groupby_column_ids().size(), 1u);
  EXPECT_EQ(aggregate_op->groupby_column_ids().at(0), ColumnID{1});

  const auto aggregate_definition = aggregate_op->aggregates()[0];
  EXPECT_EQ(aggregate_definition.column, ColumnID{2});
  EXPECT_EQ(aggregate_definition.function, AggregateFunction::Sum);
}

TEST_F(LQPTranslatorTest, MultipleNodesHierarchy) {
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
  const auto join_op = std::dynamic_pointer_cast<const JoinHash>(op);
  ASSERT_TRUE(join_op);

  const auto predicate_op_left = std::dynamic_pointer_cast<const TableScan>(join_op->input_left());
  ASSERT_TRUE(predicate_op_left);
  EXPECT_EQ(predicate_op_left->predicate().predicate_condition, PredicateCondition::Equals);

  const auto predicate_op_right = std::dynamic_pointer_cast<const TableScan>(join_op->input_right());
  ASSERT_TRUE(predicate_op_right);
  EXPECT_EQ(predicate_op_right->predicate().predicate_condition, PredicateCondition::GreaterThan);

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

  EXPECT_EQ(table_scan->predicate().column_id, ColumnID{0});
  EXPECT_EQ(*projection_a->expressions.at(0), *add_(a_plus_b_in_temporary_column, 3));
}

TEST_F(LQPTranslatorTest, ReuseSelectExpression) {
  // Test that select expressions whose result is available in an output column of the input operator are not evaluated
  // redundantly

  // clang-format off
  const auto select_lqp =
  ProjectionNode::make(expression_vector(add_(1, 2)),
    DummyTableNode::make());

  const auto select_a = lqp_select_(select_lqp);
  const auto select_b = lqp_select_(select_lqp);

  const auto lqp =
  ProjectionNode::make(expression_vector(add_(select_a, 3)),
    ProjectionNode::make(expression_vector(5, select_b),
      int_float_node));
  // clang-format on

  const auto pqp = LQPTranslator{}.translate_node(lqp);

  ASSERT_NE(pqp, nullptr);
  ASSERT_NE(pqp->input_left(), nullptr);

  const auto projection_a = std::dynamic_pointer_cast<const Projection>(pqp);
  const auto projection_b = std::dynamic_pointer_cast<const Projection>(pqp->input_left());

  ASSERT_NE(projection_a, nullptr);
  ASSERT_NE(projection_b, nullptr);

  const auto select_in_temporary_column = pqp_column_(ColumnID{1}, DataType::Int, false, "SUBSELECT");

  EXPECT_EQ(*projection_a->expressions.at(0), *add_(select_in_temporary_column, 3));
}

}  // namespace opossum
