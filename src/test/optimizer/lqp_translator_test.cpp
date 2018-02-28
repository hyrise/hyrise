#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "base_test.hpp"
#include "gtest/gtest.h"

#include "logical_query_plan/aggregate_node.hpp"
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
#include "operators/pqp_expression.hpp"
#include "operators/projection.hpp"
#include "operators/sort.hpp"
#include "operators/table_scan.hpp"
#include "operators/union_positions.hpp"
#include "storage/chunk_encoder.hpp"
#include "storage/index/group_key/group_key_index.hpp"
#include "storage/storage_manager.hpp"

namespace opossum {

class LQPTranslatorTest : public BaseTest {
 protected:
  void SetUp() override {
    StorageManager::get().add_table("table_int_float", load_table("src/test/tables/int_float.tbl", Chunk::MAX_SIZE));
    StorageManager::get().add_table("table_int_float2", load_table("src/test/tables/int_float2.tbl", Chunk::MAX_SIZE));
    StorageManager::get().add_table("table_alias_name",
                                    load_table("src/test/tables/table_alias_name.tbl", Chunk::MAX_SIZE));
    StorageManager::get().add_table("table_int_float_chunked", load_table("src/test/tables/int_float.tbl", 1));
    ChunkEncoder::encode_all_chunks(StorageManager::get().get_table("table_int_float_chunked"));
  }

  const std::vector<ChunkID> get_included_chunk_ids(const std::shared_ptr<const IndexScan>& index_scan) {
    return index_scan->_included_chunk_ids;
  }

  const std::vector<ChunkID> get_excluded_chunk_ids(const std::shared_ptr<const TableScan>& table_scan) {
    return table_scan->_excluded_chunk_ids;
  }
};

TEST_F(LQPTranslatorTest, StoredTableNode) {
  /**
   * Build LQP and translate to PQP
   */
  const auto node = StoredTableNode::make("table_int_float");
  const auto op = LQPTranslator{}.translate_node(node);

  /**
   * Check PQP
   */
  const auto get_table_op = std::dynamic_pointer_cast<GetTable>(op);
  ASSERT_TRUE(get_table_op);
  EXPECT_EQ(get_table_op->table_name(), "table_int_float");
}

TEST_F(LQPTranslatorTest, PredicateNodeUnaryScan) {
  /**
   * Build LQP and translate to PQP
   */
  const auto stored_table_node = StoredTableNode::make("table_int_float");
  auto predicate_node =
      PredicateNode::make(LQPColumnReference(stored_table_node, ColumnID{1}), PredicateCondition::Equals, 42);
  predicate_node->set_left_input(stored_table_node);
  const auto op = LQPTranslator{}.translate_node(predicate_node);

  /**
   * Check PQP
   */
  const auto table_scan_op = std::dynamic_pointer_cast<TableScan>(op);
  ASSERT_TRUE(table_scan_op);
  EXPECT_EQ(table_scan_op->left_column_id(), ColumnID{1} /* "a" */);
  EXPECT_EQ(table_scan_op->predicate_condition(), PredicateCondition::Equals);
  EXPECT_EQ(table_scan_op->right_parameter(), AllParameterVariant(42));
}

TEST_F(LQPTranslatorTest, PredicateNodeBinaryScan) {
  /**
   * Build LQP and translate to PQP
   */
  const auto stored_table_node = StoredTableNode::make("table_int_float");
  auto predicate_node = PredicateNode::make(LQPColumnReference(stored_table_node, ColumnID{0}),
                                            PredicateCondition::Between, AllParameterVariant(42), AllTypeVariant(1337));
  predicate_node->set_left_input(stored_table_node);
  const auto op = LQPTranslator{}.translate_node(predicate_node);

  /**
   * Check PQP
   */
  const auto table_scan_op2 = std::dynamic_pointer_cast<TableScan>(op);
  ASSERT_TRUE(table_scan_op2);
  EXPECT_EQ(table_scan_op2->left_column_id(), ColumnID{0} /* "a" */);
  EXPECT_EQ(table_scan_op2->predicate_condition(), PredicateCondition::LessThanEquals);
  EXPECT_EQ(table_scan_op2->right_parameter(), AllParameterVariant(1337));

  const auto table_scan_op = std::dynamic_pointer_cast<const TableScan>(table_scan_op2->input_left());
  ASSERT_TRUE(table_scan_op);
  EXPECT_EQ(table_scan_op->left_column_id(), ColumnID{0} /* "a" */);
  EXPECT_EQ(table_scan_op->predicate_condition(), PredicateCondition::GreaterThanEquals);
  EXPECT_EQ(table_scan_op->right_parameter(), AllParameterVariant(42));
}

TEST_F(LQPTranslatorTest, PredicateNodeIndexScan) {
  /**
   * Build LQP and translate to PQP
   */
  const auto stored_table_node = StoredTableNode::make("table_int_float_chunked");

  const auto table = StorageManager::get().get_table("table_int_float_chunked");
  std::vector<ColumnID> index_column_ids = {ColumnID{1}};
  std::vector<ChunkID> index_chunk_ids = {ChunkID{0}, ChunkID{2}};
  table->get_chunk(index_chunk_ids[0])->create_index<GroupKeyIndex>(index_column_ids);
  table->get_chunk(index_chunk_ids[1])->create_index<GroupKeyIndex>(index_column_ids);

  auto predicate_node =
      PredicateNode::make(LQPColumnReference(stored_table_node, ColumnID{1}), PredicateCondition::Equals, 42);
  predicate_node->set_left_input(stored_table_node);
  predicate_node->set_scan_type(ScanType::IndexScan);
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
  EXPECT_EQ(table_scan_op->left_column_id(), ColumnID{1} /* "a" */);
  EXPECT_EQ(table_scan_op->predicate_condition(), PredicateCondition::Equals);
  EXPECT_EQ(table_scan_op->right_parameter(), AllParameterVariant(42));
}

TEST_F(LQPTranslatorTest, PredicateNodeBinaryIndexScan) {
  /**
   * Build LQP and translate to PQP
   */
  const auto stored_table_node = StoredTableNode::make("table_int_float_chunked");

  const auto table = StorageManager::get().get_table("table_int_float_chunked");
  std::vector<ColumnID> index_column_ids = {ColumnID{1}};
  std::vector<ChunkID> index_chunk_ids = {ChunkID{0}, ChunkID{2}};
  table->get_chunk(index_chunk_ids[0])->create_index<GroupKeyIndex>(index_column_ids);
  table->get_chunk(index_chunk_ids[1])->create_index<GroupKeyIndex>(index_column_ids);

  auto predicate_node = PredicateNode::make(LQPColumnReference(stored_table_node, ColumnID{1}),
                                            PredicateCondition::Between, AllParameterVariant(42), AllTypeVariant(1337));
  predicate_node->set_left_input(stored_table_node);
  predicate_node->set_scan_type(ScanType::IndexScan);
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
  EXPECT_EQ(table_scan_op->left_column_id(), ColumnID{1} /* "a" */);
  EXPECT_EQ(table_scan_op->predicate_condition(), PredicateCondition::LessThanEquals);
  EXPECT_EQ(table_scan_op->right_parameter(), AllParameterVariant(1337));

  const auto table_scan_op2 = std::dynamic_pointer_cast<const TableScan>(table_scan_op->input_left());
  ASSERT_TRUE(table_scan_op2);
  EXPECT_EQ(get_excluded_chunk_ids(table_scan_op2), index_chunk_ids);
  EXPECT_EQ(table_scan_op2->left_column_id(), ColumnID{1} /* "a" */);
  EXPECT_EQ(table_scan_op2->predicate_condition(), PredicateCondition::GreaterThanEquals);
  EXPECT_EQ(table_scan_op2->right_parameter(), AllParameterVariant(42));
}

TEST_F(LQPTranslatorTest, PredicateNodeIndexScanFailsWhenNotApplicable) {
  if (!IS_DEBUG) return;
  /**
   * Build LQP and translate to PQP
   */
  const auto stored_table_node = StoredTableNode::make("table_int_float_chunked");

  const auto table = StorageManager::get().get_table("table_int_float_chunked");
  std::vector<ColumnID> index_column_ids = {ColumnID{1}};
  std::vector<ChunkID> index_chunk_ids = {ChunkID{0}, ChunkID{2}};
  table->get_chunk(index_chunk_ids[0])->create_index<GroupKeyIndex>(index_column_ids);
  table->get_chunk(index_chunk_ids[1])->create_index<GroupKeyIndex>(index_column_ids);

  auto predicate_node =
      PredicateNode::make(LQPColumnReference(stored_table_node, ColumnID{1}), PredicateCondition::Equals, 42);
  predicate_node->set_left_input(stored_table_node);
  auto predicate_node2 =
      PredicateNode::make(LQPColumnReference(predicate_node, ColumnID{0}), PredicateCondition::LessThanEquals, 42);
  predicate_node2->set_left_input(predicate_node);

  // The optimizer should not set this ScanType in this situation
  predicate_node2->set_scan_type(ScanType::IndexScan);
  EXPECT_THROW(LQPTranslator{}.translate_node(predicate_node2), std::logic_error);
}

TEST_F(LQPTranslatorTest, ProjectionNode) {
  /**
   * Build LQP and translate to PQP
   */
  const auto stored_table_node = StoredTableNode::make("table_int_float");
  const auto expressions = std::vector<std::shared_ptr<LQPExpression>>{
      LQPExpression::create_column(LQPColumnReference(stored_table_node, ColumnID{0}), {"a"})};
  auto projection_node = ProjectionNode::make(expressions);
  projection_node->set_left_input(stored_table_node);
  const auto op = LQPTranslator{}.translate_node(projection_node);

  /**
   * Check PQP
   */
  const auto projection_op = std::dynamic_pointer_cast<Projection>(op);
  ASSERT_TRUE(projection_op);
  EXPECT_EQ(projection_op->column_expressions().size(), 1u);
  EXPECT_EQ(projection_op->column_expressions()[0]->column_id(), ColumnID{0});
  EXPECT_EQ(*projection_op->column_expressions()[0]->alias(), "a");
}

TEST_F(LQPTranslatorTest, SortNode) {
  /**
   * Build LQP and translate to PQP
   */
  const auto stored_table_node = StoredTableNode::make("table_int_float");
  auto sort_node = SortNode::make(
      std::vector<OrderByDefinition>{{LQPColumnReference(stored_table_node, ColumnID{0}), OrderByMode::Ascending}});
  sort_node->set_left_input(stored_table_node);
  const auto op = LQPTranslator{}.translate_node(sort_node);

  const auto sort_op = std::dynamic_pointer_cast<Sort>(op);
  ASSERT_TRUE(sort_op);
  EXPECT_EQ(sort_op->column_id(), ColumnID{0});
  EXPECT_EQ(sort_op->order_by_mode(), OrderByMode::Ascending);
}

TEST_F(LQPTranslatorTest, JoinNode) {
  /**
   * Build LQP and translate to PQP
   */
  const auto stored_table_node_left = StoredTableNode::make("table_int_float");
  const auto stored_table_node_right = StoredTableNode::make("table_int_float2");
  auto join_node =
      JoinNode::make(JoinMode::Outer, std::make_pair(LQPColumnReference(stored_table_node_left, ColumnID{1}),
                                                     LQPColumnReference(stored_table_node_right, ColumnID{0})),
                     PredicateCondition::Equals);
  join_node->set_left_input(stored_table_node_left);
  join_node->set_right_input(stored_table_node_right);
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

TEST_F(LQPTranslatorTest, AggregateNodeNoArithmetics) {
  /**
   * Build LQP and translate to PQP
   */
  const auto stored_table_node = StoredTableNode::make("table_int_float");

  auto sum_expression = LQPExpression::create_aggregate_function(
      AggregateFunction::Sum, {LQPExpression::create_column(LQPColumnReference(stored_table_node, ColumnID{0}))},
      {"sum_of_a"});

  auto aggregate_node = AggregateNode::make(std::vector<std::shared_ptr<LQPExpression>>{sum_expression},
                                            std::vector<LQPColumnReference>{});
  aggregate_node->set_left_input(stored_table_node);

  const auto op = LQPTranslator{}.translate_node(aggregate_node);

  /**
   * Check PQP
   */
  const auto aggregate_op = std::dynamic_pointer_cast<Aggregate>(op);
  ASSERT_TRUE(aggregate_op);
  ASSERT_EQ(aggregate_op->aggregates().size(), 1u);
  EXPECT_EQ(aggregate_op->groupby_column_ids().size(), 0u);

  const auto aggregate_definition = aggregate_op->aggregates()[0];
  EXPECT_EQ(aggregate_definition.column, ColumnID{0});
  EXPECT_EQ(aggregate_definition.function, AggregateFunction::Sum);
  EXPECT_EQ(aggregate_definition.alias, std::optional<std::string>("sum_of_a"));
}

TEST_F(LQPTranslatorTest, AggregateNodeWithArithmetics) {
  /**
   * Build LQP and translate to PQP
   */
  const auto stored_table_node = StoredTableNode::make("table_int_float");

  // Create expression "b * 2".
  const auto expr_col_b = LQPExpression::create_column(LQPColumnReference{stored_table_node, ColumnID{1}});
  const auto expr_literal = LQPExpression::create_literal(2);
  const auto expr_multiplication =
      LQPExpression::create_binary_operator(ExpressionType::Multiplication, expr_col_b, expr_literal);

  // Create aggregate with expression "SUM(b * 2)".
  // TODO(tim): Projection cannot handle expression `$a + $b`
  // because it is not able to handle columns with different data types.
  // Create issue with failing test.
  auto sum_expression =
      LQPExpression::create_aggregate_function(AggregateFunction::Sum, {expr_multiplication}, {"sum_of_b_times_two"});
  auto aggregate_node =
      AggregateNode::make(std::vector<std::shared_ptr<LQPExpression>>{sum_expression},
                          std::vector<LQPColumnReference>{LQPColumnReference(stored_table_node, ColumnID{0})});
  aggregate_node->set_left_input(stored_table_node);

  const auto op = LQPTranslator{}.translate_node(aggregate_node);

  /**
   * Check PQP
   */
  // Check aggregate operator.
  const auto aggregate_op = std::dynamic_pointer_cast<Aggregate>(op);
  ASSERT_TRUE(aggregate_op);
  ASSERT_EQ(aggregate_op->aggregates().size(), 1u);

  ASSERT_EQ(aggregate_op->groupby_column_ids().size(), 1u);
  EXPECT_EQ(aggregate_op->groupby_column_ids()[0], ColumnID{0});

  const auto aggregate_definition = aggregate_op->aggregates()[0];
  EXPECT_EQ(aggregate_definition.column, ColumnID{1});
  EXPECT_EQ(aggregate_definition.function, AggregateFunction::Sum);
  EXPECT_EQ(aggregate_definition.alias, std::optional<std::string>("sum_of_b_times_two"));

  // Check projection operator.
  // The projection operator is required because we need the arithmetic operation to be calculated first.
  const auto left_op = aggregate_op->input_left();
  ASSERT_TRUE(left_op);

  const auto projection_op = std::dynamic_pointer_cast<const Projection>(left_op);
  ASSERT_TRUE(projection_op);

  const auto column_expressions = projection_op->column_expressions();
  ASSERT_EQ(column_expressions.size(), 2u);

  const auto column_expression0 = column_expressions[0];
  EXPECT_EQ(column_expression0->type(), ExpressionType::Column);
  EXPECT_EQ(column_expression0->column_id(), ColumnID{0});

  const auto column_expression1 = column_expressions[1];
  EXPECT_EQ(column_expression1->to_string(), "ColumnID #1 * 2");
  EXPECT_EQ(column_expression1->alias(), std::nullopt);
}

TEST_F(LQPTranslatorTest, MultipleNodesHierarchy) {
  /**
   * Build LQP and translate to PQP
   */
  const auto stored_table_node_left = StoredTableNode::make("table_int_float");
  auto predicate_node_left = PredicateNode::make(LQPColumnReference(stored_table_node_left, ColumnID{0}),
                                                 PredicateCondition::Equals, AllParameterVariant(42));
  predicate_node_left->set_left_input(stored_table_node_left);

  const auto stored_table_node_right = StoredTableNode::make("table_int_float2");
  auto predicate_node_right = PredicateNode::make(LQPColumnReference(stored_table_node_right, ColumnID{1}),
                                                  PredicateCondition::GreaterThan, AllParameterVariant(30.0));
  predicate_node_right->set_left_input(stored_table_node_right);

  auto join_node =
      JoinNode::make(JoinMode::Inner, LQPColumnReferencePair(LQPColumnReference(stored_table_node_left, ColumnID{0}),
                                                             LQPColumnReference(stored_table_node_right, ColumnID{0})),
                     PredicateCondition::Equals);
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
  EXPECT_EQ(predicate_op_left->predicate_condition(), PredicateCondition::Equals);

  const auto predicate_op_right = std::dynamic_pointer_cast<const TableScan>(join_op->input_right());
  ASSERT_TRUE(predicate_op_right);
  EXPECT_EQ(predicate_op_right->predicate_condition(), PredicateCondition::GreaterThan);

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

  const auto num_rows = 2u;
  auto limit_node = LimitNode::make(num_rows);
  limit_node->set_left_input(stored_table_node);

  /**
   * Check PQP
   */
  const auto op = LQPTranslator{}.translate_node(limit_node);
  const auto limit_op = std::dynamic_pointer_cast<Limit>(op);
  ASSERT_TRUE(limit_op);
  EXPECT_EQ(limit_op->num_rows(), num_rows);
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

  auto table_node = StoredTableNode::make("table_int_float2");
  auto predicate_node_a =
      PredicateNode::make(LQPColumnReference{table_node, ColumnID{0}}, PredicateCondition::Equals, 3);
  auto predicate_node_b =
      PredicateNode::make(LQPColumnReference{table_node, ColumnID{0}}, PredicateCondition::Equals, 4);
  auto predicate_node_c =
      PredicateNode::make(LQPColumnReference{table_node, ColumnID{1}}, PredicateCondition::Equals, 5.0);
  auto union_node = UnionNode::make(UnionMode::Positions);
  const auto& lqp = union_node;

  union_node->set_left_input(predicate_node_a);
  union_node->set_right_input(predicate_node_b);
  predicate_node_a->set_left_input(predicate_node_c);
  predicate_node_b->set_left_input(predicate_node_c);
  predicate_node_c->set_left_input(table_node);

  const auto pqp = LQPTranslator{}.translate_node(lqp);

  ASSERT_NE(pqp, nullptr);
  ASSERT_NE(pqp->input_left(), nullptr);
  ASSERT_NE(pqp->input_right(), nullptr);
  ASSERT_NE(pqp->input_left()->input_left(), nullptr);
  ASSERT_NE(pqp->input_right()->input_left(), nullptr);
  EXPECT_EQ(pqp->input_left()->input_left(), pqp->input_right()->input_left());
  EXPECT_EQ(pqp->input_left()->input_left()->input_left(), pqp->input_right()->input_left()->input_left());
}

}  // namespace opossum
