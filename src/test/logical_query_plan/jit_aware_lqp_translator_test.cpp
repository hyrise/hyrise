#include <gtest/gtest.h>

#include "base_test.hpp"
#include "expression/expression_functional.hpp"
#include "logical_query_plan/jit_aware_lqp_translator.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/projection_node.hpp"
#include "logical_query_plan/sort_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "logical_query_plan/union_node.hpp"
#include "logical_query_plan/validate_node.hpp"
#include "operators/jit_operator/operators/jit_aggregate.hpp"
#include "operators/jit_operator/operators/jit_compute.hpp"
#include "operators/jit_operator/operators/jit_filter.hpp"
#include "operators/jit_operator/operators/jit_limit.hpp"
#include "operators/jit_operator/operators/jit_read_tuples.hpp"
#include "operators/jit_operator/operators/jit_validate.hpp"
#include "operators/jit_operator/operators/jit_write_references.hpp"
#include "operators/jit_operator/operators/jit_write_tuples.hpp"
#include "sql/sql_pipeline_builder.hpp"
#include "storage/chunk_encoder.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

class JitAwareLQPTranslatorTest : public BaseTest {
 protected:
  void SetUp() override {
    const auto int_int_int_table = load_table("resources/test_data/tbl/int_int_int.tbl");
    const auto int_float_null_table = load_table("resources/test_data/tbl/int_float_null_sorted_asc.tbl");

    Hyrise::get().storage_manager.add_table("table_a", int_int_int_table);
    Hyrise::get().storage_manager.add_table("table_b", int_float_null_table);

    stored_table_node_a = std::make_shared<StoredTableNode>("table_a");
    stored_table_node_a2 = std::make_shared<StoredTableNode>("table_a");
    stored_table_node_b = std::make_shared<StoredTableNode>("table_b");

    a_a = stored_table_node_a->get_column("a");
    a_b = stored_table_node_a->get_column("b");
    a_c = stored_table_node_a->get_column("c");
  }

  // Creates an (unoptimized) LQP from a given SQL query string and passes the LQP to the jit-aware translator.
  // This allows for creating different LQPs for testing with little code. The result of the translation
  // (which could be any AbstractOperator) is dynamically cast to a JitOperatorWrapper pointer. Thus, a simple nullptr
  // check can be used to test whether a JitOperatorWrapper has been created by the translator as the root node of the
  // PQP.
  std::shared_ptr<const JitOperatorWrapper> translate_query(const std::string& sql) const {
    const auto lqp = SQLPipelineBuilder(sql).create_pipeline_statement(nullptr).get_unoptimized_logical_plan();
    return translate_lqp(lqp);
  }

  std::shared_ptr<const JitOperatorWrapper> translate_lqp(const std::shared_ptr<AbstractLQPNode>& lqp) const {
    JitAwareLQPTranslator lqp_translator;
    std::shared_ptr<const AbstractOperator> current_node = lqp_translator.translate_node(lqp);
    while (current_node && !std::dynamic_pointer_cast<const JitOperatorWrapper>(current_node)) {
      current_node = current_node->input_left();
    }
    return std::dynamic_pointer_cast<const JitOperatorWrapper>(current_node);
  }

  std::shared_ptr<StoredTableNode> stored_table_node_a, stored_table_node_a2, stored_table_node_b;
  LQPColumnReference a_a, a_b, a_c;
};

TEST_F(JitAwareLQPTranslatorTest, RequiresAtLeastTwoJittableOperators) {
  {
    const auto jit_operator_wrapper = translate_query("SELECT a FROM table_a");
    ASSERT_FALSE(jit_operator_wrapper);
  }
  {
    const auto jit_operator_wrapper = translate_query("SELECT a FROM table_a WHERE a > 1");
    ASSERT_TRUE(jit_operator_wrapper);
  }
}

TEST_F(JitAwareLQPTranslatorTest, JitPipelineRequiresASingleInputNode) {
  {
    // A UnionNode with two distinct input nodes. If the jit-aware translator is not able to determine a single input
    // node to the (intended) operator pipeline, it should not create the pipeline in the first place.
    const auto union_node = std::make_shared<UnionNode>(UnionMode::Positions);

    union_node->set_left_input(stored_table_node_a);
    union_node->set_right_input(stored_table_node_a2);

    JitAwareLQPTranslator lqp_translator;
    const auto jit_operator_wrapper =
        std::dynamic_pointer_cast<JitOperatorWrapper>(lqp_translator.translate_node(union_node));
    ASSERT_FALSE(jit_operator_wrapper);
  }
  {
    // Although both inputs of the UnionNode eventually lead to the same StoredTableNode (i.e., the LQP has a diamond
    // shape), one of the paths contains a non-jittable SortNode. Thus the jit-aware translator should reject the LQP
    // and not create an operator pipeline.
    const auto sort_node =
        std::make_shared<SortNode>(expression_vector(a_a), std::vector<OrderByMode>{OrderByMode::Ascending});
    const auto union_node = std::make_shared<UnionNode>(UnionMode::Positions);

    sort_node->set_left_input(stored_table_node_a);
    union_node->set_left_input(stored_table_node_a);
    union_node->set_right_input(sort_node);

    JitAwareLQPTranslator lqp_translator;
    const auto jit_operator_wrapper =
        std::dynamic_pointer_cast<JitOperatorWrapper>(lqp_translator.translate_node(union_node));
    ASSERT_FALSE(jit_operator_wrapper);
  }
}

TEST_F(JitAwareLQPTranslatorTest, JitOperatorsRejectIndexScan) {
  // The jit operators do not yet support index scans and should thus reject translating them
  const auto predicate_node_1 = std::make_shared<PredicateNode>(greater_than_(a_a, 1));
  const auto predicate_node_2 = std::make_shared<PredicateNode>(less_than_(a_a, 10));

  predicate_node_1->set_left_input(stored_table_node_a);
  predicate_node_2->set_left_input(predicate_node_1);

  JitAwareLQPTranslator lqp_translator;
  {
    const auto jit_operator_wrapper =
        std::dynamic_pointer_cast<JitOperatorWrapper>(lqp_translator.translate_node(predicate_node_2));
    ASSERT_TRUE(jit_operator_wrapper);
  }

  {
    predicate_node_1->scan_type = ScanType::IndexScan;
    const auto jit_operator_wrapper =
        std::dynamic_pointer_cast<JitOperatorWrapper>(lqp_translator.translate_node(predicate_node_1));
    ASSERT_FALSE(jit_operator_wrapper);
  }
}

TEST_F(JitAwareLQPTranslatorTest, InputColumnsAreAddedToJitReadTupleAdapter) {
  // The query reads two columns from the input table. These input columns must be added to the JitReadTuples adapter to
  // make their data accessible by other JitOperators.

  // clang-format off
  const auto b_a = stored_table_node_b->get_column("a");
  const auto b_b = stored_table_node_b->get_column("b");
  const auto lqp = ProjectionNode::make(expression_vector(b_a, b_b),  // SELECT a, b
                     PredicateNode::make(greater_than_(b_b, value_(0.0f)),
                        PredicateNode::make(greater_than_(b_a, value_(1)),
                          ValidateNode::make(stored_table_node_b))));
  // clang-format on

  const auto jit_operator_wrapper = translate_lqp(lqp);
  ASSERT_TRUE(jit_operator_wrapper);
  const auto jit_operators = jit_operator_wrapper->jit_operators();
  ASSERT_EQ(jit_operators.size(), 4u);

  // Check that the first operator is in fact a JitReadTuples instance
  const auto jit_read_tuples = std::dynamic_pointer_cast<JitReadTuples>(jit_operators[0]);
  ASSERT_TRUE(jit_read_tuples);

  // There should be two input columns
  const auto input_columns = jit_read_tuples->input_columns();
  ASSERT_EQ(input_columns.size(), 2u);

  ASSERT_EQ(input_columns[0].column_id, ColumnID{0});
  ASSERT_EQ(input_columns[0].tuple_entry.data_type, DataType::Int);
  ASSERT_FALSE(input_columns[0].tuple_entry.guaranteed_non_null);

  ASSERT_EQ(input_columns[1].column_id, ColumnID{1});
  ASSERT_EQ(input_columns[1].tuple_entry.data_type, DataType::Float);
  ASSERT_FALSE(input_columns[1].tuple_entry.guaranteed_non_null);
}

TEST_F(JitAwareLQPTranslatorTest, LiteralValuesAreAddedToJitReadTupleAdapter) {
  // The query contains two literals. Literals are treated like values read from a column inside the operator pipeline.
  // The JitReadTuples adapter is responsible for making these literals available from within the pipeline.

  // clang-format off
  const auto b_a = stored_table_node_b->get_column("a");
  const auto b_b = stored_table_node_b->get_column("b");
  const auto lqp = ProjectionNode::make(expression_vector(b_a, b_b),  // SELECT a, b
                     PredicateNode::make(greater_than_(b_b, value_(1.2f)),
                        PredicateNode::make(greater_than_(b_a, value_(1)),
                           ValidateNode::make(stored_table_node_b))));
  // clang-format on

  const auto jit_operator_wrapper = translate_lqp(lqp);
  ASSERT_TRUE(jit_operator_wrapper);
  const auto jit_operators = jit_operator_wrapper->jit_operators();
  ASSERT_EQ(jit_operators.size(), 4u);

  // Check that the first operator is in fact a JitReadTuples instance
  const auto jit_read_tuples = std::dynamic_pointer_cast<JitReadTuples>(jit_operators[0]);
  ASSERT_TRUE(jit_read_tuples);

  // There should be two literals read from the query
  const auto input_literals = jit_read_tuples->input_literals();
  ASSERT_EQ(input_literals.size(), 2u);

  ASSERT_EQ(input_literals[0].value, AllTypeVariant(1));
  ASSERT_EQ(input_literals[0].tuple_entry.data_type, DataType::Int);
  ASSERT_TRUE(input_literals[0].tuple_entry.guaranteed_non_null);

  ASSERT_EQ(input_literals[1].value, AllTypeVariant(1.2f));
  ASSERT_EQ(input_literals[1].tuple_entry.data_type, DataType::Float);
  ASSERT_TRUE(input_literals[1].tuple_entry.guaranteed_non_null);
}

TEST_F(JitAwareLQPTranslatorTest, ColumnSubsetIsOutputCorrectly) {
  // Select a subset of columns
  const auto jit_operator_wrapper = translate_query("SELECT a FROM table_a WHERE a > 1");
  ASSERT_TRUE(jit_operator_wrapper);
  const auto jit_operators = jit_operator_wrapper->jit_operators();
  ASSERT_EQ(jit_operators.size(), 4u);

  const auto jit_read_tuples = std::dynamic_pointer_cast<JitReadTuples>(jit_operators[0]);
  ASSERT_TRUE(jit_read_tuples);
  const auto jit_write_references = std::dynamic_pointer_cast<JitWriteReferences>(jit_operators[3]);
  ASSERT_TRUE(jit_write_references);

  const auto output_columns = jit_write_references->output_columns();
  ASSERT_EQ(output_columns.size(), 1u);

  ASSERT_EQ(output_columns[0].referenced_column_id, ColumnID{0});
}

TEST_F(JitAwareLQPTranslatorTest, AllColumnsAreOutputCorrectly) {
  // Select all columns
  const auto jit_operator_wrapper = translate_query("SELECT * FROM table_a WHERE a > 1");
  ASSERT_TRUE(jit_operator_wrapper);
  const auto jit_operators = jit_operator_wrapper->jit_operators();
  ASSERT_EQ(jit_operators.size(), 4u);

  const auto jit_read_tuples = std::dynamic_pointer_cast<JitReadTuples>(jit_operators[0]);
  ASSERT_TRUE(jit_read_tuples);
  const auto jit_write_references = std::dynamic_pointer_cast<JitWriteReferences>(jit_operators[3]);
  ASSERT_TRUE(jit_write_references);

  const auto output_columns = jit_write_references->output_columns();
  ASSERT_EQ(output_columns.size(), 3u);
  ASSERT_EQ(output_columns[0].referenced_column_id, ColumnID{0});
  ASSERT_EQ(output_columns[1].referenced_column_id, ColumnID{1});
  ASSERT_EQ(output_columns[2].referenced_column_id, ColumnID{2});
}

TEST_F(JitAwareLQPTranslatorTest, ReorderedColumnsAreOutputCorrectly) {
  // Select columns in different order
  const auto jit_operator_wrapper = translate_query("SELECT c, a FROM table_a WHERE a > 1");
  ASSERT_TRUE(jit_operator_wrapper);
  const auto jit_operators = jit_operator_wrapper->jit_operators();
  ASSERT_EQ(jit_operators.size(), 4u);

  const auto jit_read_tuples = std::dynamic_pointer_cast<JitReadTuples>(jit_operators[0]);
  ASSERT_TRUE(jit_read_tuples);
  const auto jit_write_references = std::dynamic_pointer_cast<JitWriteReferences>(jit_operators[3]);
  ASSERT_TRUE(jit_write_references);

  const auto output_columns = jit_write_references->output_columns();
  ASSERT_EQ(output_columns.size(), 2u);
  ASSERT_EQ(output_columns[0].referenced_column_id, ColumnID{2});
  ASSERT_EQ(output_columns[1].referenced_column_id, ColumnID{0});
}

TEST_F(JitAwareLQPTranslatorTest, OutputColumnNamesAndAlias) {
  const auto jit_operator_wrapper = translate_query("SELECT a, b as b_new FROM table_a WHERE a > 1");
  ASSERT_TRUE(jit_operator_wrapper);
  const auto jit_operators = jit_operator_wrapper->jit_operators();
  ASSERT_EQ(jit_operators.size(), 4u);

  const auto jit_write_references = std::dynamic_pointer_cast<JitWriteReferences>(jit_operators[3]);
  ASSERT_TRUE(jit_write_references);

  const auto output_columns = jit_write_references->output_columns();
  ASSERT_EQ(output_columns.size(), 2u);
  ASSERT_EQ(output_columns[0].column_name, "a");
  ASSERT_EQ(output_columns[1].column_name, "b");
}

TEST_F(JitAwareLQPTranslatorTest, ConsecutivePredicatesGetTransformedToConjunction) {
  // clang-format off
  const auto lqp = ProjectionNode::make(expression_vector(a_a, a_b, a_c),  // SELECT a, b, c
                     PredicateNode::make(greater_than_(a_c, a_a),
                       PredicateNode::make(greater_than_(a_b, a_c),
                         PredicateNode::make(greater_than_(a_a, a_b),
                           ValidateNode::make(stored_table_node_a)))));
  // clang-format on

  const auto jit_operator_wrapper = translate_lqp(lqp);
  ASSERT_TRUE(jit_operator_wrapper);

  // Check the type of jit operators in the operator pipeline
  const auto jit_operators = jit_operator_wrapper->jit_operators();
  ASSERT_EQ(jit_operator_wrapper->jit_operators().size(), 4u);

  const auto jit_read_tuples = std::dynamic_pointer_cast<JitReadTuples>(jit_operators[0]);
  const auto jit_validate = std::dynamic_pointer_cast<JitValidate>(jit_operators[1]);
  const auto jit_filter = std::dynamic_pointer_cast<JitFilter>(jit_operators[2]);
  const auto jit_write_references = std::dynamic_pointer_cast<JitWriteReferences>(jit_operators[3]);
  ASSERT_TRUE(jit_read_tuples);
  ASSERT_TRUE(jit_validate);
  ASSERT_TRUE(jit_filter);
  ASSERT_TRUE(jit_write_references);

  // Check the structure of the computed expression used for filtering
  const auto expression = jit_filter->expression;
  ASSERT_EQ(expression->expression_type, JitExpressionType::And);
  ASSERT_EQ(expression->right_child->expression_type, JitExpressionType::And);

  const auto b_gt_c = expression->right_child->left_child;
  const auto c_gt_a = expression->right_child->right_child;
  const auto a_gt_b = expression->left_child;

  ASSERT_EQ(a_gt_b->expression_type, JitExpressionType::GreaterThan);
  ASSERT_EQ(a_gt_b->left_child->expression_type, JitExpressionType::Column);
  ASSERT_EQ(a_gt_b->right_child->expression_type, JitExpressionType::Column);
  ASSERT_EQ(*jit_read_tuples->find_input_column(a_gt_b->left_child->result_entry), ColumnID{0});
  ASSERT_EQ(*jit_read_tuples->find_input_column(a_gt_b->right_child->result_entry), ColumnID{1});

  ASSERT_EQ(b_gt_c->expression_type, JitExpressionType::GreaterThan);
  ASSERT_EQ(b_gt_c->left_child->expression_type, JitExpressionType::Column);
  ASSERT_EQ(b_gt_c->right_child->expression_type, JitExpressionType::Column);
  ASSERT_EQ(*jit_read_tuples->find_input_column(b_gt_c->left_child->result_entry), ColumnID{1});
  ASSERT_EQ(*jit_read_tuples->find_input_column(b_gt_c->right_child->result_entry), ColumnID{2});

  ASSERT_EQ(c_gt_a->expression_type, JitExpressionType::GreaterThan);
  ASSERT_EQ(c_gt_a->left_child->expression_type, JitExpressionType::Column);
  ASSERT_EQ(c_gt_a->right_child->expression_type, JitExpressionType::Column);
  ASSERT_EQ(*jit_read_tuples->find_input_column(c_gt_a->left_child->result_entry), ColumnID{2});
  ASSERT_EQ(*jit_read_tuples->find_input_column(c_gt_a->right_child->result_entry), ColumnID{0});
}

TEST_F(JitAwareLQPTranslatorTest, UnionsGetTransformedToDisjunction) {
  // clang-format off
  const auto lqp =
  UnionNode::make(UnionMode::Positions,
    UnionNode::make(UnionMode::Positions,
      PredicateNode::make(greater_than_(a_a, a_b), stored_table_node_a),
      PredicateNode::make(greater_than_(a_b, a_c), stored_table_node_a)),
    PredicateNode::make(greater_than_(a_c, a_a), stored_table_node_a));
  // clang-format on

  const auto jit_operator_wrapper = translate_lqp(lqp);
  ASSERT_TRUE(jit_operator_wrapper);

  // Check the type of jit operators in the operator pipeline
  const auto jit_operators = jit_operator_wrapper->jit_operators();
  ASSERT_EQ(jit_operator_wrapper->jit_operators().size(), 3u);

  const auto jit_read_tuples = std::dynamic_pointer_cast<JitReadTuples>(jit_operators[0]);
  const auto jit_filter = std::dynamic_pointer_cast<JitFilter>(jit_operators[1]);
  const auto jit_write_references = std::dynamic_pointer_cast<JitWriteReferences>(jit_operators[2]);
  ASSERT_TRUE(jit_read_tuples);
  ASSERT_TRUE(jit_filter);
  ASSERT_TRUE(jit_write_references);

  // Check the structure of the computed expression used for filtering
  const auto expression = jit_filter->expression;
  ASSERT_EQ(expression->expression_type, JitExpressionType::Or);
  ASSERT_EQ(expression->left_child->expression_type, JitExpressionType::Or);

  const auto a_gt_b = expression->left_child->left_child;
  const auto b_gt_c = expression->left_child->right_child;
  const auto c_gt_a = expression->right_child;

  ASSERT_EQ(a_gt_b->expression_type, JitExpressionType::GreaterThan);
  ASSERT_EQ(a_gt_b->left_child->expression_type, JitExpressionType::Column);
  ASSERT_EQ(a_gt_b->right_child->expression_type, JitExpressionType::Column);
  ASSERT_EQ(*jit_read_tuples->find_input_column(a_gt_b->left_child->result_entry), ColumnID{0});
  ASSERT_EQ(*jit_read_tuples->find_input_column(a_gt_b->right_child->result_entry), ColumnID{1});

  ASSERT_EQ(b_gt_c->expression_type, JitExpressionType::GreaterThan);
  ASSERT_EQ(b_gt_c->left_child->expression_type, JitExpressionType::Column);
  ASSERT_EQ(b_gt_c->right_child->expression_type, JitExpressionType::Column);
  ASSERT_EQ(*jit_read_tuples->find_input_column(b_gt_c->left_child->result_entry), ColumnID{1});
  ASSERT_EQ(*jit_read_tuples->find_input_column(b_gt_c->right_child->result_entry), ColumnID{2});

  ASSERT_EQ(c_gt_a->expression_type, JitExpressionType::GreaterThan);
  ASSERT_EQ(c_gt_a->left_child->expression_type, JitExpressionType::Column);
  ASSERT_EQ(c_gt_a->right_child->expression_type, JitExpressionType::Column);
  ASSERT_EQ(*jit_read_tuples->find_input_column(c_gt_a->left_child->result_entry), ColumnID{2});
  ASSERT_EQ(*jit_read_tuples->find_input_column(c_gt_a->right_child->result_entry), ColumnID{0});
}

TEST_F(JitAwareLQPTranslatorTest, CheckOperatorOrderValidateAfterFilter) {
  const auto lqp = ValidateNode::make(
      PredicateNode::make(greater_than_(a_a, a_b), PredicateNode::make(greater_than_(a_b, a_c), stored_table_node_a)));

  const auto jit_operator_wrapper = translate_lqp(lqp);
  ASSERT_TRUE(jit_operator_wrapper);

  // Check the type of jit operators in the operator pipeline
  const auto jit_operators = jit_operator_wrapper->jit_operators();
  ASSERT_EQ(jit_operators.size(), 4u);

  const auto jit_read_tuples = std::dynamic_pointer_cast<JitReadTuples>(jit_operators[0]);
  const auto jit_filter = std::dynamic_pointer_cast<JitFilter>(jit_operators[1]);
  const auto jit_validate = std::dynamic_pointer_cast<JitValidate>(jit_operators[2]);
  const auto jit_write_references = std::dynamic_pointer_cast<JitWriteReferences>(jit_operators[3]);
  ASSERT_TRUE(jit_read_tuples);
  ASSERT_TRUE(jit_filter);
  ASSERT_TRUE(jit_validate);
  ASSERT_TRUE(jit_write_references);
}

TEST_F(JitAwareLQPTranslatorTest, CheckOperatorOrderValidateBeforeFilter) {
  const auto lqp = PredicateNode::make(
      greater_than_(a_a, a_b), PredicateNode::make(greater_than_(a_b, a_c), ValidateNode::make(stored_table_node_a)));

  const auto jit_operator_wrapper = translate_lqp(lqp);
  ASSERT_TRUE(jit_operator_wrapper);

  // Check the type of jit operators in the operator pipeline
  const auto jit_operators = jit_operator_wrapper->jit_operators();
  ASSERT_EQ(jit_operators.size(), 4u);

  const auto jit_read_tuples = std::dynamic_pointer_cast<JitReadTuples>(jit_operators[0]);
  const auto jit_validate = std::dynamic_pointer_cast<JitValidate>(jit_operators[1]);
  const auto jit_filter = std::dynamic_pointer_cast<JitFilter>(jit_operators[2]);
  const auto jit_write_references = std::dynamic_pointer_cast<JitWriteReferences>(jit_operators[3]);
  ASSERT_TRUE(jit_read_tuples);
  ASSERT_TRUE(jit_validate);
  ASSERT_TRUE(jit_filter);
  ASSERT_TRUE(jit_write_references);
}

TEST_F(JitAwareLQPTranslatorTest, AMoreComplexQuery) {
  // clang-format off
  const auto lqp = ProjectionNode::make(expression_vector(a_a, mul_(add_(a_a, a_b), a_c)),  // SELECT a, (a + b) * c
                     PredicateNode::make(greater_than_(a_b, add_(a_a, a_c)),
                       PredicateNode::make(less_than_equals_(a_a, a_b),
                         ValidateNode::make(stored_table_node_a))));
  // clang-format on

  const auto jit_operator_wrapper = translate_lqp(lqp);
  ASSERT_TRUE(jit_operator_wrapper);

  // Check the type of jit operators in the operator pipeline
  const auto jit_operators = jit_operator_wrapper->jit_operators();
  ASSERT_EQ(jit_operator_wrapper->jit_operators().size(), 5u);

  const auto jit_read_tuples = std::dynamic_pointer_cast<JitReadTuples>(jit_operators[0]);
  const auto jit_validate = std::dynamic_pointer_cast<JitValidate>(jit_operators[1]);
  const auto jit_filter = std::dynamic_pointer_cast<JitFilter>(jit_operators[2]);
  const auto jit_compute = std::dynamic_pointer_cast<JitCompute>(jit_operators[3]);
  const auto jit_write_tuples = std::dynamic_pointer_cast<JitWriteTuples>(jit_operators[4]);
  ASSERT_TRUE(jit_read_tuples);
  ASSERT_TRUE(jit_validate);
  ASSERT_TRUE(jit_filter);
  ASSERT_TRUE(jit_compute);
  ASSERT_TRUE(jit_write_tuples);

  // Check the structure of the computed filter expression
  const auto expression_1 = jit_filter->expression;
  ASSERT_EQ(expression_1->expression_type, JitExpressionType::And);

  const auto a_lte_b = expression_1->left_child;
  ASSERT_EQ(a_lte_b->expression_type, JitExpressionType::LessThanEquals);
  ASSERT_EQ(a_lte_b->left_child->expression_type, JitExpressionType::Column);
  ASSERT_EQ(a_lte_b->right_child->expression_type, JitExpressionType::Column);
  ASSERT_EQ(*jit_read_tuples->find_input_column(a_lte_b->left_child->result_entry), ColumnID{0});
  ASSERT_EQ(*jit_read_tuples->find_input_column(a_lte_b->right_child->result_entry), ColumnID{1});

  const auto b_gt_a_plus_c = expression_1->right_child;
  ASSERT_EQ(b_gt_a_plus_c->expression_type, JitExpressionType::GreaterThan);
  ASSERT_EQ(b_gt_a_plus_c->left_child->expression_type, JitExpressionType::Column);
  ASSERT_EQ(*jit_read_tuples->find_input_column(b_gt_a_plus_c->left_child->result_entry), ColumnID{1});

  const auto a_plus_c = b_gt_a_plus_c->right_child;
  ASSERT_EQ(a_plus_c->expression_type, JitExpressionType::Addition);
  ASSERT_EQ(a_plus_c->left_child->expression_type, JitExpressionType::Column);
  ASSERT_EQ(a_plus_c->right_child->expression_type, JitExpressionType::Column);
  ASSERT_EQ(*jit_read_tuples->find_input_column(a_plus_c->left_child->result_entry), ColumnID{0});
  ASSERT_EQ(*jit_read_tuples->find_input_column(a_plus_c->right_child->result_entry), ColumnID{2});

  // Check the structure of the computed expression
  const auto expression_2 = jit_compute->expression;
  ASSERT_EQ(expression_2->expression_type, JitExpressionType::Multiplication);
  ASSERT_EQ(expression_2->right_child->expression_type, JitExpressionType::Column);
  ASSERT_EQ(*jit_read_tuples->find_input_column(expression_2->right_child->result_entry), ColumnID{2});

  const auto a_plus_b = expression_2->left_child;
  ASSERT_EQ(a_plus_b->expression_type, JitExpressionType::Addition);
  ASSERT_EQ(a_plus_b->left_child->expression_type, JitExpressionType::Column);
  ASSERT_EQ(a_plus_b->right_child->expression_type, JitExpressionType::Column);
  ASSERT_EQ(*jit_read_tuples->find_input_column(a_plus_b->left_child->result_entry), ColumnID{0});
  ASSERT_EQ(*jit_read_tuples->find_input_column(a_plus_b->right_child->result_entry), ColumnID{1});

  const auto output_columns = jit_write_tuples->output_columns();
  ASSERT_EQ(output_columns.size(), 2u);
  ASSERT_EQ(*jit_read_tuples->find_input_column(output_columns[0].tuple_entry), ColumnID{0});
  ASSERT_EQ(expression_2->result_entry, std::make_optional(output_columns[1].tuple_entry));
}

TEST_F(JitAwareLQPTranslatorTest, AggregateOperator) {
  const auto jit_operator_wrapper =
      translate_query("SELECT COUNT(a), SUM(b), AVG(a + b), MIN(a), MAX(b), COUNT(*) FROM table_a GROUP BY a");
  ASSERT_TRUE(jit_operator_wrapper);

  // Check the type of jit operators in the operator pipeline
  const auto jit_operators = jit_operator_wrapper->jit_operators();
  ASSERT_EQ(jit_operator_wrapper->jit_operators().size(), 4u);

  const auto jit_read_tuples = std::dynamic_pointer_cast<JitReadTuples>(jit_operators[0]);
  const auto jit_validate = std::dynamic_pointer_cast<JitValidate>(jit_operators[1]);
  const auto jit_compute = std::dynamic_pointer_cast<JitCompute>(jit_operators[2]);
  const auto jit_aggregate = std::dynamic_pointer_cast<JitAggregate>(jit_operators[3]);
  ASSERT_TRUE(jit_read_tuples);
  ASSERT_TRUE(jit_validate);
  ASSERT_TRUE(jit_compute);
  ASSERT_TRUE(jit_aggregate);

  // Check the structure of the computed expression
  const auto expression = jit_compute->expression;
  ASSERT_EQ(expression->expression_type, JitExpressionType::Addition);
  ASSERT_EQ(*jit_read_tuples->find_input_column(expression->left_child->result_entry), ColumnID{0});
  ASSERT_EQ(*jit_read_tuples->find_input_column(expression->right_child->result_entry), ColumnID{1});

  // Check that the aggregate operator is configured with the correct group-by and aggregate columns
  const auto groupby_columns = jit_aggregate->groupby_columns();
  ASSERT_EQ(groupby_columns.size(), 1u);
  ASSERT_EQ(groupby_columns[0].column_name, "a");
  ASSERT_EQ(*jit_read_tuples->find_input_column(groupby_columns[0].tuple_entry), ColumnID{0});

  const auto aggregate_columns = jit_aggregate->aggregate_columns();
  ASSERT_EQ(aggregate_columns.size(), 6u);

  ASSERT_EQ(aggregate_columns[0].column_name, "COUNT(a)");
  ASSERT_EQ(aggregate_columns[0].function, AggregateFunction::Count);
  ASSERT_EQ(*jit_read_tuples->find_input_column(aggregate_columns[0].tuple_entry), ColumnID{0});

  ASSERT_EQ(aggregate_columns[1].column_name, "SUM(b)");
  ASSERT_EQ(aggregate_columns[1].function, AggregateFunction::Sum);
  ASSERT_EQ(*jit_read_tuples->find_input_column(aggregate_columns[1].tuple_entry), ColumnID{1});

  ASSERT_EQ(aggregate_columns[2].column_name, "AVG(a + b)");
  ASSERT_EQ(aggregate_columns[2].function, AggregateFunction::Avg);
  // This aggregate function should operates on the result of the previously computed expression
  ASSERT_EQ(aggregate_columns[2].tuple_entry, expression->result_entry);

  ASSERT_EQ(aggregate_columns[3].column_name, "MIN(a)");
  ASSERT_EQ(aggregate_columns[3].function, AggregateFunction::Min);
  ASSERT_EQ(*jit_read_tuples->find_input_column(aggregate_columns[3].tuple_entry), ColumnID{0});

  ASSERT_EQ(aggregate_columns[4].column_name, "MAX(b)");
  ASSERT_EQ(aggregate_columns[4].function, AggregateFunction::Max);
  ASSERT_EQ(*jit_read_tuples->find_input_column(aggregate_columns[4].tuple_entry), ColumnID{1});

  ASSERT_EQ(aggregate_columns[5].column_name, "COUNT(*)");
  ASSERT_EQ(aggregate_columns[5].function, AggregateFunction::Count);
  ASSERT_EQ(aggregate_columns[5].tuple_entry.tuple_index, 3u);
}

TEST_F(JitAwareLQPTranslatorTest, LimitOperator) {
  const auto node_table_a = StoredTableNode::make("table_a");
  const auto node_table_a_col_a = node_table_a->get_column("a");

  const auto value = std::make_shared<ValueExpression>(int32_t{123});
  const auto table_scan = PredicateNode::make(equals_(node_table_a_col_a, value), node_table_a);
  const auto limit = LimitNode::make(value, table_scan);

  const auto jit_operator_wrapper = translate_lqp(limit);
  ASSERT_TRUE(jit_operator_wrapper);

  // Check the type of jit operators in the operator pipeline
  const auto jit_operators = jit_operator_wrapper->jit_operators();
  ASSERT_EQ(jit_operator_wrapper->jit_operators().size(), 4u);

  const auto jit_read_tuples = std::dynamic_pointer_cast<JitReadTuples>(jit_operators[0]);
  const auto jit_filter = std::dynamic_pointer_cast<JitFilter>(jit_operators[1]);
  const auto jit_limit = std::dynamic_pointer_cast<JitLimit>(jit_operators[2]);
  const auto jit_write_references = std::dynamic_pointer_cast<JitWriteReferences>(jit_operators[3]);
  ASSERT_TRUE(jit_read_tuples);
  ASSERT_TRUE(jit_filter);
  ASSERT_TRUE(jit_limit);
  ASSERT_TRUE(jit_write_references);

  ASSERT_EQ(jit_read_tuples->row_count_expression, value);
}

TEST_F(JitAwareLQPTranslatorTest, CreatesValueIDExpressions) {
  // Check that the translator adds potential value id expressions to the JitReadTuples operator

  // clang-format off
  const auto lqp = PredicateNode::make(less_than_(a_b, correlated_parameter_(ParameterID{4}, a_c)),
                     PredicateNode::make(equals_(a_c, 2),
                       PredicateNode::make(between_inclusive_(a_b, 1, 2),
                         PredicateNode::make(is_null_(a_a), stored_table_node_a))));
  // clang-format on

  const auto jit_operator_wrapper = translate_lqp(lqp);
  ASSERT_TRUE(jit_operator_wrapper);

  const auto jit_operators = jit_operator_wrapper->jit_operators();

  const auto jit_read_tuples = std::dynamic_pointer_cast<JitReadTuples>(jit_operators[0]);
  ASSERT_TRUE(jit_read_tuples);
  const auto jit_filter = std::dynamic_pointer_cast<JitFilter>(jit_operators[1]);
  ASSERT_TRUE(jit_filter);

  const auto value_id_expressions = jit_read_tuples->value_id_expressions();
  ASSERT_EQ(value_id_expressions.size(), 5);

  const auto and_expression_1 = jit_filter->expression;
  // a is null
  ASSERT_EQ(value_id_expressions[0].jit_expression, and_expression_1->left_child);
  ASSERT_EQ(value_id_expressions[0].input_column_index, 0);
  ASSERT_EQ(value_id_expressions[0].input_literal_index, std::nullopt);
  ASSERT_EQ(value_id_expressions[0].input_parameter_index, std::nullopt);

  const auto and_expression_2 = and_expression_1->right_child;
  const auto and_expression_3 = and_expression_2->left_child;
  // b BETWEEN 1 AND 2 -> b >= 1
  ASSERT_EQ(value_id_expressions[1].jit_expression, and_expression_3->left_child);
  ASSERT_EQ(value_id_expressions[1].input_column_index, 1);
  ASSERT_EQ(value_id_expressions[1].input_literal_index, 0);
  ASSERT_EQ(value_id_expressions[1].input_parameter_index, std::nullopt);

  // b BETWEEN 1 AND 2 -> b <= 2
  ASSERT_EQ(value_id_expressions[2].jit_expression, and_expression_3->right_child);
  ASSERT_EQ(value_id_expressions[2].input_column_index, 1);
  ASSERT_EQ(value_id_expressions[2].input_literal_index, 1);
  ASSERT_EQ(value_id_expressions[2].input_parameter_index, std::nullopt);

  const auto and_expression_4 = and_expression_2->right_child;
  // c = 3
  ASSERT_EQ(value_id_expressions[3].jit_expression, and_expression_4->left_child);
  ASSERT_EQ(value_id_expressions[3].input_column_index, 2);
  ASSERT_EQ(value_id_expressions[3].input_literal_index, 2);
  ASSERT_EQ(value_id_expressions[3].input_parameter_index, std::nullopt);

  // b < par#4
  ASSERT_EQ(value_id_expressions[4].jit_expression, and_expression_4->right_child);
  ASSERT_EQ(value_id_expressions[4].input_column_index, 1);
  ASSERT_EQ(value_id_expressions[4].input_literal_index, std::nullopt);
  ASSERT_EQ(value_id_expressions[4].input_parameter_index, 0);

  const auto& input_columns = jit_read_tuples->input_columns();
  // Column a, b and c load value ids
  ASSERT_FALSE(input_columns[0].use_actual_value);
  ASSERT_FALSE(input_columns[1].use_actual_value);
  ASSERT_FALSE(input_columns[2].use_actual_value);
}

TEST_F(JitAwareLQPTranslatorTest, IgnoresValueIDExpressions) {
  // Check that value id expressions are not added if a comparison via value ids is not possible

  const auto get_jit_read_tuples =
      [&](const std::shared_ptr<AbstractLQPNode>& lqp) -> const std::shared_ptr<const JitReadTuples> {
    const auto jit_operator_wrapper = translate_lqp(lqp);
    EXPECT_TRUE(jit_operator_wrapper);

    const auto jit_operators = jit_operator_wrapper->jit_operators();

    const auto jit_read_tuples = std::dynamic_pointer_cast<JitReadTuples>(jit_operators[0]);
    EXPECT_TRUE(jit_read_tuples);
    return jit_read_tuples;
  };

  {
    // Predicate condition does not allow a comparison on value ids for (not) like and (not) in
    const auto string_table = load_table("resources/test_data/tbl/string.tbl");
    ChunkEncoder::encode_all_chunks(string_table);
    Hyrise::get().storage_manager.add_table("string_table", string_table);
    const auto stored_table = StoredTableNode::make("string_table");
    const auto col_a = stored_table->get_column("a");

    // clang-format off
    const auto lqp = PredicateNode::make(not_in_(col_a, list_("a")),
                       PredicateNode::make(in_(col_a, list_("a")),
                         PredicateNode::make(not_like_(col_a, "a"),
                           PredicateNode::make(like_(col_a, "a"), stored_table))));
    // clang-format on

    const auto jit_read_tuples = get_jit_read_tuples(lqp);

    ASSERT_TRUE(jit_read_tuples->value_id_expressions().empty());
    // Column a loads actual values
    ASSERT_TRUE(jit_read_tuples->input_columns()[0].use_actual_value);
  }

  {
    // Predicate condition does not allow a comparison on value ids for computed values
    const auto lqp = PredicateNode::make(equals_(a_b, add_(a_c, 1)), stored_table_node_a);

    const auto jit_read_tuples = get_jit_read_tuples(lqp);

    ASSERT_TRUE(jit_read_tuples->value_id_expressions().empty());

    const auto& input_columns = jit_read_tuples->input_columns();
    // Column b and c load actual values
    ASSERT_TRUE(input_columns[0].use_actual_value);
    ASSERT_TRUE(input_columns[1].use_actual_value);
  }

  {
    // Predicate condition does not allow a comparison on value ids for two columns
    const auto lqp = PredicateNode::make(equals_(a_b, a_c), stored_table_node_a);

    const auto jit_read_tuples = get_jit_read_tuples(lqp);

    ASSERT_TRUE(jit_read_tuples->value_id_expressions().empty());

    const auto& input_columns = jit_read_tuples->input_columns();
    // Column b and c load actual values
    ASSERT_TRUE(input_columns[0].use_actual_value);
    ASSERT_TRUE(input_columns[1].use_actual_value);
  }
}

TEST_F(JitAwareLQPTranslatorTest, SwapOperandsForValueIDExpressions) {
  // Check that the column is the left operand in value id expressions

  // clang-format off
  const auto lqp = PredicateNode::make(equals_(a_a, 1),
                     PredicateNode::make(not_equals_(2, a_b),
                       PredicateNode::make(greater_than_(add_(a_a, 3), a_c), stored_table_node_a)));
  // clang-format on

  const auto jit_operator_wrapper = translate_lqp(lqp);
  ASSERT_TRUE(jit_operator_wrapper);

  const auto jit_operators = jit_operator_wrapper->jit_operators();

  const auto jit_filter = std::dynamic_pointer_cast<JitFilter>(jit_operators[1]);
  ASSERT_TRUE(jit_filter);

  const auto jit_expressions = jit_filter->expression;

  // a + 2 < c -> no swap
  const auto predicate_a = jit_expressions->left_child;
  ASSERT_EQ(predicate_a->expression_type, JitExpressionType::GreaterThan);
  ASSERT_EQ(predicate_a->left_child->expression_type, JitExpressionType::Addition);
  ASSERT_EQ(predicate_a->right_child->expression_type, JitExpressionType::Column);

  // 2 != b -> swap
  const auto predicate_b = jit_expressions->right_child->left_child;
  ASSERT_EQ(predicate_b->expression_type, JitExpressionType::NotEquals);
  ASSERT_EQ(predicate_b->left_child->expression_type, JitExpressionType::Column);
  ASSERT_EQ(predicate_b->right_child->expression_type, JitExpressionType::Value);

  // a = 2 -> no swap
  const auto predicate_c = jit_expressions->right_child->right_child;
  ASSERT_EQ(predicate_c->expression_type, JitExpressionType::Equals);
  ASSERT_EQ(predicate_c->left_child->expression_type, JitExpressionType::Column);
  ASSERT_EQ(predicate_c->right_child->expression_type, JitExpressionType::Value);
}

}  // namespace opossum
