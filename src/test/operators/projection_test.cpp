#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "base_test.hpp"

#include "expression/expression_functional.hpp"
#include "expression/pqp_column_expression.hpp"
#include "operators/abstract_read_only_operator.hpp"
#include "operators/delete.hpp"
#include "operators/print.hpp"
#include "operators/projection.hpp"
#include "operators/table_scan.hpp"
#include "operators/table_wrapper.hpp"
#include "storage/chunk_encoder.hpp"
#include "storage/table.hpp"
#include "types.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

/**
 * Projection mostly forwards its computations to the ExpressionEvaluator, so this the actual expression evaluation is
 * not tested here, but in the expression_evaluator_test.cpp
 */
class OperatorsProjectionTest : public BaseTest {
 public:
  void SetUp() override {
    table_wrapper_a = std::make_shared<TableWrapper>(load_table("resources/test_data/tbl/int_float.tbl", 2));
    table_wrapper_a->execute();
    table_wrapper_b = std::make_shared<TableWrapper>(load_table("resources/test_data/tbl/int_float.tbl", 2));
    table_wrapper_b->execute();

    a_a = PQPColumnExpression::from_table(*table_wrapper_a->get_output(), "a");
    a_b = PQPColumnExpression::from_table(*table_wrapper_a->get_output(), "b");
  }

  std::shared_ptr<TableWrapper> table_wrapper_a, table_wrapper_b;
  std::shared_ptr<PQPColumnExpression> a_a, a_b, b_a, b_b;
};

TEST_F(OperatorsProjectionTest, OperatorName) {
  const auto projection = std::make_shared<opossum::Projection>(table_wrapper_a, expression_vector(a_a, a_b));
  EXPECT_EQ(projection->name(), "Projection");
}

TEST_F(OperatorsProjectionTest, ExecutedOnAllChunks) {
  const auto projection = std::make_shared<opossum::Projection>(table_wrapper_a, expression_vector(add_(a_a, a_b)));
  projection->execute();
  EXPECT_TABLE_EQ_UNORDERED(projection->get_output(),
                            load_table("resources/test_data/tbl/projection/int_float_add.tbl"));
}

TEST_F(OperatorsProjectionTest, PassThroughInvalidRowCount) {
  auto transaction_context = Hyrise::get().transaction_manager.new_transaction_context();

  auto table_scan = create_table_scan(table_wrapper_a, ColumnID{0}, PredicateCondition::GreaterThan, 123);
  table_scan->execute();

  const auto rows_to_delete = table_scan->get_output()->row_count();

  auto delete_op = std::make_shared<Delete>(table_scan);
  delete_op->set_transaction_context(transaction_context);
  delete_op->execute();

  transaction_context->commit();

  const auto projection = std::make_shared<opossum::Projection>(table_wrapper_a, expression_vector(a_a, a_b));

  projection->execute();
  const auto result_table = projection->get_output();

  auto total_invalid_row_count = 0;
  for (auto chunk_id = ChunkID{0}; chunk_id < result_table->chunk_count(); ++chunk_id) {
    total_invalid_row_count += result_table->get_chunk(chunk_id)->invalid_row_count();
  }

  EXPECT_EQ(total_invalid_row_count, rows_to_delete);
}

TEST_F(OperatorsProjectionTest, ForwardsIfPossibleDataTable) {
  // The Projection will forward segments from its input if all expressions are segment references.
  // Why would you enforce something like this? E.g., Update relies on it.

  const auto projection = std::make_shared<opossum::Projection>(table_wrapper_a, expression_vector(a_b, a_a));
  projection->execute();

  const auto input_chunk = table_wrapper_a->get_output()->get_chunk(ChunkID{0});
  const auto output_chunk = projection->get_output()->get_chunk(ChunkID{0});

  EXPECT_EQ(input_chunk->get_segment(ColumnID{1}), output_chunk->get_segment(ColumnID{0}));
  EXPECT_EQ(input_chunk->get_segment(ColumnID{0}), output_chunk->get_segment(ColumnID{1}));
  EXPECT_TRUE(projection->get_output()->uses_mvcc() == UseMvcc::Yes);
  EXPECT_TRUE(projection->get_output()->get_chunk(ChunkID{0})->mvcc_data());
}

TEST_F(OperatorsProjectionTest, ForwardsIfPossibleDataTableAndExpression) {
  const auto projection =
      std::make_shared<opossum::Projection>(table_wrapper_a, expression_vector(a_b, a_a, add_(a_b, a_a)));
  projection->execute();

  const auto input_chunk = table_wrapper_a->get_output()->get_chunk(ChunkID{0});
  const auto output_chunk = projection->get_output()->get_chunk(ChunkID{0});

  EXPECT_EQ(input_chunk->get_segment(ColumnID{1}), output_chunk->get_segment(ColumnID{0}));
  EXPECT_EQ(input_chunk->get_segment(ColumnID{0}), output_chunk->get_segment(ColumnID{1}));
}

TEST_F(OperatorsProjectionTest, DontForwardReferencesWithExpression) {
  const auto table_scan = create_table_scan(table_wrapper_a, ColumnID{0}, PredicateCondition::LessThan, 100'000);
  table_scan->execute();
  const auto projection =
      std::make_shared<opossum::Projection>(table_scan, expression_vector(a_b, a_a, add_(a_b, a_a)));
  projection->execute();

  const auto input_chunk = table_wrapper_a->get_output()->get_chunk(ChunkID{0});
  const auto output_chunk = projection->get_output()->get_chunk(ChunkID{0});

  EXPECT_NE(input_chunk->get_segment(ColumnID{1}), output_chunk->get_segment(ColumnID{0}));
  EXPECT_NE(input_chunk->get_segment(ColumnID{0}), output_chunk->get_segment(ColumnID{1}));
}

TEST_F(OperatorsProjectionTest, ForwardsIfPossibleReferenceTable) {
  // See ForwardsIfPossibleDataTable

  const auto table_scan = create_table_scan(table_wrapper_a, ColumnID{0}, PredicateCondition::LessThan, 100'000);
  table_scan->execute();
  const auto projection = std::make_shared<opossum::Projection>(table_scan, expression_vector(a_b, a_a));
  projection->execute();

  EXPECT_EQ(table_scan->get_output()->get_chunk(ChunkID{0})->get_segment(ColumnID{1}),
            projection->get_output()->get_chunk(ChunkID{0})->get_segment(ColumnID{0}));
  EXPECT_EQ(table_scan->get_output()->get_chunk(ChunkID{0})->get_segment(ColumnID{0}),
            projection->get_output()->get_chunk(ChunkID{0})->get_segment(ColumnID{1}));
}

TEST_F(OperatorsProjectionTest, SetParameters) {
  const auto table_scan_a = create_table_scan(table_wrapper_b, ColumnID{1}, PredicateCondition::GreaterThan, 5);
  const auto projection_a = std::make_shared<Projection>(table_scan_a, expression_vector(b_a));
  const auto subquery_expression =
      std::make_shared<PQPSubqueryExpression>(table_scan_a, DataType::Int, false, PQPSubqueryExpression::Parameters{});
  const auto projection_b = std::make_shared<Projection>(
      table_wrapper_a, expression_vector(correlated_parameter_(ParameterID{2}, a_a), subquery_expression));

  const auto parameters = std::unordered_map<ParameterID, AllTypeVariant>{{ParameterID{5}, AllTypeVariant{12}},
                                                                          {ParameterID{2}, AllTypeVariant{13}}};
  projection_b->set_parameters(parameters);

  const auto correlated_parameter_expression =
      std::dynamic_pointer_cast<CorrelatedParameterExpression>(projection_b->expressions.at(0));
  ASSERT_TRUE(correlated_parameter_expression);
  EXPECT_TRUE(correlated_parameter_expression->value());
  EXPECT_EQ(*correlated_parameter_expression->value(), AllTypeVariant{13});
}

TEST_F(OperatorsProjectionTest, ReusesDictionaryWhenForwarding) {
  // Checks that instead of materializing all values in an imperfectly forwarded DictionarySegment, the dictionary
  // is re-used and only the attribute vector is materialized.

  auto dict_table = load_table("resources/test_data/tbl/int_float4.tbl", 3);
  ChunkEncoder::encode_all_chunks(dict_table, EncodingType::Dictionary);
  auto table_wrapper = std::make_shared<TableWrapper>(dict_table);
  table_wrapper->execute();

  const auto table_scan = create_table_scan(table_wrapper, ColumnID{0}, PredicateCondition::Equals, 123456);
  table_scan->execute();
  const auto projection =
      std::make_shared<opossum::Projection>(table_scan, expression_vector(a_b, a_a, add_(a_b, a_a)));
  projection->execute();

  const auto input_segment = dict_table->get_chunk(ChunkID{0})->get_segment(ColumnID{0});
  const auto input_dictionary_segment = std::dynamic_pointer_cast<DictionarySegment<int>>(input_segment);
  ASSERT_TRUE(input_dictionary_segment);

  const auto& output_table = projection->get_output();
  ASSERT_EQ(output_table->chunk_count(), 3);

  // Note that column a now has column id 1
  const auto output_segment = output_table->get_chunk(ChunkID{0})->get_segment(ColumnID{1});
  const auto output_dictionary_segment = std::dynamic_pointer_cast<DictionarySegment<int>>(output_segment);
  ASSERT_TRUE(output_dictionary_segment);

  EXPECT_TRUE(output_dictionary_segment->dictionary() == input_dictionary_segment->dictionary());

  const auto& output_attribute_vector = output_dictionary_segment->attribute_vector();
  EXPECT_EQ(output_attribute_vector->size(), 1);

  const auto output_attribute_vector_decompressor = output_attribute_vector->create_base_decompressor();
  EXPECT_EQ(output_attribute_vector_decompressor->get(0), ValueID{1});
}

}  // namespace opossum
