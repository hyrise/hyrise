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
#include "operators/sort.hpp"
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
  auto transaction_context = Hyrise::get().transaction_manager.new_transaction_context(AutoCommit::No);

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

TEST_F(OperatorsProjectionTest, ForwardsDataTable) {
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

  EXPECT_TRUE(dynamic_cast<const ValueSegment<int>*>(&*output_chunk->get_segment(ColumnID{1})));
  EXPECT_TRUE(dynamic_cast<const ValueSegment<float>*>(&*output_chunk->get_segment(ColumnID{0})));
}

TEST_F(OperatorsProjectionTest, ForwardsDataTableAndExpression) {
  const auto projection =
      std::make_shared<opossum::Projection>(table_wrapper_a, expression_vector(a_b, a_a, add_(a_b, a_a)));
  projection->execute();

  const auto input_chunk = table_wrapper_a->get_output()->get_chunk(ChunkID{0});
  const auto output_chunk = projection->get_output()->get_chunk(ChunkID{0});

  EXPECT_EQ(input_chunk->get_segment(ColumnID{1}), output_chunk->get_segment(ColumnID{0}));
  EXPECT_EQ(input_chunk->get_segment(ColumnID{0}), output_chunk->get_segment(ColumnID{1}));

  EXPECT_TRUE(dynamic_cast<const ValueSegment<int>*>(&*output_chunk->get_segment(ColumnID{1})));
  EXPECT_TRUE(dynamic_cast<const ValueSegment<float>*>(&*output_chunk->get_segment(ColumnID{0})));
}

TEST_F(OperatorsProjectionTest, ForwardReferencesWithExpression) {
  const auto table_scan = create_table_scan(table_wrapper_a, ColumnID{0}, PredicateCondition::LessThan, 100'000);
  table_scan->execute();
  const auto projection =
      std::make_shared<opossum::Projection>(table_scan, expression_vector(a_b, a_a, add_(a_b, a_a)));
  projection->execute();

  const auto input_chunk = table_scan->get_output()->get_chunk(ChunkID{0});
  const auto output_chunk = projection->get_output()->get_chunk(ChunkID{0});

  EXPECT_EQ(input_chunk->get_segment(ColumnID{1}), output_chunk->get_segment(ColumnID{0}));
  EXPECT_EQ(input_chunk->get_segment(ColumnID{0}), output_chunk->get_segment(ColumnID{1}));

  EXPECT_TRUE(dynamic_cast<const ReferenceSegment*>(&*output_chunk->get_segment(ColumnID{1})));
  EXPECT_TRUE(dynamic_cast<const ReferenceSegment*>(&*output_chunk->get_segment(ColumnID{0})));
  EXPECT_TRUE(dynamic_cast<const ReferenceSegment*>(&*output_chunk->get_segment(ColumnID{2})));

  for (const auto& row : projection->get_output()->get_rows()) {
    EXPECT_FLOAT_EQ(boost::get<float>(row[0]) + static_cast<float>(boost::get<int>(row[1])), boost::get<float>(row[2]));
  }
}

TEST_F(OperatorsProjectionTest, ForwardsReferenceTable) {
  const auto table_scan = create_table_scan(table_wrapper_a, ColumnID{0}, PredicateCondition::LessThan, 100'000);
  table_scan->execute();
  const auto projection = std::make_shared<opossum::Projection>(table_scan, expression_vector(a_b, a_a));
  projection->execute();

  EXPECT_EQ(table_scan->get_output()->get_chunk(ChunkID{0})->get_segment(ColumnID{1}),
            projection->get_output()->get_chunk(ChunkID{0})->get_segment(ColumnID{0}));
  EXPECT_EQ(table_scan->get_output()->get_chunk(ChunkID{0})->get_segment(ColumnID{0}),
            projection->get_output()->get_chunk(ChunkID{0})->get_segment(ColumnID{1}));

  const auto output_chunk = projection->get_output()->get_chunk(ChunkID{0});
  EXPECT_TRUE(dynamic_cast<const ReferenceSegment*>(&*output_chunk->get_segment(ColumnID{1})));
  EXPECT_TRUE(dynamic_cast<const ReferenceSegment*>(&*output_chunk->get_segment(ColumnID{0})));
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

TEST_F(OperatorsProjectionTest, ForwardSortedByFlag) {
  // Verify that the sorted_by flag is not set when it's not present in left input.
  const auto projection_a_unsorted = std::make_shared<Projection>(table_wrapper_a, expression_vector(a_a));
  projection_a_unsorted->execute();

  const auto& result_table_unsorted = projection_a_unsorted->get_output();
  for (auto chunk_id = ChunkID{0}; chunk_id < result_table_unsorted->chunk_count(); ++chunk_id) {
    const auto& sorted_by = result_table_unsorted->get_chunk(chunk_id)->individually_sorted_by();
    EXPECT_TRUE(sorted_by.empty());
  }

  // Verify that the sorted_by flag is set when it's present in left input.
  // sorting on column a (ColumnID 0)
  const auto sort =
      std::make_shared<Sort>(table_wrapper_a, std::vector<SortColumnDefinition>{SortColumnDefinition{ColumnID{0}}});
  sort->execute();

  const auto projection_b_a_sorted = std::make_shared<Projection>(sort, expression_vector(a_b, a_a));
  projection_b_a_sorted->execute();

  const auto& result_table_sorted = projection_b_a_sorted->get_output();

  for (auto chunk_id = ChunkID{0}; chunk_id < result_table_sorted->chunk_count(); ++chunk_id) {
    const auto& sorted_by = result_table_sorted->get_chunk(chunk_id)->individually_sorted_by();
    ASSERT_FALSE(sorted_by.empty());
    // Expect sort to be column a, now with ColumnID 1
    const auto expected_sorted_by = std::vector<SortColumnDefinition>{SortColumnDefinition{ColumnID{1}}};
    EXPECT_EQ(sorted_by, expected_sorted_by);
  }
}

}  // namespace opossum
