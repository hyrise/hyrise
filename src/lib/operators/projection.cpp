#include "projection.hpp"

#include <algorithm>
#include <functional>
#include <memory>
#include <numeric>
#include <string>
#include <utility>
#include <vector>

#include "expression/evaluation/expression_evaluator.hpp"
#include "expression/expression_utils.hpp"
#include "expression/pqp_column_expression.hpp"
#include "expression/value_expression.hpp"
#include "storage/resolve_encoded_segment_type.hpp"
#include "storage/segment_iterables/create_iterable_from_attribute_vector.hpp"
#include "storage/segment_iterate.hpp"
#include "storage/vector_compression/vector_compression.hpp"
#include "utils/assert.hpp"
#include "utils/timer.hpp"

namespace opossum {

Projection::Projection(const std::shared_ptr<const AbstractOperator>& input_operator,
                       const std::vector<std::shared_ptr<AbstractExpression>>& init_expressions)
    : AbstractReadOnlyOperator(OperatorType::Projection, input_operator, nullptr,
                               std::make_unique<OperatorPerformanceData<OperatorSteps>>()),
      expressions(init_expressions) {}

const std::string& Projection::name() const {
  static const auto name = std::string{"Projection"};
  return name;
}

std::shared_ptr<AbstractOperator> Projection::_on_deep_copy(
    const std::shared_ptr<AbstractOperator>& copied_left_input,
    const std::shared_ptr<AbstractOperator>& copied_right_input) const {
  return std::make_shared<Projection>(copied_left_input, expressions_deep_copy(expressions));
}

void Projection::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {
  expressions_set_parameters(expressions, parameters);
}

void Projection::_on_set_transaction_context(const std::weak_ptr<TransactionContext>& transaction_context) {
  expressions_set_transaction_context(expressions, transaction_context);
}

std::shared_ptr<const Table> Projection::_on_execute() {
  Timer timer;

  const auto& input_table = *left_input_table();

  /**
   * For PQPColumnExpressions, it is possible to forward the input column if the input TableType (References or Data)
   * matches the output column type (ReferenceSegment or not).
   */
  const auto only_projects_columns = std::all_of(expressions.begin(), expressions.end(), [&](const auto& expression) {
    return expression->type == ExpressionType::PQPColumn;
  });

  const auto output_table_type = only_projects_columns ? input_table.type() : TableType::Data;
  const auto forward_columns = input_table.type() == output_table_type;

  const auto uncorrelated_subquery_results =
      ExpressionEvaluator::populate_uncorrelated_subquery_results_cache(expressions);

  auto& step_performance_data = dynamic_cast<OperatorPerformanceData<OperatorSteps>&>(*performance_data);
  if (!uncorrelated_subquery_results->empty()) {
    step_performance_data.set_step_runtime(OperatorSteps::UncorrelatedSubqueries, timer.lap());
  }

  auto column_is_nullable = std::vector<bool>(expressions.size(), false);

  /**
   * Perform the projection
   */
  auto output_chunk_segments = std::vector<Segments>(input_table.chunk_count());

  auto forwarding_cost = std::chrono::nanoseconds{};
  auto expression_evaluator_cost = std::chrono::nanoseconds{};

  const auto chunk_count_input_table = input_table.chunk_count();
  for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count_input_table; ++chunk_id) {
    const auto input_chunk = input_table.get_chunk(chunk_id);
    Assert(input_chunk, "Physically deleted chunk should not reach this point, see get_chunk / #1686.");

    auto output_segments = Segments{expressions.size()};

    ExpressionEvaluator evaluator(left_input_table(), chunk_id, uncorrelated_subquery_results);

    for (auto column_id = ColumnID{0}; column_id < expressions.size(); ++column_id) {
      const auto& expression = expressions[column_id];

      // Forward input column if possible
      if (expression->type == ExpressionType::PQPColumn && forward_columns) {
        const auto pqp_column_expression = std::static_pointer_cast<PQPColumnExpression>(expression);
        output_segments[column_id] = input_chunk->get_segment(pqp_column_expression->column_id);
        column_is_nullable[column_id] =
            column_is_nullable[column_id] || input_table.column_is_nullable(pqp_column_expression->column_id);
        forwarding_cost += timer.lap();
      } else if (expression->type == ExpressionType::PQPColumn && !forward_columns) {
        // The current column will be returned without any logical modifications. As other columns do get modified (and
        // returned as a ValueSegment), all segments (including this one) need to become ValueSegments. This segment is
        // not yet a ValueSegment (otherwise forward_columns would be true); thus we need to materialize it.

        // TODO(jk): Once we have a smart pos list that knows that a single chunk is referenced in its entirety, we can
        //           simply forward that chunk here instead of materializing it.

        const auto pqp_column_expression = std::static_pointer_cast<PQPColumnExpression>(expression);
        const auto segment = input_chunk->get_segment(pqp_column_expression->column_id);

        resolve_data_type(expression->data_type(), [&](const auto data_type) {
          using ColumnDataType = typename decltype(data_type)::type;

          const auto reference_segment = std::dynamic_pointer_cast<ReferenceSegment>(segment);
          DebugAssert(reference_segment, "Expected ReferenceSegment");

          // If the ReferenceSegment references a single (FixedString)DictionarySegment, do not materialize it as a
          // ValueSegment, but re-use its dictionary and only copy the value ids.
          auto referenced_dictionary_segment = std::shared_ptr<BaseDictionarySegment>{};

          const auto& pos_list = reference_segment->pos_list();
          if (pos_list->references_single_chunk()) {
            const auto& referenced_table = reference_segment->referenced_table();
            const auto& referenced_chunk = referenced_table->get_chunk(pos_list->common_chunk_id());
            const auto& referenced_segment = referenced_chunk->get_segment(reference_segment->referenced_column_id());
            referenced_dictionary_segment = std::dynamic_pointer_cast<BaseDictionarySegment>(referenced_segment);
          }

          if (referenced_dictionary_segment) {
            // Resolving the BaseDictionarySegment so that we can handle both regular and fixed-string dictionaries
            resolve_encoded_segment_type<ColumnDataType>(
                *referenced_dictionary_segment, [&](const auto& typed_segment) {
                  using DictionarySegmentType = std::decay_t<decltype(typed_segment)>;

                  // Write new a attribute vector containing only positions given from the input_pos_list.
                  [[maybe_unused]] auto materialize_filtered_attribute_vector = [](const auto& dictionary_segment,
                                                                                   const auto& input_pos_list) {
                    auto filtered_attribute_vector = pmr_vector<ValueID::base_type>(input_pos_list->size());
                    auto iterable = create_iterable_from_attribute_vector(dictionary_segment);
                    auto chunk_offset = ChunkOffset{0};
                    iterable.with_iterators(input_pos_list, [&](auto it, auto end) {
                      while (it != end) {
                        filtered_attribute_vector[chunk_offset] = it->value();
                        ++it;
                        ++chunk_offset;
                      }
                    });
                    // DictionarySegments take BaseCompressedVectors, not an std::vector<ValueId> for the attribute
                    // vector. But the latter can be wrapped into a FixedSizeByteAligned<uint32_t> without copying.
                    return std::make_shared<FixedSizeByteAlignedVector<uint32_t>>(std::move(filtered_attribute_vector));
                  };

                  if constexpr (std::is_same_v<DictionarySegmentType, DictionarySegment<ColumnDataType>>) {  // NOLINT
                    const auto compressed_attribute_vector =
                        materialize_filtered_attribute_vector(typed_segment, pos_list);
                    const auto& dictionary = typed_segment.dictionary();

                    output_segments[column_id] = std::make_shared<DictionarySegment<ColumnDataType>>(
                        dictionary, std::move(compressed_attribute_vector));
                  } else if constexpr (std::is_same_v<DictionarySegmentType,  // NOLINT - lint.sh wants {} on same line
                                                      FixedStringDictionarySegment<ColumnDataType>>) {
                    const auto compressed_attribute_vector =
                        materialize_filtered_attribute_vector(typed_segment, pos_list);
                    const auto& dictionary = typed_segment.fixed_string_dictionary();

                    output_segments[column_id] = std::make_shared<FixedStringDictionarySegment<ColumnDataType>>(
                        dictionary, std::move(compressed_attribute_vector));
                  } else {
                    Fail("Referenced segment was dynamically casted to BaseDictionarySegment, but resolve failed");
                  }
                  // clang-format on
                });
          } else {
            // End of dictionary segment shortcut - handle all other referenced segments and ReferenceSegments that
            // reference more than a single chunk by materializing them into a ValueSegment
            bool has_null = false;
            auto values = pmr_vector<ColumnDataType>(segment->size());
            auto null_values = pmr_vector<bool>(
                input_table.column_is_nullable(pqp_column_expression->column_id) ? segment->size() : 0);

            auto chunk_offset = ChunkOffset{0};
            segment_iterate<ColumnDataType>(*segment, [&](const auto& position) {
              if (position.is_null()) {
                DebugAssert(!null_values.empty(), "Mismatching NULL information");
                has_null = true;
                null_values[chunk_offset] = true;
              } else {
                values[chunk_offset] = position.value();
              }
              ++chunk_offset;
            });

            auto value_segment = std::shared_ptr<ValueSegment<ColumnDataType>>{};
            if (has_null) {
              value_segment = std::make_shared<ValueSegment<ColumnDataType>>(std::move(values), std::move(null_values));
            } else {
              value_segment = std::make_shared<ValueSegment<ColumnDataType>>(std::move(values));
            }

            output_segments[column_id] = std::move(value_segment);
            column_is_nullable[column_id] = has_null;
          }
        });
        forwarding_cost += timer.lap();
      } else {
        auto output_segment = evaluator.evaluate_expression_to_segment(*expression);
        column_is_nullable[column_id] = column_is_nullable[column_id] || output_segment->is_nullable();
        output_segments[column_id] = std::move(output_segment);
        expression_evaluator_cost += timer.lap();
      }
    }

    output_chunk_segments[chunk_id] = std::move(output_segments);
  }

  step_performance_data.set_step_runtime(OperatorSteps::ForwardUnmodifiedColumns, forwarding_cost);
  step_performance_data.set_step_runtime(OperatorSteps::EvaluateNewColumns, expression_evaluator_cost);

  /**
   * Determine the TableColumnDefinitions and build the output table
   */
  TableColumnDefinitions column_definitions;
  for (auto column_id = ColumnID{0}; column_id < expressions.size(); ++column_id) {
    column_definitions.emplace_back(expressions[column_id]->as_column_name(), expressions[column_id]->data_type(),
                                    column_is_nullable[column_id]);
  }

  auto output_chunks = std::vector<std::shared_ptr<Chunk>>{chunk_count_input_table};

  // Maps input columns to output columns (which may be reordered). Only contains input column IDs that are forwarded
  // to the output without modfications.
  auto input_column_to_output_column = std::unordered_map<ColumnID, ColumnID>{};
  for (auto expression_id = ColumnID{0}; expression_id < expressions.size(); ++expression_id) {
    const auto& expression = expressions[expression_id];
    if (const auto pqp_column_expression = std::dynamic_pointer_cast<PQPColumnExpression>(expression)) {
      const auto& original_id = pqp_column_expression->column_id;
      input_column_to_output_column[original_id] = expression_id;
    }
  }

  for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count_input_table; ++chunk_id) {
    const auto input_chunk = input_table.get_chunk(chunk_id);
    Assert(input_chunk, "Physically deleted chunk should not reach this point, see get_chunk / #1686.");

    // The output chunk contains all rows that are in the stored chunk, including invalid rows. We forward this
    // information so that following operators (currently, the Validate operator) can use it for optimizations.
    const auto chunk = std::make_shared<Chunk>(std::move(output_chunk_segments[chunk_id]), input_chunk->mvcc_data());
    chunk->increase_invalid_row_count(input_chunk->invalid_row_count());
    chunk->finalize();

    // Forward sorted_by flags, mapping column ids
    const auto& sorted_by = input_chunk->individually_sorted_by();
    if (!sorted_by.empty()) {
      std::vector<SortColumnDefinition> transformed;
      transformed.reserve(sorted_by.size());
      for (const auto& [column_id, mode] : sorted_by) {
        if (!input_column_to_output_column.count(column_id)) {
          continue;  // column is not present in output expression list
        }
        const auto projected_column_id = input_column_to_output_column[column_id];
        transformed.emplace_back(SortColumnDefinition{projected_column_id, mode});
      }
      if (!transformed.empty()) {
        chunk->set_individually_sorted_by(transformed);
      }
    }

    output_chunks[chunk_id] = chunk;
  }

  step_performance_data.set_step_runtime(OperatorSteps::BuildOutput, timer.lap());

  return std::make_shared<Table>(column_definitions, output_table_type, std::move(output_chunks),
                                 input_table.uses_mvcc());
}

// returns the singleton dummy table used for literal projections
std::shared_ptr<Table> Projection::dummy_table() {
  static auto shared_dummy = std::make_shared<DummyTable>();
  return shared_dummy;
}

}  // namespace opossum
