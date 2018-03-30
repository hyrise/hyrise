#include "case_operator.hpp"

#include <sstream>

#include "storage/column_iterables/any_column_iterator.hpp"
#include "storage/create_iterable_from_column.hpp"
#include "storage/materialize.hpp"

namespace {

using namespace opossum;  // NOLINT

/**
 * Turns a PhysicalCaseExpressionResult - which is either a THEN result or an ELSE result - into a value/null - pair
 */
template <typename ResultDataType>
std::pair<bool, ResultDataType> materialize_case_result(
    const PhysicalCaseExpressionResult<ResultDataType>& case_result, const Chunk& chunk, const ChunkOffset chunk_offset,
    std::vector<std::optional<std::vector<std::pair<bool, ResultDataType>>>>& materialized_column_cache) {
  /**
   * CaseExpressionResult can be either Null, a value from a column or a constant value
   */

  if (case_result.type() == typeid(Null)) {
    return {true, {}};
  } else if (case_result.type() == typeid(ColumnID)) {
    // If not yet done, materialize the column as values and nulls and cache it.
    const auto column_id = boost::get<ColumnID>(case_result);
    auto& materialized_column = materialized_column_cache[column_id];
    if (!materialized_column) {
      materialized_column.emplace();
      materialized_column->reserve(chunk.size());
      materialize_values_and_nulls(*chunk.get_column(column_id), *materialized_column);
    }

    return (*materialized_column)[chunk_offset];
  } else if (case_result.type() == typeid(ResultDataType)) {
    return {false, boost::get<ResultDataType>(case_result)};
  } else {
    Fail("Unexpected type in THEN");
  }
}

}  // namespace

namespace opossum {

CaseOperator::CaseOperator(const std::shared_ptr<AbstractOperator>& input, const Expressions& case_expressions)
    : AbstractReadOnlyOperator(OperatorType::Case, input), _case_expressions(std::move(case_expressions)) {}

const std::string CaseOperator::name() const { return "Case"; }

const std::string CaseOperator::description(DescriptionMode description_mode) const {
  /**
   * SingleLine:
   *    CASE {[WHEN #Col0 THEN 42, WHEN #Col1 THEN 43, ELSE 55], [WHEN #Col0 THEN #Col1, ELSE NULL]}
   *
   * MultiLine:
   *    CASE {
   *        [WHEN #Col0 THEN 42, WHEN #Col1 THEN 43, ELSE 55]
   *        [WHEN #Col0 THEN #Col1, ELSE NULL]}
   */

  const auto next_expression_separator = description_mode == DescriptionMode::SingleLine ? ", " : "\n     ";

  std::stringstream stream;
  stream << "CASE {";

  if (description_mode == DescriptionMode::MultiLine) stream << next_expression_separator;

  for (auto expression_idx = size_t{0}; expression_idx < _case_expressions.size(); ++expression_idx) {
    const auto& abstract_expression = _case_expressions[expression_idx];

    stream << "[";

    resolve_data_type(abstract_expression->result_data_type, [&](auto type) {
      using ResultDataType = typename decltype(type)::type;

      // Needed for THEN and ELSE. Lambda because having this as a free function feels unnecessary
      const auto stream_case_result = [&](const auto& case_result) {
        if (case_result.type() == typeid(Null)) {
          stream << "NULL";
        } else if (case_result.type() == typeid(ColumnID)) {
          stream << "#Col" << boost::get<ColumnID>(case_result);
        } else if (case_result.type() == typeid(ResultDataType)) {
          stream << "'" << boost::get<ResultDataType>(case_result) << "'";
        }
      };

      const auto& expression = static_cast<const PhysicalCaseExpression<ResultDataType>&>(*abstract_expression);

      for (const auto& clause : expression.clauses) {
        stream << "WHEN #Col" << clause.when << " THEN ";
        stream_case_result(clause.then);
        stream << ", ";
      }

      stream << "ELSE ";
      stream_case_result(expression.else_);
    });

    stream << "]";

    if (expression_idx + 1 < _case_expressions.size()) stream << next_expression_separator;
  }

  stream << "}";

  return stream.str();
}

std::shared_ptr<const Table> CaseOperator::_on_execute() {
  /**
   * Create ColumnDefinitions for the output table - consisting of the columns from the input, plus one for every
   * _case_expression.
   */
  auto column_definitions = input_table_left()->column_definitions();
  for (const auto& case_expression : _case_expressions) {
    column_definitions.emplace_back("case_expr", case_expression->result_data_type, true);
  }

  const auto output_table = std::make_shared<Table>(column_definitions, TableType::Data);

  /**
   * For every Chunk in the input Table, create a Chunk in the output Table that contains the same columns as the input
   * Table with the results from the CaseExpressions each as an additional Column
   */
  for (ChunkID chunk_id{0}; chunk_id < input_table_left()->chunk_count(); ++chunk_id) {
    const auto input_chunk = input_table_left()->get_chunk(chunk_id);

    /**
     * Case appends a new column for every CaseExpression. All input columns are forwarded, and as Case produces a
     * materialized Data Table, we re-use the columns from the input chunk if they are Data Columns and materialize them
     * if they are Reference Columns.
     */
    ChunkColumns output_columns;
    if (input_table_left()->type() == TableType::References) {
      for (const auto& column : input_chunk->columns()) {
        output_columns.emplace_back(materialize_as_value_column(*column));
      }
    } else {
      output_columns = input_chunk->columns();
    }

    /**
     * Create an output_column from each CaseExpression
     */

    // If a column in the input chunk is used as a WHEN column, it is materialized into this cache
    std::vector<std::optional<std::vector<int32_t>>> materialized_when_columns(input_table_left()->column_count());

    for (const auto& abstract_case_expression : _case_expressions) {
      resolve_data_type(abstract_case_expression->result_data_type, [&](auto type) {
        using ResultDataType = typename decltype(type)::type;
        using NullOrValue = std::pair<bool, ResultDataType>;

        // If a column in the input chunk is used as a THEN column, it is materialized into this cache
        std::vector<std::optional<std::vector<NullOrValue>>> materialized_then_columns(
            input_table_left()->column_count());

        const auto& case_expression =
            static_cast<const PhysicalCaseExpression<ResultDataType>&>(*abstract_case_expression);

        pmr_concurrent_vector<ResultDataType> output_values(input_chunk->size());
        pmr_concurrent_vector<bool> output_nulls(input_chunk->size());

        /**
         * Compute the CaseExpression for each row in the Chunk.
         */
        for (auto chunk_offset = ChunkOffset{0}; chunk_offset < input_chunk->size(); ++chunk_offset) {
          auto any_when_claused_fulfilled = false;  // If no WHEN clause was TRUE, we have to use the ELSE clause

          /**
           * Find the first case_clause whose WHEN is true and write its THEN to output_values/nulls
           */
          for (const auto& case_clause : case_expression.clauses) {
            // If not yet done, materialize the WHEN column and cache it.
            auto& materialized_when_column = materialized_when_columns[case_clause.when];
            if (!materialized_when_column) {
              materialized_when_column.emplace();
              materialized_when_column->reserve(input_chunk->size());
              materialize_values(*input_chunk->get_column(case_clause.when), *materialized_when_column);
            }

            // The case_clause is not true? Continue with the next clause.
            if ((*materialized_when_column)[chunk_offset] == int32_t{0}) continue;

            const auto null_and_value =
                materialize_case_result(case_clause.then, *input_chunk, chunk_offset, materialized_then_columns);
            output_nulls[chunk_offset] = null_and_value.first;
            output_values[chunk_offset] = null_and_value.second;

            any_when_claused_fulfilled = true;

            break;
          }

          /**
           * If none of the case_clauses were TRUE, the result of the CaseExpression is in ELSE (which is NULL by default)
           */
          if (!any_when_claused_fulfilled) {
            const auto null_and_value =
                materialize_case_result(case_expression.else_, *input_chunk, chunk_offset, materialized_then_columns);
            output_nulls[chunk_offset] = null_and_value.first;
            output_values[chunk_offset] = null_and_value.second;
          }
        }

        output_columns.emplace_back(
            std::make_shared<ValueColumn<ResultDataType>>(std::move(output_values), std::move(output_nulls)));
      });
    }

    output_table->append_chunk(output_columns);
  }

  return output_table;
}

std::shared_ptr<AbstractOperator> CaseOperator::_on_recreate(
    const std::vector<AllParameterVariant>& args, const std::shared_ptr<AbstractOperator>& recreated_input_left,
    const std::shared_ptr<AbstractOperator>& recreated_input_right) const {
  return std::make_shared<CaseOperator>(recreated_input_left, _case_expressions);
}

}  // namespace opossum
