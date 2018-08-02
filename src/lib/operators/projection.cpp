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
#include "utils/assert.hpp"

namespace opossum {

Projection::Projection(const std::shared_ptr<const AbstractOperator>& in,
                       const std::vector<std::shared_ptr<AbstractExpression>>& expressions)
    : AbstractReadOnlyOperator(OperatorType::Projection, in), expressions(expressions) {}

const std::string Projection::name() const { return "Projection"; }

std::shared_ptr<AbstractOperator> Projection::_on_deep_copy(
    const std::shared_ptr<AbstractOperator>& copied_input_left,
    const std::shared_ptr<AbstractOperator>& copied_input_right) const {
  return std::make_shared<Projection>(copied_input_left, expressions_deep_copy(expressions));
}

void Projection::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {
  expressions_set_parameters(expressions, parameters);
}

void Projection::_on_set_transaction_context(const std::weak_ptr<TransactionContext>& transaction_context) {
  expressions_set_transaction_context(expressions, transaction_context);
}

std::shared_ptr<const Table> Projection::_on_execute() {
  /**
   * Determine the TableColumnDefinitions
   */
  TableColumnDefinitions column_definitions;
  for (const auto& expression : expressions) {
    column_definitions.emplace_back(expression->as_column_name(), expression->data_type(), expression->is_nullable());
  }

  /**
   * Check if we should just forward columns from the input
   */
  const auto forward_columns = std::all_of(expressions.begin(), expressions.end(), [&](const auto& expression) {
    return expression->type == ExpressionType::PQPColumn;
  });

  const auto output_table_type = forward_columns ? input_table_left()->type() : TableType::Data;
  const auto output_table =
      std::make_shared<Table>(column_definitions, output_table_type, input_table_left()->max_chunk_size());

  /**
   * Perform the projection
   */
  for (auto chunk_id = ChunkID{0}; chunk_id < input_table_left()->chunk_count(); ++chunk_id) {
    ChunkColumns output_columns;
    output_columns.reserve(expressions.size());

    ExpressionEvaluator evaluator(input_table_left(), chunk_id);
    for (const auto& expression : expressions) {
      if (forward_columns) {
        const auto pqp_column_expression = std::dynamic_pointer_cast<PQPColumnExpression>(expression);
        Assert(pqp_column_expression, "Expected PQPColumnExpression");
        output_columns.emplace_back(
            input_table_left()->get_chunk(chunk_id)->get_mutable_column(pqp_column_expression->column_id));
      } else {
        output_columns.emplace_back(evaluator.evaluate_expression_to_column(*expression));
      }
    }

    output_table->append_chunk(output_columns);
  }

  return output_table;
}

// returns the singleton dummy table used for literal projections
std::shared_ptr<Table> Projection::dummy_table() {
  static auto shared_dummy = std::make_shared<DummyTable>();
  return shared_dummy;
}

}  // namespace opossum
