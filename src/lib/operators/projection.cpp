#include "projection.hpp"

#include <algorithm>
#include <functional>
#include <memory>
#include <numeric>
#include <string>
#include <utility>
#include <vector>

#include "expression/expression_utils.hpp"
#include "expression/evaluation/expression_evaluator.hpp"
#include "expression/value_expression.hpp"

namespace opossum {

Projection::Projection(const std::shared_ptr<const AbstractOperator>& in, const std::vector<std::shared_ptr<AbstractExpression>>& expressions)
    : AbstractReadOnlyOperator(OperatorType::Projection, in), expressions(expressions) {}

const std::string Projection::name() const { return "Projection"; }

std::shared_ptr<AbstractOperator> Projection::_on_recreate(
    const std::vector<AllParameterVariant>& args, const std::shared_ptr<AbstractOperator>& recreated_input_left,
    const std::shared_ptr<AbstractOperator>& recreated_input_right) const {
  return std::make_shared<Projection>(recreated_input_left, expressions_copy(expressions));
}

void Projection::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {
  expressions_set_parameters(expressions, parameters);
}

void Projection::_on_set_transaction_context(std::weak_ptr<TransactionContext> transaction_context) {
  expressions_set_transaction_context(expressions, transaction_context);
}

std::shared_ptr<const Table> Projection::_on_execute() {
  /**
   * Determine the TableColumnDefinitions and create the output table from them
   */
  TableColumnDefinitions column_definitions;
  for (const auto& expression : expressions) {
    TableColumnDefinition column_definition;

    column_definition.data_type = expression->data_type();
    column_definition.name = expression->as_column_name();
    column_definition.nullable = expression->is_nullable();

    column_definitions.emplace_back(column_definition);
  }

  auto output_table = std::make_shared<Table>(column_definitions, TableType::Data, input_table_left()->max_chunk_size());

  /**
   * Perform the projection
   */
  for (auto chunk_id = ChunkID{0}; chunk_id < input_table_left()->chunk_count(); ++chunk_id) {
    ChunkColumns output_columns;
    output_columns.reserve(expressions.size());

    ExpressionEvaluator evaluator(input_table_left(), chunk_id);

    for (const auto& expression : expressions) {
      output_columns.emplace_back(evaluator.evaluate_expression_to_column(*expression));
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
