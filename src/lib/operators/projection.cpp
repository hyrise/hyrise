#include "projection.hpp"

#include <algorithm>
#include <functional>
#include <memory>
#include <numeric>
#include <string>
#include <utility>
#include <vector>

#include "expression/expression_utils.hpp"
#include "expression/expression_evaluator.hpp"
#include "expression/external_expression.hpp"
#include "expression/value_expression.hpp"
#include "expression/value_placeholder_expression.hpp"

namespace opossum {

Projection::Projection(const std::shared_ptr<const AbstractOperator>& in, const std::vector<std::shared_ptr<AbstractExpression>>& expressions)
    : AbstractReadOnlyOperator(OperatorType::Projection, in), expressions(expressions) {}

const std::string Projection::name() const { return "Projection"; }

std::shared_ptr<AbstractOperator> Projection::_on_recreate(
    const std::vector<AllParameterVariant>& args, const std::shared_ptr<AbstractOperator>& recreated_input_left,
    const std::shared_ptr<AbstractOperator>& recreated_input_right) const {

  /**
   * Replace ExternalExpressions and ValuePlaceholderExpressions with actual values. This makes corelated subqueries
   * and parameterised queries work.
   */
  auto expressions = expressions_copy(this->expressions);
  for (auto& expression : expressions) {
    visit_expression(expression, [&](auto& current_expression) {
      if (current_expression->type != ExpressionType::ValuePlaceholder && current_expression->type != ExpressionType::External) return true;

      auto value_placeholder_idx = uint16_t{0};
      if (current_expression->type == ExpressionType::ValuePlaceholder) {
        const auto value_placeholder_expression = std::static_pointer_cast<ValuePlaceholderExpression>(current_expression);
        value_placeholder_idx = value_placeholder_expression->value_placeholder.index();
      } else {
        const auto external_expression = std::static_pointer_cast<ExternalExpression>(current_expression);
        value_placeholder_idx = external_expression->value_placeholder.index();
      }

      current_expression = std::make_shared<ValueExpression>(boost::get<AllTypeVariant>(args[value_placeholder_idx]));
      return false;
    });
  }

  return std::make_shared<Projection>(recreated_input_left, expressions);
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
    column_definition.nullable = true;

    column_definitions.emplace_back(column_definition);
  }

  auto output_table = std::make_shared<Table>(column_definitions, TableType::Data, input_table_left()->max_chunk_size());

  /**
   * Perform the projection
   */
  for (auto chunk_id = ChunkID{0}; chunk_id < input_table_left()->chunk_count(); ++chunk_id) {
    ChunkColumns output_columns;
    output_columns.reserve(expressions.size());

    ExpressionEvaluator evaluator(input_table_left()->get_chunk(chunk_id));

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
