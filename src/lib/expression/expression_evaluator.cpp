#include "expression_evaluator.hpp"

#include "pqp_column_expression.hpp"

namespace opossum {

ExpressionEvaluator::ExpressionEvaluator(const std::shared_ptr<Chunk>& chunk):
_chunk(chunk)
{}

DataType ExpressionEvaluator::get_expression_data_type(const AbstractExpression& expression) const {
  if (expression.type == ExpressionType::Column) {
    const auto* pqp_column_expression = dynamic_cast<const PQPColumnExpression*>(&expression);
    Assert(pqp_column_expression, "ColumnExpressions to be evaluated have to be PQPColumnExpressions");
    return _chunk->get_column(pqp_column_expression->column_id)->data_type();
  } else {
    return boost::get<DataType>(expression.data_type());
  }
}

}  // namespace opossum