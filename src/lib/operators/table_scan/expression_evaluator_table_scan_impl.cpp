#include "expression_evaluator_table_scan_impl.hpp"

#include <memory>
#include <string>

#include "expression/abstract_expression.hpp"
#include "expression/evaluation/expression_evaluator.hpp"
#include "storage/pos_lists/row_id_pos_list.hpp"

namespace hyrise {

ExpressionEvaluatorTableScanImpl::ExpressionEvaluatorTableScanImpl(
    const std::shared_ptr<const Table>& in_table, const std::shared_ptr<const AbstractExpression>& expression)
    : _in_table(in_table), _expression(expression) {}

std::string ExpressionEvaluatorTableScanImpl::description() const {
  return "ExpressionEvaluator";
}

std::shared_ptr<RowIDPosList> ExpressionEvaluatorTableScanImpl::scan_chunk(ChunkID chunk_id) {
  return std::make_shared<RowIDPosList>(
      ExpressionEvaluator{_in_table, chunk_id}.evaluate_expression_to_pos_list(*_expression));
}

}  // namespace hyrise
