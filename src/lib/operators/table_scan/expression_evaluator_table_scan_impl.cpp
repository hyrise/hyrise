#include "expression_evaluator_table_scan_impl.hpp"

#include "expression/evaluation/expression_evaluator.hpp"

namespace opossum {

ExpressionEvaluatorTableScanImpl::ExpressionEvaluatorTableScanImpl(const std::shared_ptr<const Table>& in_table, const std::shared_ptr<AbstractExpression>& expression):
  _in_table(in_table), _expression(expression) {

}

std::shared_ptr<PosList> ExpressionEvaluatorTableScanImpl::scan_chunk(ChunkID chunk_id) {
  return std::make_shared<PosList>(ExpressionEvaluator{_in_table, chunk_id}.evaluate_expression_to_pos_list(*_expression));
}

}  // namespace opossum