#include "expression_evaluator_table_scan_impl.hpp"

#include "expression/expression_utils.hpp"
#include "expression/evaluation/expression_evaluator.hpp"

namespace opossum {

ExpressionEvaluatorTableScanImpl::ExpressionEvaluatorTableScanImpl(const std::shared_ptr<const Table>& in_table, const std::shared_ptr<AbstractExpression>& expression):
  _expression(expression), _expression_evaluator(in_table) {
  _expression_evaluator.populate_uncorrelated_select_cache({expression});
}

std::string ExpressionEvaluatorTableScanImpl::description() const {
  return "ExpressionEvaluator";
}

std::shared_ptr<PosList> ExpressionEvaluatorTableScanImpl::scan_chunk(ChunkID chunk_id) {
  _expression_evaluator.set_chunk_id(chunk_id);
  return std::make_shared<PosList>(_expression_evaluator.evaluate_expression_to_pos_list(*_expression));
}

}  // namespace opossum