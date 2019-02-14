#include "expression_evaluator_table_scan_impl.hpp"

#include "expression/evaluation/expression_evaluator.hpp"
#include "expression/expression_utils.hpp"

namespace opossum {

ExpressionEvaluatorTableScanImpl::ExpressionEvaluatorTableScanImpl(
    const std::shared_ptr<const Table>& in_table, const std::shared_ptr<AbstractExpression>& expression)
    : _in_table(in_table), _expression(expression) {
  _uncorrelated_subquery_results = ExpressionEvaluator::populate_uncorrelated_subquery_results_cache({expression});
}

std::string ExpressionEvaluatorTableScanImpl::description() const { return "ExpressionEvaluator"; }

std::shared_ptr<PosList> ExpressionEvaluatorTableScanImpl::scan_chunk(ChunkID chunk_id) const {
  return std::make_shared<PosList>(
      ExpressionEvaluator{_in_table, chunk_id, _uncorrelated_subquery_results}.evaluate_expression_to_pos_list(
          *_expression));
}

}  // namespace opossum
