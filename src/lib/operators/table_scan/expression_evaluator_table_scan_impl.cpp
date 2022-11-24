#include "expression_evaluator_table_scan_impl.hpp"

#include "expression/evaluation/expression_evaluator.hpp"
#include "expression/expression_utils.hpp"

namespace hyrise {

ExpressionEvaluatorTableScanImpl::ExpressionEvaluatorTableScanImpl(
    const std::shared_ptr<const Table>& in_table, const std::shared_ptr<const AbstractExpression>& expression,
    const std::shared_ptr<const ExpressionEvaluator::UncorrelatedSubqueryResults>& uncorrelated_subquery_results)
    : _in_table(in_table), _expression(expression), _uncorrelated_subquery_results(uncorrelated_subquery_results) {}

std::string ExpressionEvaluatorTableScanImpl::description() const {
  return "ExpressionEvaluator";
}

std::shared_ptr<RowIDPosList> ExpressionEvaluatorTableScanImpl::scan_chunk(ChunkID chunk_id) {
  return std::make_shared<RowIDPosList>(
      ExpressionEvaluator{_in_table, chunk_id, _uncorrelated_subquery_results}.evaluate_expression_to_pos_list(
          *_expression));
}

}  // namespace hyrise
