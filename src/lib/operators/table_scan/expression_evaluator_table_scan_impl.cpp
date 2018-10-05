#include "expression_evaluator_table_scan_impl.hpp"

#include "expression/expression_utils.hpp"
#include "expression/evaluation/expression_evaluator.hpp"

namespace opossum {

ExpressionEvaluatorTableScanImpl::ExpressionEvaluatorTableScanImpl(const std::shared_ptr<const Table>& in_table, const std::shared_ptr<AbstractExpression>& expression):
  _in_table(in_table), _expression(expression) {

  /**
   * Performance hack:
   *  Identify PQPSelectExpressions that are not correlated and execute them once (instead of once per chunk),
   *  cache their result tables and use them in the per-chunk loop below.
   */
  _uncorrelated_select_results = std::make_shared<ExpressionEvaluator::UncorrelatedSelectResults>();
  {
    auto evaluator = ExpressionEvaluator{};
    visit_expression(expression, [&](const auto& sub_expression) {
      const auto pqp_select_expression = std::dynamic_pointer_cast<PQPSelectExpression>(sub_expression);
      if (pqp_select_expression && !pqp_select_expression->is_correlated()) {
        auto result = evaluator.evaluate_uncorrelated_select_expression(*pqp_select_expression);
        _uncorrelated_select_results->emplace(pqp_select_expression->pqp, std::move(result));
        return ExpressionVisitation::DoNotVisitArguments;
      }

      return ExpressionVisitation::VisitArguments;
    });
  }
}

std::shared_ptr<PosList> ExpressionEvaluatorTableScanImpl::scan_chunk(ChunkID chunk_id) {
  return std::make_shared<PosList>(ExpressionEvaluator{_in_table, chunk_id, _uncorrelated_select_results}.evaluate_expression_to_pos_list(*_expression));
}

}  // namespace opossum