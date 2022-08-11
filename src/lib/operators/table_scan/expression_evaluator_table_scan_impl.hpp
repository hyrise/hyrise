#pragma once

#include "abstract_table_scan_impl.hpp"
#include "expression/abstract_expression.hpp"
#include "expression/evaluation/expression_evaluator.hpp"

namespace hyrise {

class Table;
class UncorrelatedSubqueryResults;

/**
 * Uses the ExpressionEvaluator::evaluate_expression_to_pos_list() for a fallback implementation of the
 * AbstractTableScanImpl. This is likely slower than any specialized `AbstractTableScanImpl` and should thus only be
 * used if a particular expression type doesn't have a specialized `AbstractTableScanImpl`.
 */
class ExpressionEvaluatorTableScanImpl : public AbstractTableScanImpl {
 public:
  ExpressionEvaluatorTableScanImpl(
      const std::shared_ptr<const Table>& in_table, const std::shared_ptr<const AbstractExpression>& expression,
      const std::shared_ptr<const ExpressionEvaluator::UncorrelatedSubqueryResults>& uncorrelated_subquery_results);

  std::string description() const override;
  std::shared_ptr<RowIDPosList> scan_chunk(ChunkID chunk_id) override;

 private:
  std::shared_ptr<const Table> _in_table;
  std::shared_ptr<const AbstractExpression> _expression;
  const std::shared_ptr<const ExpressionEvaluator::UncorrelatedSubqueryResults> _uncorrelated_subquery_results;
};

}  // namespace hyrise
