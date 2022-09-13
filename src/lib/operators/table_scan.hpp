#pragma once

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "abstract_read_only_operator.hpp"
#include "all_parameter_variant.hpp"
#include "expression/abstract_expression.hpp"
#include "table_scan/abstract_table_scan_impl.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace hyrise {

class PQPSubqueryExpression;
class Table;

class TableScan : public AbstractReadOnlyOperator {
  friend class LQPTranslatorTest;

 public:
  TableScan(const std::shared_ptr<const AbstractOperator>& input_operator,
            const std::shared_ptr<AbstractExpression>& predicate);

  const std::shared_ptr<AbstractExpression>& predicate() const;

  const std::string& name() const override;
  std::string description(DescriptionMode description_mode) const override;

  /**
   * Create the TableScanImpl based on the predicate type. Public for testing purposes.
   * Resolves uncorrelated subqueries, caches their results in the ExpressionEvaluator or a new predicate, and
   * deregisters accordingly.
   */
  std::unique_ptr<AbstractTableScanImpl> create_impl();

  /**
   * @brief If set, the specified chunks will not be scanned.
   *
   * There are different implementations of table scans.
   * This is the standard linear scan; another one is
   * the index scan, which scans a column using an index.
   * Depending on the situation, it is advantageous to use
   * the index scan for some chunks and the standard scan for
   * others. However one has to ensure that all chunks including
   * newly added are scanned including those that were added
   * since the optimizer had distributed the chunks between
   * operators. This is why this scan accepts a list of
   * excluded chunks and all others a list of included chunks.
   */
  std::vector<ChunkID> excluded_chunk_ids;

  struct PerformanceData : public OperatorPerformanceData<AbstractOperatorPerformanceData::NoSteps> {
    std::atomic_size_t num_chunks_with_early_out{0};
    std::atomic_size_t num_chunks_with_all_rows_matching{0};
    std::atomic_size_t num_chunks_with_binary_search{0};

    void output_to_stream(std::ostream& stream, DescriptionMode description_mode) const override {
      OperatorPerformanceData<AbstractOperatorPerformanceData::NoSteps>::output_to_stream(stream, description_mode);

      const auto separator = (description_mode == DescriptionMode::SingleLine ? ' ' : '\n');
      stream << separator << "Chunks: " << num_chunks_with_early_out.load() << " skipped with no results, ";
      stream << separator << num_chunks_with_all_rows_matching.load() << " skipped with all matching, ";
      stream << num_chunks_with_binary_search.load() << " scanned using binary search.";
    }
  };

 protected:
  std::shared_ptr<const Table> _on_execute() override;

  std::shared_ptr<AbstractOperator> _on_deep_copy(
      const std::shared_ptr<AbstractOperator>& copied_left_input,
      const std::shared_ptr<AbstractOperator>& copied_right_input,
      std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& copied_ops) const override;

  void _on_set_transaction_context(const std::weak_ptr<TransactionContext>& transaction_context) override;
  void _on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) override;

  void _on_cleanup() override;

  // Turns top-level uncorrelated subqueries into their value, e.g. `a = (SELECT 123)` becomes `a = 123`. This makes it
  // easier to avoid using the more expensive ExpressionEvaluatorTableScanImpl.
  std::shared_ptr<const AbstractExpression> _resolve_uncorrelated_subqueries(
      const std::shared_ptr<const AbstractExpression>& predicate);

 private:
  const std::shared_ptr<AbstractExpression> _predicate;
  std::vector<std::shared_ptr<PQPSubqueryExpression>> _uncorrelated_subquery_expressions;

  std::unique_ptr<AbstractTableScanImpl> _impl;

  // The description of the impl, so that it still available after the _impl is resetted in _on_cleanup()
  std::string _impl_description{"Unset"};
};

}  // namespace hyrise
