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

namespace opossum {

class Table;

class TableScan : public AbstractReadOnlyOperator {
  friend class LQPTranslatorTest;

 public:
  TableScan(const std::shared_ptr<const AbstractOperator>& in, const std::shared_ptr<AbstractExpression>& predicate);

  const std::shared_ptr<AbstractExpression>& predicate() const;

  const std::string& name() const override;
  std::string description(DescriptionMode description_mode) const override;

  /**
   * Create the TableScanImpl based on the predicate type. Public for testing purposes.
   */
  std::unique_ptr<AbstractTableScanImpl> create_impl() const;

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

 protected:
  std::shared_ptr<const Table> _on_execute() override;

  std::shared_ptr<AbstractOperator> _on_deep_copy(
      const std::shared_ptr<AbstractOperator>& copied_input_left,
      const std::shared_ptr<AbstractOperator>& copied_input_right) const override;

  void _on_set_transaction_context(const std::weak_ptr<TransactionContext>& transaction_context) override;
  void _on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) override;

  void _on_cleanup() override;

  // Turns top-level uncorrelated subqueries into their value, e.g. `a = (SELECT 123)` becomes `a = 123`. This makes it
  // easier to avoid using the more expensive ExpressionEvaluatorTableScanImpl.
  static std::shared_ptr<AbstractExpression> _resolve_uncorrelated_subqueries(
      const std::shared_ptr<AbstractExpression>& predicate);

 private:
  const std::shared_ptr<AbstractExpression> _predicate;

  std::unique_ptr<AbstractTableScanImpl> _impl;

  // The description of the impl, so that it still available after the _impl is resetted in _on_cleanup()
  std::string _impl_description{"Unset"};
};

}  // namespace opossum
