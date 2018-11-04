#pragma once

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "abstract_read_only_operator.hpp"
#include "all_parameter_variant.hpp"
#include "expression/abstract_expression.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

class AbstractTableScanImpl;
class Table;

class TableScan : public AbstractReadOnlyOperator {
  friend class LQPTranslatorTest;

 public:
  TableScan(const std::shared_ptr<const AbstractOperator>& in, const std::shared_ptr<AbstractExpression>& predicate);

  ~TableScan();

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
  void set_excluded_chunk_ids(const std::vector<ChunkID>& chunk_ids);

  const std::shared_ptr<AbstractExpression>& predicate() const;

  const std::string name() const override;
  const std::string description(DescriptionMode description_mode) const override;

  /**
   * Create the TableScanImpl based on the predicate type. Public for testing purposes.
   */
  std::unique_ptr<AbstractTableScanImpl> create_impl() const;

 protected:
  std::shared_ptr<const Table> _on_execute() override;

  std::shared_ptr<AbstractOperator> _on_deep_copy(
      const std::shared_ptr<AbstractOperator>& copied_input_left,
      const std::shared_ptr<AbstractOperator>& copied_input_right) const override;

  void _on_set_transaction_context(const std::weak_ptr<TransactionContext>& transaction_context) override;
  void _on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) override;

  void _on_cleanup() override;

 private:
  const std::shared_ptr<AbstractExpression> _predicate;

  std::unique_ptr<AbstractTableScanImpl> _impl;

  // The description of the impl, so that it still available after the _impl is resetted in _on_cleanup()
  std::string _impl_description{"Unset"};

  std::vector<ChunkID> _excluded_chunk_ids;
};

}  // namespace opossum
