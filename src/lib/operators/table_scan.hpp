#pragma once

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "abstract_read_only_operator.hpp"
#include "all_parameter_variant.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

class BaseTableScanImpl;
class Table;

class TableScan : public AbstractReadOnlyOperator {
  friend class LQPTranslatorTest;

 public:
  TableScan(const std::shared_ptr<const AbstractOperator>& in, ColumnID left_column_id,
            const PredicateCondition predicate_condition, const AllParameterVariant& right_parameter);

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

  ColumnID left_column_id() const;
  PredicateCondition predicate_condition() const;
  const AllParameterVariant& right_parameter() const;

  const std::string name() const override;
  const std::string description(DescriptionMode description_mode) const override;

 protected:
  std::shared_ptr<const Table> _on_execute() override;

  std::shared_ptr<AbstractOperator> _on_deep_copy(
      const std::shared_ptr<AbstractOperator>& copied_input_left,
      const std::shared_ptr<AbstractOperator>& copied_input_right) const override;

  void _on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) override;

  void _on_cleanup() override;

  void _init_scan();

 private:
  const ColumnID _left_column_id;
  const PredicateCondition _predicate_condition;
  AllParameterVariant _right_parameter;

  std::vector<ChunkID> _excluded_chunk_ids;

  std::shared_ptr<const Table> _in_table;
  std::unique_ptr<BaseTableScanImpl> _impl;
  std::shared_ptr<Table> _output_table;
};

}  // namespace opossum
