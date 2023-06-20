#pragma once

#include <memory>

#include "abstract_read_only_operator.hpp"

#include "all_type_variant.hpp"
#include "storage/pos_lists/row_id_pos_list.hpp"
#include "types.hpp"

namespace hyrise {

class Table;
class AbstractTask;

/**
 * Operator that performs a predicate search using indexes
 *
 * Note: Scans only the set of chunks passed to the constructor
 */
class IndexScan : public AbstractReadOnlyOperator {
 public:
  IndexScan(const std::shared_ptr<const AbstractOperator>& input_operator, const ColumnID indexed_column_id,
            const PredicateCondition predicate_condition, const AllTypeVariant scan_value);

  const std::string& name() const final;

  // Must not be empty because only the specified chunks will be scanned. See TableScan::excluded_chunk_ids for usage.
  // Note: These ChunkIDs are reffering to the ChunkIDs of a (!! non-pruned !!) data table to which the index belongs.
  // They do not refer to the ChunkIDs output by potentially chunk-pruned GetTable operartors (i.e., the index scan's
  // input).
  std::vector<ChunkID> included_chunk_ids;

 protected:
  std::shared_ptr<const Table> _on_execute() final;

  std::shared_ptr<AbstractOperator> _on_deep_copy(
      const std::shared_ptr<AbstractOperator>& copied_left_input,
      const std::shared_ptr<AbstractOperator>& /*copied_right_input*/,
      std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& /*copied_ops*/) const override;
  void _on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) override;

 private:
  ColumnID _indexed_column_id;
  const PredicateCondition _predicate_condition;
  const AllTypeVariant _scan_value;

  std::shared_ptr<const Table> _in_table;
  std::shared_ptr<Table> _out_table;

  const std::optional<std::vector<std::optional<ChunkID>>> _chunk_id_mapping;
};

}  // namespace hyrise
