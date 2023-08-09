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
 * Operator that performs a predicate search using indexes.
 * Note: IndexScan only scans the set of chunks passed to the constructor.
 */
class IndexScan : public AbstractReadOnlyOperator {
 public:
  IndexScan(const std::shared_ptr<const AbstractOperator>& input_operator, const ColumnID indexed_column_id,
            const PredicateCondition predicate_condition, const AllTypeVariant scan_value);

  const std::string& name() const final;

  // Must not be empty because only the specified chunks will be scanned. See TableScan::excluded_chunk_ids for usage.
  // Note: These ChunkIDs are referring to the ChunkIDs of the input operator (i.e., GetTable) at optimization-time.
  // Due to dynamic pruning, the included ChunkIDs must potentially be updated for further pruned chunks.
  std::shared_ptr<std::vector<ChunkID>> included_chunk_ids;

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
};

}  // namespace hyrise
