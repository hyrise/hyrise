#pragma once

#include "abstract_read_only_operator.hpp"
#include "concurrency/transaction_context.hpp"

namespace hyrise {

// Operator to retrieve a table from the StorageManager by specifying its name. Depending on how the operator was
// constructed, chunks and columns may be pruned if they are irrelevant for the final result. The returned table is NOT
// the same table as stored in the StorageManager. If that stored table is changed (most importantly: if a chunk is
// added), this is not reflected in GetTable's result. This is by design to make sure that following operators do not
// have to deal with tables that change their chunk count while they are being looked at. However, rows added to a chunk
// within that stored table that was already present when GetTable was executed will be visible when calling
// get_output().
class GetTable : public AbstractReadOnlyOperator {
 public:
  // Convenience constructor without pruning info
  explicit GetTable(const std::string& name);

  // Constructor with pruning info
  GetTable(const std::string& name, const std::vector<ChunkID>& pruned_chunk_ids,
           const std::vector<ColumnID>& pruned_column_ids);

  const std::string& name() const override;
  std::string description(DescriptionMode description_mode) const override;

  const std::string& table_name() const;
  const std::vector<ChunkID>& pruned_chunk_ids() const;
  const std::vector<ColumnID>& pruned_column_ids() const;

  // Predicates that contain uncorrelated subqueries cannot be used for chunk pruning in the optimization phase since we
  // do not know the predicate value yet. However, the ChunkPruningRule attaches the corresponding PredicateNodes to the
  // StoredTableNode of the table the predicates are performed on. We attach the translated predicates (i.e.,
  // TableScans) to the GetTable operators so they can use them for pruning during execution ("dynamic pruning"), when
  // the subqueries might have already been executed and the predicate value is known.
  void set_prunable_subquery_predicates(const std::vector<std::weak_ptr<const AbstractOperator>>& subquery_scans) const;
  std::vector<std::shared_ptr<const AbstractOperator>> prunable_subquery_predicates() const;

 protected:
  std::shared_ptr<AbstractOperator> _on_deep_copy(
      const std::shared_ptr<AbstractOperator>& /*copied_left_input*/,
      const std::shared_ptr<AbstractOperator>& /*copied_right_input*/,
      std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& /*copied_ops*/) const override;
  void _on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) override;

  std::shared_ptr<const Table> _on_execute() override;

  // Resolve the predicate values for uncorrelated subqueries if they have already been executed. If so, perform chunk
  // pruning with the predicates and return the pruned ChunkIDs.
  std::set<ChunkID> _prune_chunks_dynamically();

  // Name of the table to retrieve.
  const std::string _name;
  const std::vector<ChunkID> _pruned_chunk_ids;
  const std::vector<ColumnID> _pruned_column_ids;

  mutable std::vector<std::weak_ptr<const AbstractOperator>> _prunable_subquery_scans{};
  std::set<ChunkID> _dynamically_pruned_chunk_ids{};
};

}  // namespace hyrise
