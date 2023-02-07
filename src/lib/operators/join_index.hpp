#pragma once

#include <memory>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "abstract_join_operator.hpp"
#include "storage/index/partial_hash/partial_hash_index.hpp"
#include "storage/pos_lists/row_id_pos_list.hpp"
#include "types.hpp"

namespace hyrise {

class MultiPredicateJoinEvaluator;

/**
   * This operator joins two tables using one column of each table.
   * A speedup compared to the Nested Loop Join is achieved by avoiding the inner loop, and instead
   * finding the index side values utilizing the index.
   * 
   * For index reference joins, only the join JoinMode::Inner is supported. Additionally, if the join segments of the
   * reference table don't provide the guarantee of referencing one single chunk (of the original data table), then the
   * fallback solution (nested join loop) is used. Using the fallback solution does not increment the number of chunks
   * scanned with index in the performance data.
   *
   * Note: An index needs to be present on the index side table in order to execute an index join.
   */
class JoinIndex : public AbstractJoinOperator {
  friend class OperatorsJoinIndexTest_DeepCopy_Test;

 public:
  static bool supports(const JoinConfiguration config);

  JoinIndex(const std::shared_ptr<const AbstractOperator>& left, const std::shared_ptr<const AbstractOperator>& right,
            const JoinMode mode, const OperatorJoinPredicate& primary_predicate,
            const std::vector<OperatorJoinPredicate>& secondary_predicates = {},
            const IndexSide index_side = IndexSide::Right,
            const std::optional<ColumnID> index_column_id_before_pruning = std::nullopt);

  const std::string& name() const override;

  enum class OperatorSteps : uint8_t { IndexJoining, NestedLoopJoining, OutputWriting };

  struct PerformanceData : public OperatorPerformanceData<OperatorSteps> {
    void output_to_stream(std::ostream& stream, DescriptionMode description_mode) const override;

    // Per default, the right input is the index side and the left side is the probe side.
    bool right_input_is_index_side{true};

    size_t chunks_scanned_with_index{0};
    size_t chunks_scanned_without_index{0};
  };

  std::string description(DescriptionMode description_mode) const override;

 protected:
  std::shared_ptr<const Table> _on_execute() override;

  std::shared_ptr<AbstractOperator> _on_deep_copy(
      const std::shared_ptr<AbstractOperator>& copied_left_input,
      const std::shared_ptr<AbstractOperator>& copied_right_input,
      std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& copied_ops) const override;

  template <typename Functor>
  void _scan_probe_side_input(const Functor& functor);

  void _fallback_nested_loop(const ChunkID index_chunk_id, const bool track_probe_matches,
                             const bool track_index_matches, const bool is_semi_or_anti_join,
                             MultiPredicateJoinEvaluator& secondary_predicate_evaluator);


  template <typename ProbeIterator>
  void _data_join_probe_segment_with_indexed_segments(ProbeIterator probe_iter, ProbeIterator probe_end,
                                                      const ChunkID probe_chunk_id,
                                                      const std::shared_ptr<PartialHashIndex>& table_index);

  void _append_matches_table_index(const PartialHashIndex::Iterator& range_begin,
                                   const PartialHashIndex::Iterator& range_end, const ChunkOffset probe_chunk_offset,
                                   const ChunkID probe_chunk_id);

  void _append_matches_dereferenced(const ChunkID& probe_chunk_id, const ChunkOffset& probe_chunk_offset,
                                    const RowIDPosList& index_table_matches);

  void _append_matches_non_inner(const bool is_semi_or_anti_join);

  void _write_output_segments(Segments& output_segments, const std::shared_ptr<const Table>& input_table,
                              const std::shared_ptr<RowIDPosList>& pos_list);

  void _on_cleanup() override;

  const IndexSide _index_side;
  OperatorJoinPredicate _adjusted_primary_predicate;
  std::shared_ptr<Table> _output_table;

  std::shared_ptr<const Table> _probe_input_table;
  std::shared_ptr<const Table> _index_input_table;

  std::shared_ptr<RowIDPosList> _probe_pos_list;
  std::shared_ptr<RowIDPosList> _index_pos_list;
  std::vector<bool> _index_pos_dereferenced;

  // for left/right/outer joins
  // The outer vector enumerates chunks, the inner enumerates chunk_offsets
  std::vector<std::vector<bool>> _probe_matches;
  std::vector<std::vector<bool>> _index_matches;

  // ID of the column on which the index was created on the corresponding base table. If some of the base table's
  // columns were pruned, _index_column_id represents the column id before the columns were pruned.
  ColumnID _index_column_id;
};

}  // namespace hyrise
