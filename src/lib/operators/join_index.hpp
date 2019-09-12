#pragma once

#include <memory>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "abstract_join_operator.hpp"
#include "storage/index/abstract_index.hpp"
#include "storage/pos_list.hpp"
#include "types.hpp"

namespace opossum {

class MultiPredicateJoinEvaluator;
using IndexRange = std::pair<AbstractIndex::Iterator, AbstractIndex::Iterator>;

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
 public:
  static bool supports(const JoinConfiguration config);

  JoinIndex(const std::shared_ptr<const AbstractOperator>& left, const std::shared_ptr<const AbstractOperator>& right,
            const JoinMode mode, const OperatorJoinPredicate& primary_predicate,
            const std::vector<OperatorJoinPredicate>& secondary_predicates = {},
            const IndexSide index_side = IndexSide::Right);

  const std::string name() const override;

  struct PerformanceData : public OperatorPerformanceData {
    size_t chunks_scanned_with_index{0};
    size_t chunks_scanned_without_index{0};

    void output_to_stream(std::ostream& stream, DescriptionMode description_mode) const override;
  };

 protected:
  std::shared_ptr<const Table> _on_execute() override;

  std::shared_ptr<AbstractOperator> _on_deep_copy(
      const std::shared_ptr<AbstractOperator>& copied_input_left,
      const std::shared_ptr<AbstractOperator>& copied_input_right) const override;
  void _on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) override;

  void _fallback_nested_loop(const ChunkID index_chunk_id, const bool track_probe_matches,
                             const bool track_index_matches, const bool is_semi_or_anti_join,
                             MultiPredicateJoinEvaluator& secondary_predicate_evaluator);

  template <typename ProbeIterator>
  void _data_join_two_segments_using_index(ProbeIterator probe_iter, ProbeIterator probe_end,
                                           const ChunkID probe_chunk_id, const ChunkID index_chunk_id,
                                           const std::shared_ptr<AbstractIndex>& index);

  template <typename ProbeIterator>
  void _reference_join_two_segments_using_index(ProbeIterator probe_iter, ProbeIterator probe_end,
                                                const ChunkID probe_chunk_id, const ChunkID index_chunk_id,
                                                const std::shared_ptr<AbstractIndex>& index,
                                                const std::shared_ptr<const PosList>& reference_segment_pos_list);

  template <typename SegmentPosition>
  const std::vector<IndexRange> _index_ranges_for_value(SegmentPosition probe_side_position,
                                                        const std::shared_ptr<AbstractIndex>& index) const;

  void _append_matches(const AbstractIndex::Iterator& range_begin, const AbstractIndex::Iterator& range_end,
                       const ChunkOffset probe_chunk_offset, const ChunkID probe_chunk_id,
                       const ChunkID index_chunk_id);

  void _append_matches_dereferenced(const ChunkID& probe_chunk_id, const ChunkOffset& probe_chunk_offset,
                                    const PosList& index_table_matches);

  void _append_matches_non_inner(const bool is_semi_or_anti_join);

  void _write_output_segments(Segments& output_segments, const std::shared_ptr<const Table>& input_table,
                              std::shared_ptr<PosList> pos_list);

  void _on_cleanup() override;

  const IndexSide _index_side;
  OperatorJoinPredicate _adjusted_primary_predicate;
  std::shared_ptr<Table> _output_table;

  std::shared_ptr<const Table> _probe_input_table;
  std::shared_ptr<const Table> _index_input_table;

  std::shared_ptr<PosList> _probe_pos_list;
  std::shared_ptr<PosList> _index_pos_list;
  std::vector<bool> _index_pos_dereferenced;

  // for left/right/outer joins
  // The outer vector enumerates chunks, the inner enumerates chunk_offsets
  std::vector<std::vector<bool>> _probe_matches;
  std::vector<std::vector<bool>> _index_matches;
};

}  // namespace opossum
