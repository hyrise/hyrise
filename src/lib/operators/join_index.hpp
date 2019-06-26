#pragma once

#include <memory>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "abstract_join_operator.hpp"
#include "storage/index/base_index.hpp"
#include "storage/pos_list.hpp"
#include "types.hpp"

namespace opossum {

enum class IndexSide { Left, Right };

/**
   * This operator joins two tables using one column of each table.
   * A speedup compared to the Nested Loop Join is achieved by avoiding the inner loop, and instead
   * finding the index side values utilizing the index.
   *
   * Note: An index needs to be present on the index side table in order to execute an index join.
   */
class JoinIndex : public AbstractJoinOperator {
 public:
  static bool supports(JoinMode join_mode, PredicateCondition predicate_condition, DataType left_data_type,
                       DataType right_data_type, bool secondary_predicates);

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

  void _append_matches(const ChunkID& probe_chunk_id, const ChunkOffset& probe_chunk_offset,
                       const PosList& index_table_matches);

  template <typename ProbeIterator>
  void _join_two_segments_using_index(ProbeIterator probe_iter, ProbeIterator probe_end, const ChunkID probe_chunk_id,
                                      const ChunkID index_chunk_id, const std::shared_ptr<BaseIndex>& index);

  void _append_matches(const BaseIndex::Iterator& range_begin, const BaseIndex::Iterator& range_end,
                       const ChunkOffset probe_chunk_offset, const ChunkID probe_chunk_id,
                       const ChunkID index_chunk_id);

  void _write_output_segments(Segments& output_segments, const std::shared_ptr<const Table>& input_table,
                              std::shared_ptr<PosList> pos_list);

  void _on_cleanup() override;

  const IndexSide _index_side;
  OperatorJoinPredicate _adjusted_primary_predicate;
  std::shared_ptr<Table> _output_table;

  std::shared_ptr<PosList> _probe_pos_list;
  std::shared_ptr<PosList> _index_pos_list;

  // for left/right/outer joins
  // The outer vector enumerates chunks, the inner enumerates chunk_offsets
  std::vector<std::vector<bool>> _probe_matches;
  std::vector<std::vector<bool>> _index_matches;
};

}  // namespace opossum
