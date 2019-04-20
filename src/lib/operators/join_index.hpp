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
/**
   * This operator joins two tables using one column of each table.
   * A speedup compared to the Nested Loop Join is achieved by avoiding the inner loop, and instead
   * finding the right values utilizing the index.
   *
   * Note: An index needs to be present on the right table in order to execute an index join.
   */
class JoinIndex : public AbstractJoinOperator {
 public:
  JoinIndex(const std::shared_ptr<const AbstractOperator>& left, const std::shared_ptr<const AbstractOperator>& right,
            const JoinMode mode, const OperatorJoinPredicate& primary_predicate);

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

  void _perform_join();

  template <typename LeftIterator>
  void _join_two_segments_using_index(LeftIterator left_it, LeftIterator left_end, const ChunkID chunk_id_left,
                                      const ChunkID chunk_id_right, const std::shared_ptr<BaseIndex>& index);

  template <typename BinaryFunctor, typename LeftIterator, typename RightIterator>
  void _join_two_segments_nested_loop(const BinaryFunctor& func, LeftIterator left_it, LeftIterator left_end,
                                      RightIterator right_begin, RightIterator right_end, const ChunkID chunk_id_left,
                                      const ChunkID chunk_id_right);

  void _append_matches(const BaseIndex::Iterator& range_begin, const BaseIndex::Iterator& range_end,
                       const ChunkOffset chunk_offset_left, const ChunkID chunk_id_left, const ChunkID chunk_id_right);

  void _write_output_segments(Segments& output_segments, const std::shared_ptr<const Table>& input_table,
                              std::shared_ptr<PosList> pos_list);

  void _on_cleanup() override;

  std::shared_ptr<Table> _output_table;

  std::shared_ptr<PosList> _pos_list_left;
  std::shared_ptr<PosList> _pos_list_right;

  // for left/right/outer joins
  // The outer vector enumerates chunks, the inner enumerates chunk_offsets
  std::vector<std::vector<bool>> _left_matches;
  std::vector<std::vector<bool>> _right_matches;
};

}  // namespace opossum
