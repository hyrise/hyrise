#pragma once

#include <memory>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "abstract_join_operator.hpp"
#include "multi_predicate_join/multi_predicate_join_evaluator.hpp"
#include "storage/pos_list.hpp"
#include "types.hpp"

namespace opossum {

class JoinIndex;

class JoinNestedLoop : public AbstractJoinOperator {
 public:
  static bool supports(const JoinConfiguration config);

  JoinNestedLoop(const std::shared_ptr<const AbstractOperator>& left,
                 const std::shared_ptr<const AbstractOperator>& right, const JoinMode mode,
                 const OperatorJoinPredicate& primary_predicate,
                 const std::vector<OperatorJoinPredicate>& secondary_predicates = {});

  const std::string name() const override;

  struct JoinParams {
    PosList& pos_list_left;
    PosList& pos_list_right;
    std::vector<bool>& left_matches;
    std::vector<bool>& right_matches;
    bool track_left_matches{};
    bool track_right_matches{};
    JoinMode mode;
    PredicateCondition predicate_condition;
    MultiPredicateJoinEvaluator& secondary_predicate_evaluator;

    // Disable for Semi/Anti
    bool write_pos_lists{};
  };

 protected:
  std::shared_ptr<AbstractOperator> _on_deep_copy(
      const std::shared_ptr<AbstractOperator>& copied_input_left,
      const std::shared_ptr<AbstractOperator>& copied_input_right) const override;
  void _on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) override;

  std::shared_ptr<const Table> _on_execute() override;

  // Having all these static methods and passing around the state of the JoinNestedLoop is somewhat ugly, but it allows
  // us to reuse the code in JoinIndex as a fallback. __attribute__((noinline)) is simply magic - otherwise the
  // compiler would try to put the entire join for all types into a single, monolithic function. For -O3 on clang, this
  // reduces the compile time to a fourth.

  static void __attribute__((noinline))
  _join_two_untyped_segments(const BaseSegment& base_segment_left, const BaseSegment& base_segment_right,
                             const ChunkID chunk_id_left, const ChunkID chunk_id_right, JoinParams& params);

  void _write_output_chunk(Segments& segments, const std::shared_ptr<const Table>& input_table,
                           const std::shared_ptr<PosList>& pos_list);

  // The JoinIndex uses this join as a fallback if no index exists
  friend class JoinIndex;
};

}  // namespace opossum
