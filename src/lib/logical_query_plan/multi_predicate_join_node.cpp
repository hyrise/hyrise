#include "multi_predicate_join_node.hpp"

#include <vector>

#include "abstract_lqp_node.hpp"
#include "types.hpp"

namespace opossum {

MultiPredicateJoinNode::MultiPredicateJoinNode(const JoinMode join_mode, JoinPredicate primary_join_predicate,
    std::vector<JoinPredicate> additional_predicates)
    : AbstractLQPNode(LQPNodeType::MultiPredicateJoin),
      join_mode(join_mode),
      primary_join_predicate{std::move(primary_join_predicate)},
      additional_predicates{std::move(additional_predicates)} {
  Assert(join_mode != JoinMode::Cross, "MultiPredicateJoins cannot be cross joins.");
  Assert(!additional_predicates.empty(), "Use standard JoinNode if there is only one join predicate.");
}

std::shared_ptr<AbstractLQPNode> MultiPredicateJoinNode::_on_shallow_copy(LQPNodeMapping& node_mapping) const {
  return MultiPredicateJoinNode::make(join_mode, primary_join_predicate, additional_predicates);
}

bool MultiPredicateJoinNode::_on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const {
  const auto& join_node = static_cast<const MultiPredicateJoinNode&>(rhs);

  if (join_mode != join_node.join_mode) return false;
  if (primary_join_predicate.predicate_condition !=  join_node.primary_join_predicate.predicate_condition) return false;
  if (primary_join_predicate.column_id_pair !=  join_node.primary_join_predicate.column_id_pair) return false;

  // WIP: Write equals method for JoinPredicate
  for (auto predicate_index {0u}; predicate_index < additional_predicates.size(); ++predicate_index) {
    const auto& pred_a = additional_predicates[predicate_index];
    const auto& pred_b = join_node.additional_predicates[predicate_index];

    if (pred_a.predicate_condition != pred_b.predicate_condition) return false;
    if (pred_a.column_id_pair != pred_b.column_id_pair) return false;
  }

  return true;
}

}   // namespace opossum