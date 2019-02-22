#pragma once

#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "types.hpp"

#include "abstract_lqp_node.hpp"
#include "expression/abstract_expression.hpp"
#include "lqp_column_reference.hpp"

namespace opossum {

class MultiPredicateJoinNode : public EnableMakeForLQPNode<MultiPredicateJoinNode>, public AbstractLQPNode {
 public:
  // Constructor for predicated joins
  explicit MultiPredicateJoinNode(const JoinMode join_mode, JoinPredicate primary_join_predicate,
      std::vector<JoinPredicate> additional_predicates);

  std::string description() const override;

  const JoinMode join_mode;
  const JoinPredicate primary_join_predicate;
  const std::vector<JoinPredicate> additional_predicates;

 protected:
  std::shared_ptr<AbstractLQPNode> _on_shallow_copy(LQPNodeMapping& node_mapping) const override;
  bool _on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const override;
};

}  // namespace opossum
