#pragma once

#include <memory>
#include <string>
#include <vector>

#include "abstract_rule.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/join_node.hpp"

namespace opossum {

class AbstractLQPNode;
class PredicateNode;


class JoinToPredicateRewriteRule : public AbstractRule {
 public:
  std::string name() const override;

 protected:
  void _apply_to_plan_without_subqueries(const std::shared_ptr<AbstractLQPNode>& lqp_root) const override;
  bool _check_rewrite_validity(const std::shared_ptr<JoinNode>& join_node, const std::shared_ptr<LQPInputSide> removable_side, std::shared_ptr<PredicateNode>& valid_predicate) const;
  void _perform_rewrite(const std::shared_ptr<JoinNode>& join_node, const std::shared_ptr<LQPInputSide> removable_side, const std::shared_ptr<PredicateNode>& valid_predicate) const;
};

}  // namespace opossum
