#pragma once

#include <memory>
#include <string>
#include <vector>

#include "abstract_rule.hpp"
#include "optimizer/abstract_syntax_tree/abstract_ast_node.hpp"

namespace opossum {

class Expression;
class PredicateNode;
class StoredTableNode;

class JoinConditionDetectionRule : AbstractRule {
 public:
  const std::shared_ptr<AbstractASTNode> apply_to(const std::shared_ptr<AbstractASTNode> node) override;

 private:
  const std::shared_ptr<PredicateNode> _find_predicate_for_cross_join(
      const std::shared_ptr<AbstractASTNode> &cross_join);
};

}  // namespace opossum