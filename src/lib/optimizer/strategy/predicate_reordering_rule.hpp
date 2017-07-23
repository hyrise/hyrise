#pragma once

#include <memory>
#include <vector>

#include "abstract_rule.hpp"
#include "optimizer/abstract_syntax_tree/abstract_ast_node.hpp"
#include "optimizer/abstract_syntax_tree/predicate_node.hpp"

namespace opossum {
class PredicateReorderingRule : public AbstractRule {
 public:
  std::shared_ptr<AbstractASTNode> apply_rule(std::shared_ptr<AbstractASTNode> node) override;

 private:
  static void reorder_predicates(std::vector<std::shared_ptr<PredicateNode>> &predicates);
};

}  // namespace opossum
