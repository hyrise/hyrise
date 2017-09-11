#pragma once

#include <memory>
#include <string>
#include <vector>

#include "abstract_rule.hpp"
#include "optimizer/abstract_syntax_tree/abstract_ast_node.hpp"

namespace opossum {

class ExpressionNode;
class PredicateNode;
class StoredTableNode;

class JoinConditionDetectionRule : AbstractRule {
 public:
  const std::shared_ptr<AbstractASTNode> apply_to(const std::shared_ptr<AbstractASTNode> node) override;

 private:
  const std::shared_ptr<ExpressionNode> _find_predicate_for_cross_join(
      const std::shared_ptr<AbstractASTNode> &cross_join);

  const std::shared_ptr<ExpressionNode> _find_join_condition(const std::shared_ptr<StoredTableNode> &left_table,
                                                             const std::shared_ptr<StoredTableNode> &right_table,
                                                             const std::shared_ptr<PredicateNode> &predicate,
                                                             const std::shared_ptr<ExpressionNode> &expression);

  bool _is_join_condition(const std::string &expected_left_table_name, const std::string &expected_right_table_name,
                          const std::string &actual_left_table_name, const std::string &actual_right_table_name);
};

}  // namespace opossum