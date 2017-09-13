#pragma once

#include <memory>
#include <string>
#include <vector>

#include "abstract_rule.hpp"

namespace opossum {

class AbstractASTNode;
class JoinNode;
class PredicateNode;

struct ColumnID;

/**
 * This optimizer rule tries to find join conditions for cross join.
 * The rule tries to rewrite the corresponding ASTs for the following SQL statements to something that is equivalent :
 *
 * SELECT * FROM a, b WHERE a.id = b.id;
 * =>
 * SELECT * FROM a INNER JOIN b ON a.id = b.id
 *
 * 
 */
class JoinConditionDetectionRule : AbstractRule {
 public:
  const std::shared_ptr<AbstractASTNode> apply_to(const std::shared_ptr<AbstractASTNode> node) override;

 private:
  const std::shared_ptr<PredicateNode> _find_predicate_for_cross_join(
      const std::shared_ptr<JoinNode> &cross_join);

  bool _is_join_condition(ColumnID left, ColumnID right, size_t number_columns_left);
};

}  // namespace opossum