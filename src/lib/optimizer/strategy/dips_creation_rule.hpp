#pragma once

#include <memory>
#include <set>
#include <string>
#include <vector>

#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"

#include "abstract_rule.hpp"


namespace opossum {

  class AbstractLQPNode;


  /**
  * This rule determines which chunks can be pruned from table scans based on
  * the predicates present in the LQP and stores that information in the stored
  * table nodes.
  */
  class DipsCreationRule : public AbstractRule {
  public:
    void apply_to(const std::shared_ptr<AbstractLQPNode>& node) const override;

  protected:
    std::vector<std::shared_ptr<PredicateNode>> get_pruned_attribute_statistics(const std::shared_ptr<const StoredTableNode> table_node, ColumnID column_id, std::shared_ptr<LQPColumnExpression> join_partner) const;
  };

}  // namespace opossum
