#pragma once

#include <string>
#include <vector>

#include "abstract_lqp_node.hpp"
#include "lqp_column_reference.hpp"
#include "types.hpp"

namespace opossum {

/**
 * Struct to specify Order By items.
 * Order By items are defined by the column they operate on and their sort order.
 */
struct OrderByDefinition {
  OrderByDefinition(const LQPColumnReference& column_reference, const OrderByMode order_by_mode);

  LQPColumnReference column_reference;
  OrderByMode order_by_mode;
};

using OrderByDefinitions = std::vector<OrderByDefinition>;

/**
 * This node type represents sorting operations as defined in ORDER BY clauses.
 */
class SortNode : public EnableMakeForLQPNode<SortNode>, public AbstractLQPNode {
 public:
  explicit SortNode(const OrderByDefinitions& order_by_definitions);

  std::string description() const override;

  const OrderByDefinitions& order_by_definitions() const;

  bool shallow_equals(const AbstractLQPNode& rhs) const override;

 protected:
  std::shared_ptr<AbstractLQPNode> _deep_copy_impl(
      const std::shared_ptr<AbstractLQPNode>& copied_left_input,
      const std::shared_ptr<AbstractLQPNode>& copied_right_input) const override;

 private:
  const OrderByDefinitions _order_by_definitions;
};

}  // namespace opossum
