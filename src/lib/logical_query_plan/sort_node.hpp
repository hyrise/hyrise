#pragma once

#include <string>
#include <vector>

#include "abstract_lqp_node.hpp"
#include "column_origin.hpp"
#include "types.hpp"

namespace opossum {

/**
 * Struct to specify Order By items.
 * Order By items are defined by the column they operate on and their sort order.
 */
struct OrderByDefinition {
  OrderByDefinition(const ColumnOrigin& column_origin, const OrderByMode order_by_mode);

  ColumnOrigin column_origin;
  OrderByMode order_by_mode;
};

/**
 * This node type represents sorting operations as defined in ORDER BY clauses.
 */
class SortNode : public AbstractLQPNode {
 public:
  explicit SortNode(const std::vector<OrderByDefinition>& order_by_definitions);

  std::string description() const override;

  const std::vector<OrderByDefinition>& order_by_definitions() const;

 private:
  const std::vector<OrderByDefinition> _order_by_definitions;
};

}  // namespace opossum
