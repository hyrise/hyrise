#pragma once

#include <memory>
#include <string>
#include <vector>

#include "abstract_logical_query_plan_node.hpp"

namespace opossum {

/**
 * Node type to represent updates (i.e., invalidation and inserts) in a table.
 */
class UpdateNode : public AbstractLogicalQueryPlanNode {
 public:
  explicit UpdateNode(const std::string& table_name,
                      const std::vector<std::shared_ptr<Expression>>& column_expressions);

  std::string description() const override;

  const std::string& table_name() const;

  const std::vector<std::shared_ptr<Expression>>& column_expressions() const;

 protected:
  const std::string _table_name;
  const std::vector<std::shared_ptr<Expression>> _column_expressions;
};

}  // namespace opossum
