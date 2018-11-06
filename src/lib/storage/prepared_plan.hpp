#pragma once

#include <iostream>
#include <memory>
#include <vector>

#include "expression/parameter_expression.hpp"

namespace opossum {

class AbstractLQPNode;

/**
 * Representing a prepared SQL statement, with the ParameterIDs to be used for the arguments to the prepared statement.
 */
class PreparedPlan final {
 public:
  PreparedPlan(const std::shared_ptr<AbstractLQPNode>& lqp, const std::vector<ParameterID>& parameter_ids);

  std::shared_ptr<PreparedPlan> deep_copy() const;

  void print(std::ostream& stream) const;

  bool operator==(const PreparedPlan& rhs) const;

  std::shared_ptr<AbstractLQPNode> lqp;
  std::vector<ParameterID> parameter_ids;
};

}  // namespace opossum
