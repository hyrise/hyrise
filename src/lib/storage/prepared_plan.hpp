#pragma once

#include <iostream>
#include <memory>
#include <vector>

#include "types.hpp"

namespace opossum {

class AbstractLQPNode;
class AbstractExpression;

/**
 * Representing a prepared SQL statement, with the ParameterIDs to be used for the arguments to the prepared statement.
 */
class PreparedPlan final {
 public:
  PreparedPlan(const std::shared_ptr<AbstractLQPNode>& lqp, const std::vector<ParameterID>& parameter_ids);

  std::shared_ptr<PreparedPlan> deep_copy() const;

  /**
   * @return A copy of the prepared plan, with the specified @param parameters filled into the placeholders
   */
  std::shared_ptr<AbstractLQPNode> instantiate(
      const std::vector<std::shared_ptr<AbstractExpression>>& parameters) const;

  bool operator==(const PreparedPlan& rhs) const;

  std::shared_ptr<AbstractLQPNode> lqp;
  std::vector<ParameterID> parameter_ids;
};

std::ostream& operator<<(std::ostream& stream, const PreparedPlan& prepared_plan);

}  // namespace opossum
