#pragma once

#include <memory>
#include <vector>

#include "expression/parameter_expression.hpp"

namespace opossum {

/**
 * Representing a prepared SQL statement, with the ParameterIDs to be used for the arguments to the prepared statement.
 */
class LQPPreparedStatement final {
 public:
  LQPPreparedStatement(const std::shared_ptr<AbstractLQPNode>& lqp, const std::vector<ParameterID>& parameter_ids);

  std::shared_ptr<LQPPreparedStatement> deep_copy() const;

  std::shared_ptr<AbstractLQPNode> lqp;
  std::vector<ParameterID> parameter_ids;
};

}  // namespace opossum
