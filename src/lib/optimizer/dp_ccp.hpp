#pragma once

#include <memory>
#include <vector>

namespace opossum {

class AbstractExpression;
class AbstractCostModel;
class AbstractLQPNode;
class JoinGraph;

/**
 * Optimal join ordering algorithm described in https://dl.acm.org/citation.cfm?id=1164207
 */
class DpCcp final {
 public:
  explicit DpCcp(const std::shared_ptr<AbstractCostModel>& cost_model);

  std::shared_ptr<AbstractLQPNode> operator()(const JoinGraph& join_graph);

 private:
  std::shared_ptr<AbstractLQPNode> _add_predicates_to_plan(
  const std::shared_ptr<AbstractLQPNode> &lqp,
  const std::vector<std::shared_ptr<AbstractExpression>> &predicates) const;
  std::shared_ptr<AbstractLQPNode> _add_join_to_plan(const std::shared_ptr<AbstractLQPNode> &left_lqp,
                                                     const std::shared_ptr<AbstractLQPNode> &right_lqp,
                                                     std::vector<std::shared_ptr<AbstractExpression>> join_predicates) const;

  std::shared_ptr<AbstractCostModel> _cost_model;
};

}  // namespace opossum
