#pragma once

#include <memory>
#include <string>

#include "abstract_rule.hpp"

namespace hyrise {

class AbstractLQPNode;

/**
 * TODO
 */
class ColumnVsColumnScanRemovalRule : public AbstractRule {
 public:
  std::string name() const override;

 protected:
  void _apply_to_plan_without_subqueries(const std::shared_ptr<AbstractLQPNode>& lqp_root) const override;
};

}  // namespace hyrise
