#pragma once

#include <memory>
#include <set>
#include <string>
#include <vector>

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

  };

}  // namespace opossum
