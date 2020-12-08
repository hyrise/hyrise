#pragma once

#include <memory>
#include <string>
#include <vector>

#include "abstract_rule.hpp"

namespace opossum {

class AbstractLQPNode;
class PredicateNode;

class NullScanRemovalRule : public AbstractRule {
 public:
  void apply_to(const std::shared_ptr<AbstractLQPNode>& root) const override;

 private:
  void _remove_nodes(const std::vector<std::shared_ptr<AbstractLQPNode>>& nodes) const;
};

}  // namespace opossum
