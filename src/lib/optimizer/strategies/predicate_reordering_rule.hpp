#pragma once

#include <memory>
#include <vector>

#include "abstract_rule.hpp"
#include "optimizer/abstract_syntax_tree/abstract_node.hpp"
#include "optimizer/abstract_syntax_tree/table_scan_node.hpp"

namespace opossum {
class PredicateReorderingRule : public AbstractRule {
 public:
  std::shared_ptr<AbstractNode> apply_rule(std::shared_ptr<AbstractNode> node) override;

 private:
  std::vector<std::shared_ptr<TableScanNode>> find_all_predicates_in_scope(std::shared_ptr<AbstractNode> node);
};

}  // namespace opossum
