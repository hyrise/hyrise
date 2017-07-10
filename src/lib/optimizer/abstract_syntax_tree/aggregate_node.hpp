#pragma once

#include <string>
#include <vector>

#include "common.hpp"
#include "optimizer/abstract_syntax_tree/abstract_node.hpp"
#include "optimizer/abstract_syntax_tree/expression_node.hpp"
#include "types.hpp"

namespace opossum {

struct AggregateColumnDefinition
{
  std::string alias;
  std::shared_ptr<ExpressionNode> expr;
};

class AggregateNode : public AbstractNode {
 public:
  explicit AggregateNode(const std::vector<AggregateColumnDefinition> aggregates,
                         const std::vector<std::string> groupby_columns);

  const std::string description() const override;

  const std::vector<std::string> output_columns() override;

 private:
  std::vector<AggregateColumnDefinition> _aggregates;
  std::vector<std::string> _groupby_columns;
};

class AggregateNode::

}  // namespace opossum
