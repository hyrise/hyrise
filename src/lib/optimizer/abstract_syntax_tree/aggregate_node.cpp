#include "aggregate_node.hpp"

#include <sstream>
#include <string>
#include <vector>

#include "common.hpp"

namespace opossum {

AggregateNode::AggregateNode(const std::vector<AggregateColumnDefinition> aggregates,
                       const std::vector<std::string> groupby_columns):
  AbstractAstNode(AstNodeType::Aggregate),
  _aggregates(aggregates),
  _groupby_columns(groupby_columns)
{}

std::string AggregateNode::description() const {
  return "Aggregate";
}

const std::vector<std::string> & AggregateNode::output_columns() const {
  return _output_columns;
}
}  // namespace opossum
