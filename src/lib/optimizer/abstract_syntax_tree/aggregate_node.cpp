#include "aggregate_node.hpp"

#include <sstream>
#include <string>
#include <vector>

#include "common.hpp"

namespace opossum {

AggregateNode::AggregateNode(const std::vector<std::pair<std::string, AggregateFunction>> aggregates,
                       const std::vector<std::string> groupby_columns):
  AbstractNode(NodeType::Aggregate),
  _aggregates(aggregates),
  _groupby_columns(groupby_columns)
{}

const std::string AggregateNode::description() const {
  return "Aggregate";
}

const std::vector<std::string> AggregateNode::output_columns() {
  return {};
}
}  // namespace opossum
