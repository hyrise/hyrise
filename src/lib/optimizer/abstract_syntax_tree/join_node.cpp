#include "join_node.hpp"

#include <string>
#include <utility>
#include <vector>

#include "common.hpp"

namespace opossum {

JoinNode::JoinNode(optional<std::pair<std::string, std::string>> join_column_names, const ScanType scan_type,
                   const JoinMode join_mode, const std::string &prefix_left, const std::string &prefix_right)
    : AbstractNode(NodeType::Join),
      _join_column_names(join_column_names),
      _scan_type(scan_type),
      _join_mode(join_mode),
      _prefix_left(prefix_left),
      _prefix_right(prefix_right) {}

const std::string JoinNode::description() const {
  std::ostringstream desc;

  // TODO(tim): add more details
  desc << "Join";
  if (_join_column_names) {
    desc << ": [" << (*_join_column_names).first << "-" << (*_join_column_names).second << "]";
  }

  return desc.str();
}

const std::vector<std::string> JoinNode::output_columns() {
  std::vector<std::string> output_columns;

  for (auto &column_name : left()->output_columns()) {
    output_columns.push_back(prefix_left() + column_name);
  }

  for (auto &column_name : right()->output_columns()) {
    output_columns.push_back(prefix_right() + column_name);
  }

  return output_columns;
}

optional<std::pair<std::string, std::string>> JoinNode::join_column_names() const { return _join_column_names; }

ScanType JoinNode::scan_type() const { return _scan_type; }

JoinMode JoinNode::join_mode() const { return _join_mode; }

std::string JoinNode::prefix_left() const { return _prefix_left; }

std::string JoinNode::prefix_right() const { return _prefix_right; }

}  // namespace opossum
