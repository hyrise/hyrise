#include "join_node.hpp"

#include <string>

#include "common.hpp"

namespace opossum {

JoinNode::JoinNode(optional<std::pair<std::string, std::string>> column_names, const ScanType scan_type,
                   const JoinMode join_mode, const std::string &prefix_left, const std::string &prefix_right)
    : AbstractNode(NodeType::Join),
      _column_names(column_names),
      _scan_type(scan_type),
      _join_mode(join_mode),
      _prefix_left(prefix_left),
      _prefix_right(prefix_right) {}

const std::string JoinNode::description() const {
  std::ostringstream desc;

  // TODO(tim): add more details
  desc << "Join: [" << _column_names->first << "-" << _column_names->second << "]";

  return desc.str();
}

optional<std::pair<std::string, std::string>> JoinNode::column_names() const { return _column_names; }

ScanType JoinNode::scan_type() const { return _scan_type; }

JoinMode JoinNode::join_mode() const { return _join_mode; }

std::string JoinNode::prefix_left() const { return _prefix_left; }

std::string JoinNode::prefix_right() const { return _prefix_right; }

}  // namespace opossum
