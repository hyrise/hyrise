#include "join_node.hpp"

#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "common.hpp"
#include "constant_mappings.hpp"

namespace opossum {

JoinNode::JoinNode(const JoinMode join_mode, const std::string &prefix_left, const std::string &prefix_right,
                   optional<std::pair<std::string, std::string>> join_column_names, optional<ScanType> scan_type)
    : AbstractASTNode(ASTNodeType::Join),
      _join_column_names(join_column_names),
      _scan_type(scan_type),
      _join_mode(join_mode),
      _prefix_left(prefix_left),
      _prefix_right(prefix_right) {}

std::string JoinNode::description() const {
  std::ostringstream desc;

  desc << "Join";
  desc << " [" << join_mode_to_string.at(_join_mode) << "]";

  if (_join_column_names && _scan_type) {
    desc << " [" << (*_join_column_names).first;
    desc << " " << scan_type_to_string.left.at(*_scan_type);
    desc << " " << (*_join_column_names).second << "]";
  }

  desc << " [" << _prefix_left << " / " << _prefix_right << "]";

  return desc.str();
}

std::vector<std::string> JoinNode::output_column_names() const {
  /**
   * Add respective prefix to column names.
   */
  std::vector<std::string> output_column_names;

  for (auto &column_name : left_child()->output_column_names()) {
    output_column_names.push_back(prefix_left() + column_name);
  }

  for (auto &column_name : right_child()->output_column_names()) {
    output_column_names.push_back(prefix_right() + column_name);
  }

  return output_column_names;
}

optional<std::pair<std::string, std::string>> JoinNode::join_column_names() const { return _join_column_names; }

optional<ScanType> JoinNode::scan_type() const { return _scan_type; }

JoinMode JoinNode::join_mode() const { return _join_mode; }

const std::string &JoinNode::prefix_left() const { return _prefix_left; }

const std::string &JoinNode::prefix_right() const { return _prefix_right; }

}  // namespace opossum
