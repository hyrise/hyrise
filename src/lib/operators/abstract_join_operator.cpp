#include "abstract_join_operator.hpp"

#include <memory>
#include <string>
#include <utility>

namespace opossum {

AbstractJoinOperator::AbstractJoinOperator(const std::shared_ptr<const AbstractOperator> left,
                                           const std::shared_ptr<const AbstractOperator> right,
                                           optional<std::pair<std::string, std::string>> column_names,
                                           const ScanType scan_type, const JoinMode mode,
                                           const std::string &prefix_left, const std::string &prefix_right)
    : AbstractReadOnlyOperator(left, right),
      _scan_type(scan_type),
      _mode(mode),
      _prefix_left(prefix_left),
      _prefix_right(prefix_right),
      _column_names(column_names) {}

ScanType AbstractJoinOperator::scan_type() const { return _scan_type; }

JoinMode AbstractJoinOperator::mode() const { return _mode; }

const std::string &AbstractJoinOperator::prefix_left() const { return _prefix_left; }

const std::string &AbstractJoinOperator::prefix_right() const { return _prefix_right; }

const optional<std::pair<std::string, std::string>> &AbstractJoinOperator::column_names() const {
  return _column_names;
}

}  // namespace opossum
