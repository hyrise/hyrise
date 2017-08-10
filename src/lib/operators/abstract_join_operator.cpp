#include "abstract_join_operator.hpp"

#include <memory>
#include <string>
#include <utility>

namespace opossum {

AbstractJoinOperator::AbstractJoinOperator(const std::shared_ptr<const AbstractOperator> left,
                                           const std::shared_ptr<const AbstractOperator> right,
                                           optional<std::pair<ColumnID, ColumnID>> column_ids, const ScanType scan_type,
                                           const JoinMode mode)
    : AbstractReadOnlyOperator(left, right), _scan_type(scan_type), _mode(mode), _column_ids(column_ids) {}

ScanType AbstractJoinOperator::scan_type() const { return _scan_type; }

JoinMode AbstractJoinOperator::mode() const { return _mode; }

const std::string &AbstractJoinOperator::prefix_left() const { return _prefix_left; }

const std::string &AbstractJoinOperator::prefix_right() const { return _prefix_right; }

const optional<std::pair<std::string, std::string>> &AbstractJoinOperator::column_names() const {
  return _column_names;
}

}  // namespace opossum
