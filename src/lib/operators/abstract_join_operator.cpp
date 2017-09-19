#include "abstract_join_operator.hpp"

#include <memory>
#include <string>
#include <utility>

namespace opossum {

AbstractJoinOperator::AbstractJoinOperator(const std::shared_ptr<const AbstractOperator> left,
                                           const std::shared_ptr<const AbstractOperator> right, const JoinMode mode,
                                           const std::pair<ColumnID, ColumnID> &column_ids, const ScanType scan_type)
    : AbstractReadOnlyOperator(left, right), _mode(mode), _column_ids(column_ids), _scan_type(scan_type) {
  DebugAssert(mode != JoinMode::Cross && mode != JoinMode::Natural,
              "Specified JoinMode not supported by an AbstractJoin, use Product etc. instead.");
}

JoinMode AbstractJoinOperator::mode() const { return _mode; }

const std::pair<ColumnID, ColumnID> &AbstractJoinOperator::column_ids() const { return _column_ids; }

ScanType AbstractJoinOperator::scan_type() const { return _scan_type; }

}  // namespace opossum
