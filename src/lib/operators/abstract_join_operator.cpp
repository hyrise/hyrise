#include "abstract_join_operator.hpp"

#include <memory>
#include <string>
#include <utility>

namespace opossum {

AbstractJoinOperator::AbstractJoinOperator(const std::shared_ptr<const AbstractOperator> left,
                                           const std::shared_ptr<const AbstractOperator> right, const JoinMode mode)
    : AbstractReadOnlyOperator(left, right), _mode(mode), _column_ids(nullopt), _scan_type(nullopt) {
  DebugAssert(mode == JoinMode::Cross || mode == JoinMode::Natural,
              "Specified JoinMode must also specify column ids and scan type.");
}

AbstractJoinOperator::AbstractJoinOperator(const std::shared_ptr<const AbstractOperator> left,
                                           const std::shared_ptr<const AbstractOperator> right, const JoinMode mode,
                                           const std::pair<ColumnID, ColumnID> &column_ids, const ScanType scan_type)
    : AbstractReadOnlyOperator(left, right), _mode(mode), _column_ids(column_ids), _scan_type(scan_type) {
  DebugAssert(mode != JoinMode::Cross && mode != JoinMode::Natural,
              "Specified JoinMode must specify neither column ids nor scan type.");
}

JoinMode AbstractJoinOperator::mode() const { return _mode; }

const optional<std::pair<ColumnID, ColumnID>> &AbstractJoinOperator::column_ids() const { return _column_ids; }

const optional<ScanType> &AbstractJoinOperator::scan_type() const { return _scan_type; }

}  // namespace opossum
