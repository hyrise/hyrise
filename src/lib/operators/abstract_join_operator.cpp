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

}  // namespace opossum
