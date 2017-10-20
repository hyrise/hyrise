#include "abstract_join_operator.hpp"

#include <memory>
#include <string>
#include <utility>

#include "constant_mappings.hpp"

namespace opossum {

AbstractJoinOperator::AbstractJoinOperator(const std::shared_ptr<const AbstractOperator>& left,
                                           const std::shared_ptr<const AbstractOperator>& right, const JoinMode mode,
                                           const std::pair<ColumnID, ColumnID>& column_ids, const ScanType scan_type)
    : AbstractReadOnlyOperator(left, right), _mode(mode), _column_ids(column_ids), _scan_type(scan_type) {
  DebugAssert(mode != JoinMode::Cross && mode != JoinMode::Natural,
              "Specified JoinMode not supported by an AbstractJoin, use Product etc. instead.");
}

JoinMode AbstractJoinOperator::mode() const { return _mode; }

const std::pair<ColumnID, ColumnID>& AbstractJoinOperator::column_ids() const { return _column_ids; }

ScanType AbstractJoinOperator::scan_type() const { return _scan_type; }

const std::string AbstractJoinOperator::description() const {
  std::string column_name_left = std::string("Col #") + std::to_string(_column_ids.first);
  std::string column_name_right = std::string("Col #") + std::to_string(_column_ids.second);

  if (_input_table_left()) column_name_left = _input_table_left()->column_name(_column_ids.first);
  if (_input_table_right()) column_name_right = _input_table_right()->column_name(_column_ids.second);

  return name() + "\\n(" + join_mode_to_string.at(_mode) + " Join where " + column_name_left + " " +
         scan_type_to_string.left.at(_scan_type) + " " + column_name_right + ")";
}

}  // namespace opossum
