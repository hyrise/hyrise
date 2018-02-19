#include "abstract_join_operator.hpp"

#include <memory>
#include <string>
#include <utility>

#include "constant_mappings.hpp"

namespace opossum {

AbstractJoinOperator::AbstractJoinOperator(const std::shared_ptr<const AbstractOperator> left,
                                           const std::shared_ptr<const AbstractOperator> right, const JoinMode mode,
                                           const ColumnIDPair& column_ids, const PredicateCondition predicate_condition)
    : AbstractReadOnlyOperator(left, right),
      _mode(mode),
      _column_ids(column_ids),
      _predicate_condition(predicate_condition) {
  DebugAssert(mode != JoinMode::Cross && mode != JoinMode::Natural,
              "Specified JoinMode not supported by an AbstractJoin, use Product etc. instead.");
}

JoinMode AbstractJoinOperator::mode() const { return _mode; }

const ColumnIDPair& AbstractJoinOperator::column_ids() const { return _column_ids; }

PredicateCondition AbstractJoinOperator::predicate_condition() const { return _predicate_condition; }

const std::string AbstractJoinOperator::description(DescriptionMode description_mode) const {
  std::string column_name_left = std::string("Col #") + std::to_string(_column_ids.first);
  std::string column_name_right = std::string("Col #") + std::to_string(_column_ids.second);

  if (_input_table_left()) column_name_left = _input_table_left()->column_name(_column_ids.first);
  if (_input_table_right()) column_name_right = _input_table_right()->column_name(_column_ids.second);

  const auto separator = description_mode == DescriptionMode::MultiLine ? "\n" : " ";

  return name() + separator + "(" + join_mode_to_string.at(_mode) + " Join where " + column_name_left + " " +
         predicate_condition_to_string.left.at(_predicate_condition) + " " + column_name_right + ")";
}

}  // namespace opossum
