#include "abstract_join_operator.hpp"

#include <memory>
#include <string>
#include <utility>

#include "constant_mappings.hpp"

namespace opossum {

AbstractJoinOperator::AbstractJoinOperator(const OperatorType type, const std::shared_ptr<const AbstractOperator>& left,
                                           const std::shared_ptr<const AbstractOperator>& right, const JoinMode mode,
                                           const ColumnIDPair& column_ids, const PredicateCondition predicate_condition,
                                           std::unique_ptr<OperatorPerformanceData> performance_data)
    : AbstractReadOnlyOperator(type, left, right, std::move(performance_data)),
      _mode(mode),
      _column_ids(column_ids),
      _predicate_condition(predicate_condition) {
  DebugAssert(mode != JoinMode::Cross,
              "Specified JoinMode not supported by an AbstractJoin, use Product etc. instead.");
}

JoinMode AbstractJoinOperator::mode() const { return _mode; }

const ColumnIDPair& AbstractJoinOperator::column_ids() const { return _column_ids; }

PredicateCondition AbstractJoinOperator::predicate_condition() const { return _predicate_condition; }

const std::string AbstractJoinOperator::description(DescriptionMode description_mode) const {
  std::string column_name_left = std::string("Column #") + std::to_string(_column_ids.first);
  std::string column_name_right = std::string("Column #") + std::to_string(_column_ids.second);

  if (input_table_left()) column_name_left = input_table_left()->column_name(_column_ids.first);
  if (input_table_right()) column_name_right = input_table_right()->column_name(_column_ids.second);

  const auto separator = description_mode == DescriptionMode::MultiLine ? "\n" : " ";

  return name() + separator + "(" + join_mode_to_string.at(_mode) + " Join where " + column_name_left + " " +
         predicate_condition_to_string.left.at(_predicate_condition) + " " + column_name_right + ")";
}

void AbstractJoinOperator::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {}

std::shared_ptr<Table> AbstractJoinOperator::_initialize_output_table() const {
  const auto left_in_table = _input_left->get_output();
  const auto right_in_table = _input_right->get_output();

  const bool left_may_produce_null = (_mode == JoinMode::Right || _mode == JoinMode::FullOuter);
  const bool right_may_produce_null = (_mode == JoinMode::Left || _mode == JoinMode::FullOuter);

  TableColumnDefinitions output_column_definitions;

  // Preparing output table by adding segments from left table
  for (ColumnID column_id{0}; column_id < left_in_table->column_count(); ++column_id) {
    const auto nullable = (left_may_produce_null || left_in_table->column_is_nullable(column_id));
    output_column_definitions.emplace_back(left_in_table->column_name(column_id),
                                           left_in_table->column_data_type(column_id), nullable);
  }

  // Preparing output table by adding segments from right table
  if (_mode != JoinMode::Semi && _mode != JoinMode::Anti) {
    for (ColumnID column_id{0}; column_id < right_in_table->column_count(); ++column_id) {
      const auto nullable = (right_may_produce_null || right_in_table->column_is_nullable(column_id));
      output_column_definitions.emplace_back(right_in_table->column_name(column_id),
                                             right_in_table->column_data_type(column_id), nullable);
    }
  }

  return std::make_shared<Table>(output_column_definitions, TableType::References);
}

}  // namespace opossum
