#include "abstract_join_operator.hpp"

#include <memory>
#include <string>
#include <utility>

#include "constant_mappings.hpp"

namespace opossum {

AbstractJoinOperator::AbstractJoinOperator(const OperatorType type, const std::shared_ptr<const AbstractOperator>& left,
                                           const std::shared_ptr<const AbstractOperator>& right, const JoinMode mode,
                                           const ColumnIDPair& primary_column_ids,
                                           const PredicateCondition primary_predicate_condition,
                                           std::vector<OperatorJoinPredicate> secondary_predicates,
                                           std::unique_ptr<OperatorPerformanceData> performance_data)
    : AbstractReadOnlyOperator(type, left, right, std::move(performance_data)),
      _mode(mode),
      _primary_column_ids(primary_column_ids),
      _primary_predicate_condition(primary_predicate_condition),
      _secondary_predicates(std::move(secondary_predicates)) {
  DebugAssert(mode != JoinMode::Cross,
              "Specified JoinMode not supported by an AbstractJoin, use Product etc. instead.");
}

JoinMode AbstractJoinOperator::mode() const { return _mode; }

const ColumnIDPair& AbstractJoinOperator::primary_column_ids() const { return _primary_column_ids; }

PredicateCondition AbstractJoinOperator::primary_predicate_condition() const { return _primary_predicate_condition; }

const std::string AbstractJoinOperator::description(DescriptionMode description_mode) const {
  std::string primary_column_name_left = std::string("Primary column #") + std::to_string(_primary_column_ids.first);
  std::string primary_column_name_right = std::string("Primary column #") + std::to_string(_primary_column_ids.second);

  if (input_table_left()) primary_column_name_left = input_table_left()->column_name(_primary_column_ids.first);
  if (input_table_right()) primary_column_name_right = input_table_right()->column_name(_primary_column_ids.second);

  const auto separator = description_mode == DescriptionMode::MultiLine ? "\n" : " ";

  std::stringstream description_stream;
  description_stream << name() << separator << "(" << join_mode_to_string.at(_mode) << " Join where "
                     << primary_column_name_left << " "
                     << predicate_condition_to_string.left.at(_primary_predicate_condition) << " "
                     << primary_column_name_right;
  // add information about secondary join predicates
  for (const auto& secondary_predicate : _secondary_predicates) {
    const auto column_name_left = input_table_left()->column_name(secondary_predicate.column_ids.first);
    const auto column_name_right = input_table_right()->column_name(secondary_predicate.column_ids.second);
    description_stream << " AND " << column_name_left << " "
                       << predicate_condition_to_string.left.at(secondary_predicate.predicate_condition) << " "
                       << column_name_right;
  }
  description_stream << ")";
  return description_stream.str();
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
  if (_mode != JoinMode::Semi && _mode != JoinMode::AntiDiscardNulls && _mode != JoinMode::AntiRetainNulls) {
    for (ColumnID column_id{0}; column_id < right_in_table->column_count(); ++column_id) {
      const auto nullable = (right_may_produce_null || right_in_table->column_is_nullable(column_id));
      output_column_definitions.emplace_back(right_in_table->column_name(column_id),
                                             right_in_table->column_data_type(column_id), nullable);
    }
  }

  return std::make_shared<Table>(output_column_definitions, TableType::References);
}

}  // namespace opossum
