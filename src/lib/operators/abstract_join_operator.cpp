#include "abstract_join_operator.hpp"

#include <memory>
#include <string>
#include <utility>

#include "constant_mappings.hpp"

using namespace std::string_literals;  // NOLINT

namespace opossum {

AbstractJoinOperator::AbstractJoinOperator(const OperatorType type, const std::shared_ptr<const AbstractOperator>& left,
                                           const std::shared_ptr<const AbstractOperator>& right, const JoinMode mode,
                                           const OperatorJoinPredicate& primary_predicate,
                                           const std::vector<OperatorJoinPredicate>& secondary_predicates,
                                           std::unique_ptr<OperatorPerformanceData> performance_data)
    : AbstractReadOnlyOperator(type, left, right, std::move(performance_data)),
      _mode(mode),
      _primary_predicate(primary_predicate),
      _secondary_predicates(secondary_predicates) {
  DebugAssert(mode != JoinMode::Cross,
              "Specified JoinMode not supported by an AbstractJoin, use Product etc. instead.");
}

JoinMode AbstractJoinOperator::mode() const { return _mode; }

const OperatorJoinPredicate& AbstractJoinOperator::primary_predicate() const { return _primary_predicate; }

const std::vector<OperatorJoinPredicate>& AbstractJoinOperator::secondary_predicates() const {
  return _secondary_predicates;
}

const std::string AbstractJoinOperator::description(DescriptionMode description_mode) const {
  const auto column_name = [](const auto& table, const auto column_id) {
    return table ? table->column_name(column_id) : "Column #"s + std::to_string(column_id);
  };

  const auto separator = description_mode == DescriptionMode::MultiLine ? "\n" : " ";

  std::stringstream stream;
  stream << name() << separator << "(" << _mode << " Join where "
         << column_name(input_table_left(), _primary_predicate.column_ids.first) << " "
         << _primary_predicate.predicate_condition << " "
         << column_name(input_table_right(), _primary_predicate.column_ids.second);

  // add information about secondary join predicates
  for (const auto& secondary_predicate : _secondary_predicates) {
    stream << " AND " << column_name(input_table_left(), secondary_predicate.column_ids.first) << " "
           << secondary_predicate.predicate_condition << " "
           << column_name(input_table_right(), secondary_predicate.column_ids.second);
  }

  stream << ")";

  return stream.str();
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
  if (_mode != JoinMode::Semi && _mode != JoinMode::AntiNullAsTrue && _mode != JoinMode::AntiNullAsFalse) {
    for (ColumnID column_id{0}; column_id < right_in_table->column_count(); ++column_id) {
      const auto nullable = (right_may_produce_null || right_in_table->column_is_nullable(column_id));
      output_column_definitions.emplace_back(right_in_table->column_name(column_id),
                                             right_in_table->column_data_type(column_id), nullable);
    }
  }

  return std::make_shared<Table>(output_column_definitions, TableType::References);
}

}  // namespace opossum
