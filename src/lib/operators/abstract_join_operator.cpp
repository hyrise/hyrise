#include "abstract_join_operator.hpp"

#include <memory>
#include <string>
#include <utility>

#include "constant_mappings.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/join_node.hpp"

using namespace std::string_literals;  // NOLINT

namespace hyrise {

AbstractJoinOperator::AbstractJoinOperator(const OperatorType type, const std::shared_ptr<const AbstractOperator>& left,
                                           const std::shared_ptr<const AbstractOperator>& right, const JoinMode mode,
                                           const OperatorJoinPredicate& primary_predicate,
                                           const std::vector<OperatorJoinPredicate>& secondary_predicates,
                                           std::unique_ptr<AbstractOperatorPerformanceData> init_performance_data)
    : AbstractReadOnlyOperator(type, left, right, std::move(init_performance_data)),
      _mode(mode),
      _primary_predicate(primary_predicate),
      _secondary_predicates(secondary_predicates) {
  Assert(mode != JoinMode::Cross, "Specified JoinMode not supported by an AbstractJoin, use Product etc. instead.");
  Assert(primary_predicate.predicate_condition == PredicateCondition::Equals ||
             primary_predicate.predicate_condition == PredicateCondition::LessThan ||
             primary_predicate.predicate_condition == PredicateCondition::GreaterThan ||
             primary_predicate.predicate_condition == PredicateCondition::LessThanEquals ||
             primary_predicate.predicate_condition == PredicateCondition::GreaterThanEquals ||
             primary_predicate.predicate_condition == PredicateCondition::NotEquals,
         "Unsupported predicate condition");
}

JoinMode AbstractJoinOperator::mode() const {
  return _mode;
}

const OperatorJoinPredicate& AbstractJoinOperator::primary_predicate() const {
  return _primary_predicate;
}

const std::vector<OperatorJoinPredicate>& AbstractJoinOperator::secondary_predicates() const {
  return _secondary_predicates;
}

std::string AbstractJoinOperator::description(DescriptionMode description_mode) const {
  const auto column_name = [&](const auto from_left, const auto column_id) {
    const auto state = from_left ? _left_input->state() : _right_input->state();
    if (state == OperatorState::ExecutedAndAvailable) {
      const auto& input_table = from_left ? _left_input->get_output() : _right_input->get_output();
      // If input table is still available, use name from there
      if (input_table) {
        return input_table->column_name(column_id);
      }
    }

    if (lqp_node) {
      // LQP is available, use column name from there
      const auto& input_lqp_node = lqp_node->input(from_left ? LQPInputSide::Left : LQPInputSide::Right);
      return input_lqp_node->output_expressions()[column_id]->as_column_name();
    }

    // Fallback - use column ID
    return "Column #"s + std::to_string(column_id);
  };

  const auto separator = (description_mode == DescriptionMode::SingleLine ? ' ' : '\n');
  std::stringstream stream;
  stream << AbstractOperator::description(description_mode);
  if (_mode == JoinMode::Semi && lqp_node && std::static_pointer_cast<const JoinNode>(lqp_node)->is_semi_reduction()) {
    stream << " (Semi Reduction)" << separator;
  } else {
    stream << " (" << _mode << ")" << separator;
  }
  stream << column_name(true, _primary_predicate.column_ids.first) << " ";
  stream << _primary_predicate.predicate_condition << " ";
  stream << column_name(false, _primary_predicate.column_ids.second);

  // Add information about secondary join predicates
  for (const auto& secondary_predicate : _secondary_predicates) {
    stream << separator << "AND ";
    stream << column_name(true, secondary_predicate.column_ids.first) << " ";
    stream << secondary_predicate.predicate_condition << " ";
    stream << column_name(false, secondary_predicate.column_ids.second);
  }

  return stream.str();
}

void AbstractJoinOperator::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {}

std::shared_ptr<Table> AbstractJoinOperator::_build_output_table(std::vector<std::shared_ptr<Chunk>>&& chunks,
                                                                 const TableType table_type) const {
  const auto left_in_table = _left_input->get_output();
  const auto right_in_table = _right_input->get_output();

  const bool left_may_produce_null = (_mode == JoinMode::Right || _mode == JoinMode::FullOuter);
  const bool right_may_produce_null = (_mode == JoinMode::Left || _mode == JoinMode::FullOuter);

  TableColumnDefinitions output_column_definitions;

  // Preparing output table by adding segments from left table
  for (auto column_id = ColumnID{0}; column_id < left_in_table->column_count(); ++column_id) {
    const auto nullable = (left_may_produce_null || left_in_table->column_is_nullable(column_id));
    output_column_definitions.emplace_back(left_in_table->column_name(column_id),
                                           left_in_table->column_data_type(column_id), nullable);
  }

  // Preparing output table by adding segments from right table
  if (_mode != JoinMode::Semi && _mode != JoinMode::AntiNullAsTrue && _mode != JoinMode::AntiNullAsFalse) {
    for (auto column_id = ColumnID{0}; column_id < right_in_table->column_count(); ++column_id) {
      const auto nullable = (right_may_produce_null || right_in_table->column_is_nullable(column_id));
      output_column_definitions.emplace_back(right_in_table->column_name(column_id),
                                             right_in_table->column_data_type(column_id), nullable);
    }
  }

  return std::make_shared<Table>(output_column_definitions, table_type, std::move(chunks));
}

}  // namespace hyrise
