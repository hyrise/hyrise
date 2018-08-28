#include "abstract_join_operator.hpp"

#include <memory>
#include <string>
#include <utility>

#include "constant_mappings.hpp"

namespace opossum {

AbstractJoinOperator::AbstractJoinOperator(const OperatorType type, const std::shared_ptr<const AbstractOperator>& left,
                                           const std::shared_ptr<const AbstractOperator>& right, const JoinMode mode,
                                           const CxlumnIDPair& cxlumn_ids, const PredicateCondition predicate_condition,
                                           std::unique_ptr<OperatorPerformanceData> performance_data)
    : AbstractReadOnlyOperator(type, left, right, std::move(performance_data)),
      _mode(mode),
      _cxlumn_ids(cxlumn_ids),
      _predicate_condition(predicate_condition) {
  DebugAssert(mode != JoinMode::Cross,
              "Specified JoinMode not supported by an AbstractJoin, use Product etc. instead.");
}

JoinMode AbstractJoinOperator::mode() const { return _mode; }

const CxlumnIDPair& AbstractJoinOperator::cxlumn_ids() const { return _cxlumn_ids; }

PredicateCondition AbstractJoinOperator::predicate_condition() const { return _predicate_condition; }

const std::string AbstractJoinOperator::description(DescriptionMode description_mode) const {
  std::string cxlumn_name_left = std::string("Cxlumn #") + std::to_string(_cxlumn_ids.first);
  std::string cxlumn_name_right = std::string("Cxlumn #") + std::to_string(_cxlumn_ids.second);

  if (input_table_left()) cxlumn_name_left = input_table_left()->cxlumn_name(_cxlumn_ids.first);
  if (input_table_right()) cxlumn_name_right = input_table_right()->cxlumn_name(_cxlumn_ids.second);

  const auto separator = description_mode == DescriptionMode::MultiLine ? "\n" : " ";

  return name() + separator + "(" + join_mode_to_string.at(_mode) + " Join where " + cxlumn_name_left + " " +
         predicate_condition_to_string.left.at(_predicate_condition) + " " + cxlumn_name_right + ")";
}

void AbstractJoinOperator::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {}

}  // namespace opossum
