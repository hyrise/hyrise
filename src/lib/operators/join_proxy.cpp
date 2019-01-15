#include "join_proxy.hpp"

#include <map>
#include <memory>
#include <numeric>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "all_type_variant.hpp"
#include "cost_model/cost_model_adaptive.hpp"
#include "cost_model/cost_model_coefficient_reader.hpp"
#include "operators/join_hash.hpp"
#include "operators/join_index.hpp"
#include "operators/join_nested_loop.hpp"
#include "operators/join_mpsm.hpp"
#include "operators/join_sort_merge.hpp"
#include "resolve_type.hpp"
#include "storage/index/base_index.hpp"
#include "storage/segment_iterate.hpp"
#include "type_comparison.hpp"
#include "utils/assert.hpp"
#include "utils/performance_warning.hpp"

namespace opossum {

/*
 * This is a proxy join implementation.
 */

JoinProxy::JoinProxy(const std::shared_ptr<const AbstractOperator>& left,
                     const std::shared_ptr<const AbstractOperator>& right, const JoinMode mode,
                     const std::pair<ColumnID, ColumnID>& column_ids, const PredicateCondition predicate_condition)
    : AbstractJoinOperator(OperatorType::JoinIndex, left, right, mode, column_ids, predicate_condition,
                           std::make_unique<JoinProxy::PerformanceData>()), _cost_model(std::make_shared<CostModelAdaptive>(CostModelCoefficientReader::read_table_scan_coefficients(), CostModelCoefficientReader::read_join_coefficients())) {


}

const std::string JoinProxy::name() const { return "JoinProxy"; }

std::shared_ptr<AbstractOperator> JoinProxy::_on_deep_copy(
    const std::shared_ptr<AbstractOperator>& copied_input_left,
    const std::shared_ptr<AbstractOperator>& copied_input_right) const {
  return std::make_shared<JoinProxy>(copied_input_left, copied_input_right, _mode, _column_ids, _predicate_condition);
}

void JoinProxy::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {}

std::shared_ptr<const Table> JoinProxy::_on_execute() {
  // Get inputs
  const auto& left_input_table = _input_left->get_output();
  const auto& right_input_table = _input_right->get_output();
  const auto& left_input_size = left_input_table->row_count();
  const auto& right_input_size = right_input_table->row_count();

  std::cout << left_input_size << std::endl;
  std::cout << right_input_size << std::endl;

  // Create Operators for all valid join algorithms
//  const auto hash_join = std::make_shared<JoinHash>(_input_left, _input_right, _mode, _column_ids, _predicate_condition);
//  const auto index_join = std::make_shared<JoinIndex>(_input_left, _input_right, _mode, _column_ids, _predicate_condition);
//  const auto nested_loop_join = std::make_shared<JoinNestedLoop>(_input_left, _input_right, _mode, _column_ids, _predicate_condition);
//  const auto mpsm_join = std::make_shared<JoinMPSM>(_input_left, _input_right, _mode, _column_ids, _predicate_condition);
//  const auto sort_merge_join = std::make_shared<JoinSortMerge>(_input_left, _input_right, _mode, _column_ids, _predicate_condition);

  // Cost all valid join algorithms
//  _cost_model->

  // Order by costs and select Join Algorithm

  // Execute Join

  // Return output
  return nullptr;
}

std::string JoinProxy::PerformanceData::to_string(DescriptionMode description_mode) const {
  std::string string = OperatorPerformanceData::to_string(description_mode);
  //        string += (description_mode == DescriptionMode::SingleLine ? " / " : "\\n");
  return string;
}

}  // namespace opossum
