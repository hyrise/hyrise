#include "join_proxy.hpp"

#include <map>
#include <memory>
#include <numeric>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "all_type_variant.hpp"
#include "constant_mappings.hpp"
#include "cost_estimation/cost_estimator_adaptive.hpp"
#include "cost_estimation/cost_estimator_coefficient_reader.hpp"
#include "cost_estimation/feature/cost_model_features.hpp"
#include "cost_estimation/feature/join_features.hpp"
#include "operators/join_hash.hpp"
#include "operators/join_index.hpp"
#include "operators/join_nested_loop.hpp"
#include "operators/join_sort_merge.hpp"
#include "resolve_type.hpp"
#include "storage/index/abstract_index.hpp"
#include "storage/segment_iterate.hpp"
#include "type_comparison.hpp"
#include "utils/assert.hpp"
#include "utils/performance_warning.hpp"

#include "operators/operator_join_predicate.hpp"

using namespace std::string_literals;  // NOLINT

namespace opossum {

/*
 * This is a proxy join implementation.
 */

bool JoinProxy::supports(JoinMode join_mode, PredicateCondition predicate_condition, DataType left_data_type,
                         DataType right_data_type, bool secondary_predicates) {
  return true;
}

JoinProxy::JoinProxy(const std::shared_ptr<const AbstractOperator>& left,
                     const std::shared_ptr<const AbstractOperator>& right, const JoinMode mode,
                     const OperatorJoinPredicate& primary_predicate,
                     const std::vector<OperatorJoinPredicate>& secondary_predicates)
    : AbstractJoinOperator(OperatorType::JoinIndex, left, right, mode, primary_predicate, secondary_predicates) {
  _cost_model = std::make_shared<CostEstimatorAdaptive>(std::make_shared<CardinalityEstimator>());
  auto cost_estimator_adaptive = std::dynamic_pointer_cast<CostEstimatorAdaptive>(_cost_model);
  cost_estimator_adaptive->initialize(CostEstimatorCoefficientReader::default_coefficients());
}

const std::string& JoinProxy::name() const {
  static const auto name = std::string{"JoinHash"};
  return name;
}

std::shared_ptr<AbstractOperator> JoinProxy::_on_deep_copy(
    const std::shared_ptr<AbstractOperator>& copied_left_input,
    const std::shared_ptr<AbstractOperator>& copied_right_input) const {
  return std::make_shared<JoinProxy>(copied_left_input, copied_right_input, _mode, _primary_predicate,
                                     _secondary_predicates);
}

void JoinProxy::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {}

std::shared_ptr<const Table> JoinProxy::_on_execute() {

  // return std::shared_ptr<const Table>();

  // Get inputs
  const auto& left_input_table = _left_input->get_output();
  const auto& right_input_table = _right_input->get_output();
  const auto& left_input_size = left_input_table->row_count();
  const auto& right_input_size = right_input_table->row_count();

  std::cout << "JoinProxy: " << left_input_size << "x" << right_input_size << std::endl;

  CostModelFeatures cost_model_features{};
  if (left_input_size > 0 && right_input_size > 0) {
    if (left_input_size > right_input_size) {
      cost_model_features.input_table_size_ratio = static_cast<float>(left_input_size) / static_cast<float>(right_input_size);
    } else {
      cost_model_features.input_table_size_ratio = static_cast<float>(right_input_size) / static_cast<float>(left_input_size);
    }
  }

  cost_model_features.left_input_row_count = left_input_size;
  cost_model_features.right_input_row_count = right_input_size;
  cost_model_features.total_row_count =
      std::max<uint64_t>(1ul, left_input_size) * std::max<uint64_t>(1ul, right_input_size);
  // cost_model_features.logical_cost_sort_merge = left_input_size * static_cast<float>(std::log(right_input_size));
  // cost_model_features.logical_cost_hash = left_input_size + right_input_size;

  const auto left_column_id = _primary_predicate.column_ids.first;
  const auto right_column_id = _primary_predicate.column_ids.second;

  size_t left_memory_usage = 0;
  bool is_left_reference_segment = false;
  const auto left_chunk_count = left_input_table->chunk_count();
  for (auto chunk_id = ChunkID{0}; chunk_id < left_chunk_count; ++chunk_id) {
    const auto& chunk = left_input_table->get_chunk(chunk_id);
    const auto& segment = chunk->get_segment(left_column_id);
    left_memory_usage += segment->memory_usage(MemoryUsageCalculationMode::Sampled);

    const auto reference_segment = std::dynamic_pointer_cast<ReferenceSegment>(segment);
    is_left_reference_segment = is_left_reference_segment || reference_segment;
  }

  size_t right_memory_usage = 0;
  bool is_right_reference_segment = false;
  const auto right_chunk_count = right_input_table->chunk_count();
  for (auto chunk_id = ChunkID{0}; chunk_id < right_chunk_count; ++chunk_id) {
    const auto& chunk = right_input_table->get_chunk(chunk_id);
    const auto& segment = chunk->get_segment(right_column_id);
    right_memory_usage += segment->memory_usage(MemoryUsageCalculationMode::Sampled);

    const auto reference_segment = std::dynamic_pointer_cast<ReferenceSegment>(segment);
    is_right_reference_segment = is_right_reference_segment || reference_segment;
  }

  bool is_referenced = is_left_reference_segment || is_right_reference_segment;

  // Hard-coded Join Features for TPCH
  JoinFeatures join_features{};
  join_features.join_mode = _mode;
  join_features.left_join_column.column_memory_usage_bytes = left_memory_usage;
  join_features.left_join_column.column_data_type = DataType::Int;
  // join_features.left_join_column.column_segment_encoding_Dictionary_percentage = 1.0f;
  // join_features.left_join_column.column_segment_encoding_RunLength_percentage = 0.0f;
  // join_features.left_join_column.column_segment_encoding_Unencoded_percentage = 0.0f;

  join_features.right_join_column.column_memory_usage_bytes = right_memory_usage;
  join_features.right_join_column.column_data_type = DataType::Int;
  // join_features.right_join_column.column_segment_encoding_Dictionary_percentage = 1.0f;
  // join_features.right_join_column.column_segment_encoding_RunLength_percentage = 0.0f;
  // join_features.right_join_column.column_segment_encoding_Unencoded_percentage = 0.0f;

  // cost_model_features.join_features = join_features;

  // Build Join Models
  const auto join_coefficients = CostEstimatorCoefficientReader::read_join_coefficients();
  std::unordered_map<ModelGroup, std::shared_ptr<LinearRegressionModel>, ModelGroupHash> join_models;
  for (const auto& [group, coefficients] : join_coefficients) {
    join_models[group] = std::make_shared<LinearRegressionModel>(coefficients);
  }

  OperatorType minimal_costs_join_type = OperatorType::JoinSortMerge;
  Cost minimal_costs{std::numeric_limits<float>::max()};

  const auto valid_join_types = _valid_join_types();
  for (const auto& join_type : valid_join_types) {
    cost_model_features.operator_type = join_type;
    ModelGroup model_group{join_type, {}, is_referenced};
    const auto predicted_costs = join_models.at(model_group)->predict(cost_model_features.to_cost_model_features());
    //    const auto exp_predicted_costs = exp(predicted_costs);
    std::cout << "JoinProxy: " << operator_type_to_string.at(join_type) << " -> " << predicted_costs << std::endl;
    if (predicted_costs < minimal_costs) {
      minimal_costs_join_type = join_type;
      minimal_costs = predicted_costs;
    }
  }

  // Swap inputs for HashJoin if possible
  if (_mode == JoinMode::Inner) {
    const auto join_type = OperatorType::JoinHash;
    cost_model_features.operator_type = join_type;

    // const auto previous_left_join_column = cost_model_features.join_features.left_join_column;
    // cost_model_features.join_features.left_join_column = cost_model_features.join_features.right_join_column;
    // cost_model_features.join_features.right_join_column = previous_left_join_column;

    // const auto prev_left_input_size = cost_model_features.left_input_row_count;
    // cost_model_features.left_input_row_count = cost_model_features.right_input_row_count;
    // cost_model_features.right_input_row_count = prev_left_input_size;

    // ModelGroup model_group{join_type, {}, is_referenced};
    // const auto predicted_costs = join_models.at(model_group)->predict(cost_model_features.to_cost_model_features());
    // //    const auto exp_predicted_costs = exp(predicted_costs);
    // std::cout << "JoinProxy: " << operator_type_to_string.at(join_type) << " -> " << predicted_costs << std::endl;
    // if (predicted_costs < minimal_costs) {
    //   minimal_costs_join_type = join_type;
    //   minimal_costs = predicted_costs;
    // }
  }

  // Execute Join
  const auto join_impl = _instantiate_join(minimal_costs_join_type);
  join_impl->execute();
  const auto execution_time = join_impl->performance_data->walltime;
  const auto execution_time_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(execution_time).count();

  //  const auto execution_time_ns_log = log(execution_time_ns);

  const auto mape = static_cast<double>(abs(static_cast<double>(execution_time_ns) - static_cast<double>(minimal_costs))) / static_cast<double>(execution_time_ns) * 100.0f;
  std::cout << "Error: " << static_cast<double>(execution_time_ns) - static_cast<double>(minimal_costs) << " [actual: " << execution_time_ns << ", " << mape
            << "%]" << std::endl;
  return join_impl->get_output();
}

const std::shared_ptr<AbstractJoinOperator> JoinProxy::_instantiate_join(const OperatorType operator_type) {
  std::cout << "JoinProxy: Initializing " << operator_type_to_string.at(operator_type) << std::endl;
  _operator_type = operator_type;
  switch (operator_type) {
    case OperatorType::JoinHash:
      return std::make_shared<JoinHash>(_left_input, _right_input, _mode, _primary_predicate, _secondary_predicates);
    case OperatorType::JoinIndex:
      return std::make_shared<JoinIndex>(_left_input, _right_input, _mode, _primary_predicate, _secondary_predicates);
    case OperatorType::JoinNestedLoop:
      return std::make_shared<JoinNestedLoop>(_left_input, _right_input, _mode, _primary_predicate,
                                              _secondary_predicates);
    case OperatorType::JoinSortMerge:
      return std::make_shared<JoinSortMerge>(_left_input, _right_input, _mode, _primary_predicate,
                                             _secondary_predicates);
    default:
      Fail("Unexpected operator type in JoinProxy. Can only handle Join operators");
  }
}

const std::vector<OperatorType> JoinProxy::_valid_join_types() const {
  // TODO(Sven): Add IndexJoin
  // TODO(anyone): use supports() method of join implementations.
  if (_primary_predicate.predicate_condition == PredicateCondition::Equals && _mode != JoinMode::FullOuter) {
    return {OperatorType::JoinHash, OperatorType::JoinNestedLoop, OperatorType::JoinSortMerge};
  }

  return {OperatorType::JoinNestedLoop, OperatorType::JoinSortMerge};
}

}  // namespace opossum
