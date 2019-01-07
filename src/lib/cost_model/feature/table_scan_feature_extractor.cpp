#include "table_scan_feature_extractor.hpp"

#include "logical_query_plan/predicate_node.hpp"
#include "statistics/table_statistics.hpp"

namespace opossum {

std::unordered_map<std::string, float> TableScanFeatureExtractor::extract(
    const std::shared_ptr<AbstractLQPNode>& node) const {
  const auto predicate_node = std::static_pointer_cast<PredicateNode>(node);
  Assert(predicate_node, "Cannot call TableScanFeatureExtractor with LQP node other than PredicateNode");

  // Collecting features
  // TODO(Sven): Extract actual feature values from LQP and statistics
  const auto left_input_row_count = node->left_input() ? node->left_input()->get_statistics()->row_count() : 0.0f;
  const auto selectivity = 0;
  const auto number_of_computable_or_column_expressions = 0;
  const auto output_selectivity_distance_to_50_percent = 0;

  const auto first_column_segment_enconding_Unencoded = 0;
  const auto first_column_segment_enconding_Dictionary = 0;
  const auto first_column_segment_enconding_RunLength = 0;
  const auto first_column_segment_enconding_FixedStringDictionary = 0;
  const auto first_column_segment_enconding_undefined = 0;
  const auto first_column_is_segment_reference_segment_False = 0;
  const auto first_column_is_segment_reference_segment_True = 0;
  const auto first_column_segment_data_type_null = 0;
  const auto first_column_segment_data_type_int = 0;
  const auto first_column_segment_data_type_long = 0;
  const auto first_column_segment_data_type_float = 0;
  const auto first_column_segment_data_type_double = 0;
  const auto first_column_segment_data_type_string = 0;
  const auto first_column_segment_data_type_undefined = 0;

  const auto second_column_segment_enconding_Unencoded = 0;
  const auto second_column_segment_enconding_Dictionary = 0;
  const auto second_column_segment_enconding_RunLength = 0;
  const auto second_column_segment_enconding_FixedStringDictionary = 0;
  const auto second_column_segment_enconding_undefined = 0;
  const auto second_column_is_segment_reference_segment_False = 0;
  const auto second_column_is_segment_reference_segment_True = 0;
  const auto second_column_segment_data_type_null = 0;
  const auto second_column_segment_data_type_int = 0;
  const auto second_column_segment_data_type_long = 0;
  const auto second_column_segment_data_type_float = 0;
  const auto second_column_segment_data_type_double = 0;
  const auto second_column_segment_data_type_string = 0;
  const auto second_column_segment_data_type_undefined = 0;

  const auto third_column_segment_enconding_Unencoded = 0;
  const auto third_column_segment_enconding_Dictionary = 0;
  const auto third_column_segment_enconding_RunLength = 0;
  const auto third_column_segment_enconding_FixedStringDictionary = 0;
  const auto third_column_segment_enconding_undefined = 0;
  const auto third_column_is_segment_reference_segment_False = 0;
  const auto third_column_is_segment_reference_segment_True = 0;
  const auto third_column_segment_data_type_null = 0;
  const auto third_column_segment_data_type_int = 0;
  const auto third_column_segment_data_type_long = 0;
  const auto third_column_segment_data_type_float = 0;
  const auto third_column_segment_data_type_double = 0;
  const auto third_column_segment_data_type_string = 0;
  const auto third_column_segment_data_type_undefined = 0;

  const auto is_column_comparison_False = 0;
  const auto is_column_comparison_True = 0;

  const auto is_output_selectivity_below_50_percent_False = 0;
  const auto is_output_selectivity_below_50_percent_True = 0;

  const auto is_small_table_False = 0;
  const auto is_small_table_True = 0;

  // TODO(Sven): Add all the features that are currently not being used, but may be used eventually, e.g. hardware features

  // construct map of features
  const std::unordered_map<std::string, float> feature_map = {
      {"left_input_row_count", left_input_row_count},
      // TODO(Sven): rename
      {"selectivity", selectivity},
      {"number_of_computable_or_column_expressions", number_of_computable_or_column_expressions},
      {"output_selectivity_distance_to_50_percent", output_selectivity_distance_to_50_percent},

      {"first_column_segment_enconding_Unencoded", first_column_segment_enconding_Unencoded},
      {"first_column_segment_enconding_Dictionary", first_column_segment_enconding_Dictionary},
      {"first_column_segment_enconding_RunLength", first_column_segment_enconding_RunLength},
      {"first_column_segment_enconding_FixedStringDictionary", first_column_segment_enconding_FixedStringDictionary},
      {"first_column_segment_enconding_undefined", first_column_segment_enconding_undefined},
      {"first_column_is_segment_reference_segment_False", first_column_is_segment_reference_segment_False},
      {"first_column_is_segment_reference_segment_True", first_column_is_segment_reference_segment_True},
      {"first_column_segment_data_type_null", first_column_segment_data_type_null},
      {"first_column_segment_data_type_int", first_column_segment_data_type_int},
      {"first_column_segment_data_type_long", first_column_segment_data_type_long},
      {"first_column_segment_data_type_float", first_column_segment_data_type_float},
      {"first_column_segment_data_type_double", first_column_segment_data_type_double},
      {"first_column_segment_data_type_string", first_column_segment_data_type_string},
      {"first_column_segment_data_type_undefined", first_column_segment_data_type_undefined},

      {"second_column_segment_enconding_Unencoded", second_column_segment_enconding_Unencoded},
      {"second_column_segment_enconding_Dictionary", second_column_segment_enconding_Dictionary},
      {"second_column_segment_enconding_RunLength", second_column_segment_enconding_RunLength},
      {"second_column_segment_enconding_FixedStringDictionary", second_column_segment_enconding_FixedStringDictionary},
      {"second_column_segment_enconding_undefined", second_column_segment_enconding_undefined},
      {"second_column_is_segment_reference_segment_False", second_column_is_segment_reference_segment_False},
      {"second_column_is_segment_reference_segment_True", second_column_is_segment_reference_segment_True},
      {"second_column_segment_data_type_null", second_column_segment_data_type_null},
      {"second_column_segment_data_type_int", second_column_segment_data_type_int},
      {"second_column_segment_data_type_long", second_column_segment_data_type_long},
      {"second_column_segment_data_type_float", second_column_segment_data_type_float},
      {"second_column_segment_data_type_double", second_column_segment_data_type_double},
      {"second_column_segment_data_type_string", second_column_segment_data_type_string},
      {"second_column_segment_data_type_undefined", second_column_segment_data_type_undefined},

      {"third_column_segment_enconding_Unencoded", third_column_segment_enconding_Unencoded},
      {"third_column_segment_enconding_Dictionary", third_column_segment_enconding_Dictionary},
      {"third_column_segment_enconding_RunLength", third_column_segment_enconding_RunLength},
      {"third_column_segment_enconding_FixedStringDictionary", third_column_segment_enconding_FixedStringDictionary},
      {"third_column_segment_enconding_undefined", third_column_segment_enconding_undefined},
      {"third_column_is_segment_reference_segment_False", third_column_is_segment_reference_segment_False},
      {"third_column_is_segment_reference_segment_True", third_column_is_segment_reference_segment_True},
      {"third_column_segment_data_type_null", third_column_segment_data_type_null},
      {"third_column_segment_data_type_int", third_column_segment_data_type_int},
      {"third_column_segment_data_type_long", third_column_segment_data_type_long},
      {"third_column_segment_data_type_float", third_column_segment_data_type_float},
      {"third_column_segment_data_type_double", third_column_segment_data_type_double},
      {"third_column_segment_data_type_string", third_column_segment_data_type_string},
      {"third_column_segment_data_type_undefined", third_column_segment_data_type_undefined},

      {"is_column_comparison_False", is_column_comparison_False},
      {"is_column_comparison_True", is_column_comparison_True},

      {"is_output_selectivity_below_50_percent_False", is_output_selectivity_below_50_percent_False},
      {"is_output_selectivity_below_50_percent_True", is_output_selectivity_below_50_percent_True},

      {"is_small_table_False", is_small_table_False},
      {"is_small_table_True", is_small_table_True},
  };

  return {};
}

std::unordered_map<std::string, float> TableScanFeatureExtractor::extract(
    const std::shared_ptr<AbstractOperator>& abstract_operator) const {
  return {};
}
}  // namespace opossum