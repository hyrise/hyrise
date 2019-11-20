#include "cost_estimator_coefficient_reader.hpp"

namespace opossum {

const CoefficientsPerGroup CostEstimatorCoefficientReader::default_coefficients() {
  CoefficientsPerGroup all_coefficients{};

  const auto table_scan_coefficients = read_table_scan_coefficients();
  const auto join_coefficients = read_join_coefficients();

  all_coefficients.insert(table_scan_coefficients.begin(), table_scan_coefficients.end());
  all_coefficients.insert(join_coefficients.begin(), join_coefficients.end());

  return all_coefficients;
}

// Hard-coded efficients for now
const CoefficientsPerGroup CostEstimatorCoefficientReader::read_table_scan_coefficients(const std::string& file_path) {
  return {{ModelGroup{OperatorType::TableScan, DataType::Int, false, false},
           {{"left_input_row_count", 3.6579310596},
            //                          {"is_result_empty", -3894.4296670012},
            {"selectivity", 11285.9587666981},
            {"first_column_is_reference_segment", 0},
            {"second_column_is_reference_segment", 0},
            {"third_column_is_reference_segment", 0},
            {"is_column_comparison", -1721.1127107658},
            {"computable_or_column_expression_count", 14227.0873454307},
            {"is_selectivity_below_50_percent", 1874.7617189885},
            {"selectivity_distance_to_50_percent", -17721.3571372186},
            //                          {"is_small_table", 0},
            {"first_column_segment_encoding_Unencoded_percentage", 875.4659477475},
            {"first_column_segment_encoding_Dictionary_percentage", -2466.8790265752},
            {"first_column_segment_encoding_RunLength_percentage", 807.0515189504},
            {"first_column_segment_encoding_FixedStringDictionary_percentage", 0},
            {"first_column_segment_encoding_FrameOfReference_percentage", 2365.7972807862},
            //                          {"first_column_segment_encoding_undefined", 0},
            //                          {"first_column_data_type_null", 1581.4357209088},
            {"first_column_data_type_int", 1581.4357209088},
            {"first_column_data_type_long", 1581.4357209088},
            {"first_column_data_type_float", 1581.4357209088},
            {"first_column_data_type_double", 1581.4357209088},
            {"first_column_data_type_string", 1581.4357209088},
            {"first_column_data_type_undefined", 1581.4357209088},
            {"second_column_segment_encoding_Unencoded_percentage", -156.771035114},
            {"second_column_segment_encoding_Dictionary_percentage", -504.0943341712},
            {"second_column_segment_encoding_RunLength_percentage", -691.4961272456},
            {"second_column_segment_encoding_FixedStringDictionary_percentage", 0},
            {"second_column_segment_encoding_FrameOfReference_percentage", -368.7512142351},
            //                          {"second_column_segment_encoding_undefined", 3302.5484316746},
            //                          {"second_column_data_type_null", -1721.1127107658},
            {"second_column_data_type_int", -1721.1127107658},
            {"second_column_data_type_long", -1721.1127107658},
            {"second_column_data_type_float", -1721.1127107658},
            {"second_column_data_type_double", -1721.1127107658},
            {"second_column_data_type_string", -1721.1127107658},
            {"second_column_data_type_undefined", 3302.5484316746},
            {"third_column_segment_encoding_Unencoded_percentage", 4453.3879124653},
            {"third_column_segment_encoding_Dictionary_percentage", 2251.3754879083},
            {"third_column_segment_encoding_RunLength_percentage", 3768.4614356163},
            {"third_column_segment_encoding_FixedStringDictionary_percentage", 0},
            {"third_column_segment_encoding_FrameOfReference_percentage", 2312.1037783889},
            //                          {"third_column_segment_encoding_undefined", -11203.8928934701},
            //                          {"third_column_data_type_null", 12785.3286143789},
            {"third_column_data_type_int", 12785.3286143789},
            {"third_column_data_type_long", 12785.3286143789},
            {"third_column_data_type_float", 12785.3286143789},
            {"third_column_data_type_double", 12785.3286143789},
            {"third_column_data_type_string", 12785.3286143789},
            {"third_column_data_type_undefined", -11203.892893470}}}};
}

const CoefficientsPerGroup CostEstimatorCoefficientReader::read_join_coefficients(const std::string& file_path) {
  return {
      {ModelGroup{OperatorType::JoinHash, {}, false},
       {{"left_input_row_count", 22.45190459223849}, {"right_input_row_count", 99.68893904977213}}},
      {ModelGroup{OperatorType::JoinHash, {}, true},
       {{"left_input_row_count", 31.493424340133515}, {"right_input_row_count", 79.3849625472923}}},
      {ModelGroup{OperatorType::JoinNestedLoop, {}, false},
       {{"left_input_row_count", 72.79503982648723}, {"right_input_row_count", 16.38312327057448}}},
      {ModelGroup{OperatorType::JoinNestedLoop, {}, true},
       {{"left_input_row_count", 1931.4930924059695}, {"right_input_row_count", 70.39852863077502}}},
      {ModelGroup{OperatorType::JoinSortMerge, {}, false},
       {{"left_input_row_count", 49.03427670959679}, {"right_input_row_count", 37.967034865681484}}},
      {ModelGroup{OperatorType::JoinSortMerge, {}, true},
       {{"left_input_row_count", 163.73846985773895}, {"right_input_row_count", 37.762080132353475}}},
  };
}

}  // namespace opossum
