#include "calibration_query_generator_predicates.hpp"

#include <boost/algorithm/string/join.hpp>
#include <boost/format.hpp>
#include <experimental/iterator>

#include <algorithm>
#include <iostream>
#include <iterator>
#include <random>
#include <vector>

#include "../configuration/calibration_column_specification.hpp"
#include "utils/assert.hpp"

namespace opossum {

    const std::string CalibrationQueryGeneratorPredicates::generate_predicates(const std::map<std::string, CalibrationColumnSpecification>& column_definitions,
                                                                               const std::string column_name_prefix) {
      std::random_device random_device;
      std::mt19937 engine{random_device()};

      std::uniform_int_distribution<size_t> number_of_predicates_dist(1, 3);
      auto number_of_predicates = number_of_predicates_dist(engine);

      std::stringstream predicate_stream;

      auto remaining_column_definitions = column_definitions;

      for (size_t i = 0; i < number_of_predicates; i++) {
        predicate_stream << _generate_predicate(remaining_column_definitions, column_name_prefix);

        if (i < number_of_predicates - 1) {
          predicate_stream << " AND ";
        }
      }
      return predicate_stream.str();
    }

const std::string CalibrationQueryGeneratorPredicates::_generate_predicate(
    std::map<std::string, CalibrationColumnSpecification>& column_definitions,
    const std::string column_name_prefix) {
  std::random_device random_device;
  std::mt19937 engine{random_device()};
  std::uniform_int_distribution<u_int64_t> filter_column_dist(0, column_definitions.size() - 1);

  auto predicate_template = "%1% %2% %3%";
  auto between_predicate_template = "%1% BETWEEN %2% AND %3%";

  auto filter_column = std::next(column_definitions.begin(), filter_column_dist(engine));
  auto filter_column_name = column_name_prefix + filter_column->first;

  std::optional<std::pair<std::string, CalibrationColumnSpecification>> second_column;
  for (const auto& column : column_definitions) {
    if (column.first == filter_column->first) continue;
    if (column.second.type != filter_column->second.type) continue;

    second_column = column;
  }

  std::optional<std::pair<std::string, CalibrationColumnSpecification>> third_column;
  if (second_column) {
    for (const auto& column : column_definitions) {
      if (column.first == filter_column->first || column.first == second_column->first) continue;

      if (column.second.type != filter_column->second.type || column.second.type != second_column->second.type)
        continue;

      third_column = column;
    }
  }

  // Avoid filtering on the same column twice
  column_definitions.erase(filter_column->first);

  // We only want to measure various selectivities.
  // It shouldn't be that important whether we have Point or Range Lookups.
  // Isn't it?

  // At the same time this makes sure that the probability of having empty intermediate results is reduced.

  auto predicate_sign = "<=";

  std::uniform_real_distribution<> float_dist(0, 1);
  auto foo = float_dist(engine);

  if (foo > 0.95 && second_column && third_column) {
    // Generate between with column to column
    auto second_column_name = column_name_prefix + second_column->first;
    auto third_column_name = column_name_prefix + third_column->first;
    return boost::str(boost::format(between_predicate_template) % filter_column_name % second_column_name %
                      third_column_name);
  } else if (foo > 0.75) {
    // Generate between with value to value
    auto first_filter_column_value = _generate_table_scan_predicate_value(filter_column->second);
    auto second_filter_column_value = _generate_table_scan_predicate_value(filter_column->second);
    if (first_filter_column_value < second_filter_column_value) {
        return boost::str(boost::format(between_predicate_template) % filter_column_name % first_filter_column_value %
                          second_filter_column_value);
    }
    return boost::str(boost::format(between_predicate_template) % filter_column_name % second_filter_column_value %
    first_filter_column_value);
  } else if (foo > 0.5 && second_column) {
    // Generate column to column
    auto second_column_name = column_name_prefix + second_column->first;
    return boost::str(boost::format(predicate_template) % filter_column_name % predicate_sign % second_column_name);
  } else {
    // Generate column to value
    auto filter_column_value = _generate_table_scan_predicate_value(filter_column->second);
    return boost::str(boost::format(predicate_template) % filter_column_name % predicate_sign % filter_column_value);
  }
}

const std::string CalibrationQueryGeneratorPredicates::_generate_table_scan_predicate_value(
    const CalibrationColumnSpecification& column_definition) {
  auto column_type = column_definition.type;
  std::random_device random_device;
  std::mt19937 engine{random_device()};
  std::uniform_int_distribution<u_int16_t> int_dist(0, column_definition.distinct_values - 1);
  std::uniform_real_distribution<> float_dist(0, column_definition.distinct_values - 1);
  //      std::uniform_int_distribution<> char_dist(0, UCHAR_MAX);

  // Initialize with some random seed
  u_int32_t seed = int_dist(engine);

  if (column_type == "int") return std::to_string(int_dist(engine));
  if (column_type == "string") return "'" + std::string(1, 'a' + rand_r(&seed) % 26) + "'";
  if (column_type == "float") return std::to_string(float_dist(engine));

  Fail("Unsupported data type in CalibrationQueryGenerator");
}

}  // namespace opossum
