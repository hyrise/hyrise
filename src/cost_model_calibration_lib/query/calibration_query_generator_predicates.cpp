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
#include "../configuration/calibration_table_specification.hpp"
#include "expression/expression_functional.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"
#include "utils/assert.hpp"
#include "constant_mappings.hpp"
#include "types.hpp"

namespace opossum {

const std::optional<std::string> CalibrationQueryGeneratorPredicates::generate_predicates(
    const PredicateGeneratorFunctor& predicate_generator, const CalibrationTableSpecification& table_definition,
    const size_t number_of_predicates, const std::string& predicate_join_keyword,
    const std::string& column_name_prefix) {
  std::random_device random_device;
  std::mt19937 engine{random_device()};

  std::vector<std::string> predicates{};

  auto column_definitions_copy = table_definition.columns;

  for (size_t i = 0; i < number_of_predicates; i++) {
    CalibrationTableSpecification local_table_definition{table_definition.table_path, table_definition.table_name,
                                                         table_definition.table_size, column_definitions_copy};

    if (column_definitions_copy.size() == 0) continue;

    // select scan column
    std::uniform_int_distribution<long> filter_column_dist(0, column_definitions_copy.size() - 1);
    auto filter_column = std::next(column_definitions_copy.begin(), filter_column_dist(engine));
    auto predicate = predicate_generator(*filter_column, local_table_definition, column_name_prefix);

    if (!predicate) continue;
    predicates.push_back(*predicate);

    // Avoid filtering on the same column twice
    column_definitions_copy.erase(filter_column->first);
  }

  if (predicates.empty()) return {};
  return boost::algorithm::join(predicates, " " + predicate_join_keyword + " ");
}

const std::optional<std::string> CalibrationQueryGeneratorPredicates::_generate_between(
    const BetweenPredicateGeneratorFunctor& between_predicate_generator,
    const std::pair<std::string, CalibrationColumnSpecification>& filter_column,
    const CalibrationTableSpecification& table_definition, const std::string& column_name_prefix) {
  auto between_predicate_template = "%1% BETWEEN %2% AND %3%";
  auto filter_column_name = column_name_prefix + filter_column.first;
  const auto& between_values = between_predicate_generator(filter_column, table_definition, column_name_prefix);

  if (!between_values) return {};

  return boost::str(boost::format(between_predicate_template) % filter_column_name % between_values->first %
                    between_values->second);
}

const std::optional<std::string> CalibrationQueryGeneratorPredicates::generate_between_predicate_value(
    const std::pair<std::string, CalibrationColumnSpecification>& filter_column,
    const CalibrationTableSpecification& table_definition, const std::string& column_name_prefix) {
  const auto& between_predicate_value = [](const std::pair<std::string, CalibrationColumnSpecification>& filter_column,
                                           const CalibrationTableSpecification& table_definition,
                                           const std::string& column_name_prefix) {
    auto first_filter_column_value =
        CalibrationQueryGeneratorPredicates::_generate_table_scan_predicate_value(filter_column.second);
    auto second_filter_column_value =
        CalibrationQueryGeneratorPredicates::_generate_table_scan_predicate_value(filter_column.second);

    if (first_filter_column_value < second_filter_column_value) {
      return std::make_pair(first_filter_column_value, second_filter_column_value);
    }
    return std::make_pair(second_filter_column_value, first_filter_column_value);
  };

  return _generate_between(between_predicate_value, filter_column, table_definition, column_name_prefix);
}

const std::optional<std::string> CalibrationQueryGeneratorPredicates::generate_between_predicate_column(
    const std::pair<std::string, CalibrationColumnSpecification>& filter_column,
    const CalibrationTableSpecification& table_definition, const std::string& column_name_prefix) {
  const auto& between_predicate_column =
      [](const std::pair<std::string, CalibrationColumnSpecification>& filter_column,
         const CalibrationTableSpecification& table_definition,
         const std::string& column_name_prefix) -> std::optional<std::pair<std::string, std::string>> {
    std::random_device random_device;
    std::mt19937 engine{random_device()};

    auto first_filter_column_value = _generate_table_scan_predicate_value(filter_column.second);
    auto second_filter_column_value = _generate_table_scan_predicate_value(filter_column.second);

    const auto& column_definitions = table_definition.columns;
    std::vector<std::pair<std::string, CalibrationColumnSpecification>> v(column_definitions.begin(),
                                                                          column_definitions.end());
    std::shuffle(v.begin(), v.end(), engine);

    std::optional<std::pair<std::string, CalibrationColumnSpecification>> second_column;
    for (const auto& potential_second_column : v) {
      if (potential_second_column.first == filter_column.first) continue;
      if (potential_second_column.second.type != filter_column.second.type) continue;
      second_column = potential_second_column;
    }

    std::optional<std::pair<std::string, CalibrationColumnSpecification>> third_column;
    if (second_column) {
      for (const auto& potential_third_column : v) {
        const auto& potential_third_column_type = potential_third_column.second.type;
        if (potential_third_column.first == filter_column.first ||
            potential_third_column.first == second_column->first) {
          continue;
        }
        if (potential_third_column_type != filter_column.second.type ||
            potential_third_column_type != second_column->second.type) {
          continue;
        }

        third_column = potential_third_column;
      }
    }

    if (!second_column || !third_column) return {};

    auto second_column_name = column_name_prefix + second_column->first;
    auto third_column_name = column_name_prefix + third_column->first;

    return std::make_pair(second_column_name, third_column_name);
  };

  return _generate_between(between_predicate_column, filter_column, table_definition, column_name_prefix);
}

const std::optional<std::string> CalibrationQueryGeneratorPredicates::_generate_column_predicate(
    const PredicateGeneratorFunctor& predicate_generator,
    const std::pair<std::string, CalibrationColumnSpecification>& filter_column,
    const CalibrationTableSpecification& table_definition, const std::string& column_name_prefix) {
  const auto predicate_template = "%1% %2% %3%";
  const auto filter_column_name = column_name_prefix + filter_column.first;
  // We only want to measure various selectivities.
  // It shouldn't be that important whether we have Point or Range Lookups.
  // Isn't it?

  // At the same time this makes sure that the probability of having empty intermediate results is reduced.
  const auto predicate_sign = "<=";

  const auto filter_column_value = predicate_generator(filter_column, table_definition, column_name_prefix);

  if (!filter_column_value) {
    return {};
  }

  return boost::str(boost::format(predicate_template) % filter_column_name % predicate_sign % *filter_column_value);
}

const std::optional<std::string> CalibrationQueryGeneratorPredicates::generate_predicate_column_value(
    const std::pair<std::string, CalibrationColumnSpecification>& filter_column,
    const CalibrationTableSpecification& table_definition, const std::string& column_name_prefix) {
  const auto& filter_column_value = [](const std::pair<std::string, CalibrationColumnSpecification>& filter_column,
                                       const CalibrationTableSpecification& table_definition,
                                       const std::string& column_name_prefix) {
    return _generate_table_scan_predicate_value(filter_column.second);
  };

  return _generate_column_predicate(filter_column_value, filter_column, table_definition, column_name_prefix);
}

const std::optional<std::string> CalibrationQueryGeneratorPredicates::generate_predicate_column_column(
    const std::pair<std::string, CalibrationColumnSpecification>& filter_column,
    const CalibrationTableSpecification& table_definition, const std::string& column_name_prefix) {
  const auto& filter_column_column = [](const std::pair<std::string, CalibrationColumnSpecification>& filter_column,
                                        const CalibrationTableSpecification& table_definition,
                                        const std::string& column_name_prefix) -> std::optional<std::string> {
    std::random_device random_device;
    std::mt19937 engine{random_device()};

    const auto& column_definitions = table_definition.columns;
    std::vector<std::pair<std::string, CalibrationColumnSpecification>> v(column_definitions.begin(),
                                                                          column_definitions.end());
    std::shuffle(v.begin(), v.end(), engine);

    std::optional<std::pair<std::string, CalibrationColumnSpecification>> second_column;
    for (const auto& column : v) {
      if (column.first == filter_column.first) continue;
      if (column.second.type != filter_column.second.type) continue;

      second_column = column;
    }

    if (!second_column) {
      return std::nullopt;
    }

    return column_name_prefix + second_column->first;
  };

  return _generate_column_predicate(filter_column_column, filter_column, table_definition, column_name_prefix);
}

const std::optional<std::string> CalibrationQueryGeneratorPredicates::generate_predicate_like(
    const std::pair<std::string, CalibrationColumnSpecification>& filter_column,
    const CalibrationTableSpecification& table_definition, const std::string& column_name_prefix) {
  const auto predicate_template = "%1% %2% %3%";
  const auto filter_column_name = column_name_prefix + filter_column.first;
  const auto predicate_sign = "LIKE";

  if (filter_column.second.type != DataType::String) return {};

  auto filter_column_value = _generate_table_scan_predicate_value(filter_column.second);
  // remove trailing apostrophe
  filter_column_value.pop_back();

  // Append wildcard and apostrophe
  const auto modified = filter_column_value + "%'";

  return boost::str(boost::format(predicate_template) % filter_column_name % predicate_sign % modified);
}

const std::optional<std::string> CalibrationQueryGeneratorPredicates::generate_equi_predicate_for_strings(
    const std::pair<std::string, CalibrationColumnSpecification>& filter_column,
    const CalibrationTableSpecification& table_definition, const std::string& column_name_prefix) {
  // We just want Equi Scans on String columns for now
  if (filter_column.second.type != DataType::String) return {};

  std::random_device random_device;
  std::mt19937 engine{random_device()};

  const auto predicate_template = "%1% = '%2%'";
  const auto filter_column_name = column_name_prefix + filter_column.first;

  // TODO(Sven): Get one existing value from column and filter by that
  const auto table = StorageManager::get().get_table(table_definition.table_name);
  const auto column_id = table->column_id_by_name(filter_column.first);

  std::uniform_int_distribution<uint64_t> row_number_dist(0, table->row_count() - 1);
  const auto filter_column_value = table->get_value<std::string>(column_id, row_number_dist(engine));

  return boost::str(boost::format(predicate_template) % filter_column_name % filter_column_value);
}

const std::string CalibrationQueryGeneratorPredicates::_generate_table_scan_predicate_value(
    const CalibrationColumnSpecification& column_definition) {
  auto column_type = column_definition.type;
  std::random_device random_device;
  std::mt19937 engine{random_device()};
  std::uniform_int_distribution<uint16_t> int_dist(0, column_definition.distinct_values - 1);
  std::uniform_real_distribution<> float_dist(0, 1);
  std::uniform_int_distribution<uint16_t> char_dist(0, 25);

//  if (column_type == DataType::Int) return std::to_string(int_dist(engine));
//  if (column_type == DataType::String) return "'" + std::string(1, 'A' + char_dist(engine)) + "'";
//  if (column_type == DataType::Float) return std::to_string(float_dist(engine));

  switch (column_type) {
      case DataType::Int:
          return std::to_string(int_dist(engine));
      case DataType::String:
          return "'" + std::string(1, 'A' + char_dist(engine)) + "'";
      case DataType::Float:
          return std::to_string(float_dist(engine));
      default:
          Fail("Unsupported data type in CalibrationQueryGenerator, found " + data_type_to_string.left.at(column_type));
  }
}

}  // namespace opossum
