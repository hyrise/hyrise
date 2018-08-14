#include "calibration_query_generator.hpp"

#include <algorithm>
#include <boost/algorithm/string/join.hpp>
#include <boost/format.hpp>
#include <experimental/iterator>
#include <iterator>
#include <iostream>
#include <random>
#include <utils/assert.hpp>
#include <vector>

namespace opossum {

    const std::vector<std::string> CalibrationQueryGenerator::generate_queries(const std::vector<CalibrationTableSpecification>& table_definitions) {

      std::vector<std::string> queries;

      for (const auto & table_definition : table_definitions) {
//        std::cout << "Generating queries for " << table_definition.table_name << std::endl;

// TODO: Add queries for Aggregates, Joins
        queries.push_back(CalibrationQueryGenerator::_generate_table_scans(table_definition));
      }

      auto join_queries = CalibrationQueryGenerator::_generate_join(table_definitions);
      queries.insert(queries.end(), join_queries.begin(), join_queries.end());

      return queries;
    }

    const std::vector<std::string> CalibrationQueryGenerator::_generate_join(const std::vector<CalibrationTableSpecification>& table_definitions) {
      // TODO: Add JOIN Predicate
      auto string_template = "SELECT %1% FROM %2% l JOIN %3% r ON l.%4%=r.%5%;";

      std::vector<std::string> queries;

      for (const auto & left_table : table_definitions) {
        for (const auto & right_table : table_definitions) {
          std::map<std::string, CalibrationColumnSpecification> columns;

          for (const auto& column : left_table.columns) {
            const auto column_name = "l." + column.first;
            columns.insert(std::pair<std::string, CalibrationColumnSpecification>(column_name, column.second));
          }

          for (const auto& column : right_table.columns) {
            const auto column_name = "r." + column.first;
            columns.insert(std::pair<std::string, CalibrationColumnSpecification>(column_name, column.second));
          }

          auto left_join_column = "column_a";
          auto right_join_column = "column_a";

          auto select_columns = _generate_select_columns(columns);
          queries.push_back(boost::str(boost::format(string_template) % select_columns % left_table.table_name % right_table.table_name % left_join_column % right_join_column));
        }
      }

      return queries;
    }

    const std::string CalibrationQueryGenerator::_generate_table_scans(const CalibrationTableSpecification& table_definition) {
      std::random_device random_device;
      std::mt19937 engine{random_device()};

      auto string_template = "SELECT %1% FROM %2% WHERE %3%;";
      auto predicate_template =  "%1% %2% %3%";

      auto select_columns = _generate_select_columns(table_definition.columns);
      auto table_name = table_definition.table_name;

      auto column_definitions = table_definition.columns;

      std::uniform_int_distribution<size_t> number_of_predicates_dist(1, 5);
      auto number_of_predicates = number_of_predicates_dist(engine);

      std::stringstream predicate_stream;

      std::uniform_int_distribution<long> filter_column_dist(0, column_definitions.size() - 1);
      std::uniform_int_distribution<int> equality_dist(0, 1);
      for (size_t i = 0; i < number_of_predicates; i++) {
        auto filter_column = std::next(column_definitions.begin(), filter_column_dist(engine));
        auto filter_column_name = filter_column->first;
        auto filter_column_value = _generate_table_scan_predicate(filter_column->second);

        auto predicate_sign = equality_dist(engine) == 1 ? "=" : "<";
        predicate_stream << boost::str(boost::format(predicate_template) % filter_column_name % predicate_sign % filter_column_value);

        if (i < number_of_predicates - 1) {
          predicate_stream << " AND ";
        }
      }

      auto filter_column = std::next(column_definitions.begin(), filter_column_dist(engine));
      auto filter_column_name = filter_column->first;
      auto filter_column_value = _generate_table_scan_predicate(filter_column->second);

      return boost::str(boost::format(string_template) % select_columns % table_name % predicate_stream.str());
    }

    const std::string CalibrationQueryGenerator::_generate_table_scan_predicate(const CalibrationColumnSpecification& column_definition) {
      auto column_type = column_definition.type;
      std::random_device random_device;
      std::mt19937 engine{random_device()};
      std::uniform_int_distribution<int> int_dist(0, column_definition.distinct_values - 1);
      std::uniform_real_distribution<> float_dist(0, column_definition.distinct_values - 1);

      if (column_type == "int") return std::to_string(int_dist(engine));
      if (column_type == "string") return "'Aaron Anderson'"; // TODO: somehow get values that are actually in the generated data
      if (column_type == "float") return std::to_string(float_dist(engine));

      Fail("Unsupported data type in CalibrationQueryGenerator");
    }

    const std::vector<std::string> CalibrationQueryGenerator::_get_column_names(const std::map<std::string, CalibrationColumnSpecification>& column_definitions) {
      std::vector<std::string> column_names;
      column_names.reserve(column_definitions.size());

      for (const auto &elem : column_definitions) {
        column_names.push_back(elem.first);
      }

      return column_names;
    }

    const std::string CalibrationQueryGenerator::_generate_select_columns(const std::map<std::string, CalibrationColumnSpecification>& column_definitions) {
      auto column_names = CalibrationQueryGenerator::_get_column_names(column_definitions);

      // Random device for shuffling
      std::random_device rd;
      std::mt19937 g(rd());
      std::shuffle(column_names.begin(), column_names.end(), g);

      // Random device for sampling
      std::random_device random_device;
      std::mt19937 engine{random_device()};
      std::uniform_int_distribution<long> dist(0, column_names.size() - 1);

      long number_of_columns = dist(engine);
      if (number_of_columns == 0) return "*";

      std::vector<std::string> out;
      std::sample(column_names.begin(), column_names.end(), std::back_inserter(out), number_of_columns, std::mt19937{std::random_device{}()});
      return boost::algorithm::join(out, ", ");
    }
}