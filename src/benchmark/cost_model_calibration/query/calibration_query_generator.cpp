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
//        queries.push_back(CalibrationQueryGenerator::_generate_aggregate(table_definition));
        queries.push_back(CalibrationQueryGenerator::_generate_table_scan(table_definition));
      }

      queries.push_back(CalibrationQueryGenerator::_generate_join(table_definitions));

      return queries;
    }

    const std::string CalibrationQueryGenerator::_generate_join(const std::vector<CalibrationTableSpecification>& table_definitions) {
      // Both Join Inputs are filtered randomly beforehand
      auto string_template = "SELECT %1% FROM %2% l JOIN %3% r ON l.%4%=r.%5% WHERE %6% AND %7%;";

      std::random_device random_device;
      std::mt19937 engine{random_device()};
      std::uniform_int_distribution<long> table_dist(0, table_definitions.size() - 1);

      auto left_table = std::next(table_definitions.begin(), table_dist(engine));
      auto right_table = std::next(table_definitions.begin(), table_dist(engine));

      std::map<std::string, CalibrationColumnSpecification> columns;

      for (const auto &column : left_table->columns) {
        const auto column_name = "l." + column.first;
        columns.insert(std::pair<std::string, CalibrationColumnSpecification>(column_name, column.second));
      }

      for (const auto &column : right_table->columns) {
        const auto column_name = "r." + column.first;
        columns.insert(std::pair<std::string, CalibrationColumnSpecification>(column_name, column.second));
      }

      auto left_join_column = "column_a";
      auto right_join_column = "column_a";

      auto left_predicate = _generate_predicate(left_table->columns, "l.");
      auto right_predicate = _generate_predicate(right_table->columns, "r.");

      auto select_columns = _generate_select_columns(columns);
      return boost::str(
              boost::format(string_template)
              % select_columns
              % left_table->table_name
              % right_table->table_name
              % left_join_column
              % right_join_column
              % left_predicate
              % right_predicate);
    }

    const std::string CalibrationQueryGenerator::_generate_predicate(
            const std::map<std::string, CalibrationColumnSpecification>& column_definitions,
            const std::string column_name_prefix = ""
            ) {
      std::random_device random_device;
      std::mt19937 engine{random_device()};
      std::uniform_int_distribution<long> filter_column_dist(0, column_definitions.size() - 1);

      auto predicate_template =  "%1% %2% %3%";

      auto filter_column = std::next(column_definitions.begin(), filter_column_dist(engine));
      auto filter_column_name = column_name_prefix + filter_column->first;
      auto filter_column_value = _generate_table_scan_predicate_value(filter_column->second);

      // We only want to measure various selectivities.
      // It shouldn't be that important whether we have Point or Range Lookups.
      // Isn't it?

      // At the same time this makes sure that the probability of having empty intermediate results is reduced.
      auto predicate_sign = "<=";
      return boost::str(boost::format(predicate_template) % filter_column_name % predicate_sign % filter_column_value);
    }

    const std::string CalibrationQueryGenerator::_generate_aggregate(const CalibrationTableSpecification& table_definition) {
      std::random_device random_device;
      std::mt19937 engine{random_device()};

      auto string_template = "SELECT COUNT(*) FROM %1% %2%;";

      auto table_name = table_definition.table_name;

      auto column_definitions = table_definition.columns;

      std::uniform_int_distribution<size_t> number_of_predicates_dist(0, 3);
      auto number_of_predicates = number_of_predicates_dist(engine);

      std::stringstream predicate_stream;

      if (number_of_predicates > 0) {
        predicate_stream << " WHERE ";
      }

      for (size_t i = 0; i < number_of_predicates; i++) {
        predicate_stream << _generate_predicate(column_definitions);

        if (i < number_of_predicates - 1) {
          predicate_stream << " AND ";
        }
      }

      return boost::str(boost::format(string_template) % table_name % predicate_stream.str());
    }

    const std::string CalibrationQueryGenerator::_generate_table_scan(const CalibrationTableSpecification& table_definition) {
      std::random_device random_device;
      std::mt19937 engine{random_device()};

      auto string_template = "SELECT %1% FROM %2% WHERE %3%;";

      auto select_columns = _generate_select_columns(table_definition.columns);
      auto table_name = table_definition.table_name;

      auto column_definitions = table_definition.columns;

      std::uniform_int_distribution<size_t> number_of_predicates_dist(1, 3);
      auto number_of_predicates = number_of_predicates_dist(engine);
//      size_t number_of_predicates = 1;

      std::stringstream predicate_stream;

      for (size_t i = 0; i < number_of_predicates; i++) {
        predicate_stream << _generate_predicate(column_definitions);

        if (i < number_of_predicates - 1) {
          predicate_stream << " AND ";
        }
      }

      return boost::str(boost::format(string_template) % select_columns % table_name % predicate_stream.str());
    }

    const std::string CalibrationQueryGenerator::_generate_table_scan_predicate_value(
            const CalibrationColumnSpecification &column_definition) {
      auto column_type = column_definition.type;
      std::random_device random_device;
      std::mt19937 engine{random_device()};
      std::uniform_int_distribution<int> int_dist(0, column_definition.distinct_values - 1);
      std::uniform_real_distribution<> float_dist(0, column_definition.distinct_values - 1);
//      std::uniform_int_distribution<> char_dist(0, UCHAR_MAX);

      if (column_type == "int") return std::to_string(int_dist(engine));
      if (column_type == "string") return "'" + std::string(1, 'a' + rand()%26) + "'";
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