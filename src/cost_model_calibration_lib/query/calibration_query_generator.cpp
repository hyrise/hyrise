#include "calibration_query_generator.hpp"

#include <algorithm>
#include <random>

#include "../configuration/calibration_table_specification.hpp"
#include "calibration_query_generator_aggregates.hpp"
#include "calibration_query_generator_join.hpp"
#include "calibration_query_generator_predicate.hpp"
#include "calibration_query_generator_projection.hpp"
#include "expression/expression_functional.hpp"
#include "logical_query_plan/validate_node.hpp"

namespace opossum {

CalibrationQueryGenerator::CalibrationQueryGenerator(
    const std::vector<std::pair<std::string, size_t>>& table_names,
    const std::vector<CalibrationColumnSpecification>& column_specifications,
    const CalibrationConfiguration& configuration)
    : _column_specifications(column_specifications), _configuration(configuration), _table_names(table_names) {}

const std::vector<CalibrationQueryGeneratorPredicateConfiguration>
CalibrationQueryGenerator::_generate_predicate_permutations() const {
  std::vector<CalibrationQueryGeneratorPredicateConfiguration> output{};

  for (const auto& encoding : _configuration.encodings) {
    for (const auto& data_type : _configuration.data_types) {
      for (const auto& selectivity : _configuration.selectivities) {
        for (const auto& table_name : _table_names) {
          output.push_back({table_name.first, encoding, data_type, selectivity, false, table_name.second});
          output.push_back({table_name.first, encoding, data_type, selectivity, true, table_name.second});
        }
      }
    }
  }

  std::cout << "Generated " << output.size() << " Permutations for Predicates" << std::endl;

      return output;
}

const std::vector<std::shared_ptr<AbstractLQPNode>> CalibrationQueryGenerator::generate_queries() const {
  std::vector<std::shared_ptr<AbstractLQPNode>> queries;

  const auto& add_queries_if_present = [&](std::vector<std::shared_ptr<AbstractLQPNode>>& existing_queries,
                                          const std::vector<std::shared_ptr<AbstractLQPNode>>& new_queries) {
      for (const auto& query : new_queries) {
        if (query) {
          existing_queries.push_back(query);
        }
      }
  };

//  for (const auto& table_name : _table_names) {
//    add_queries_if_present(queries, _generate_aggregate(table_name));
//  }

  const auto permutations = _generate_predicate_permutations();
  for (const auto& permutation : permutations) {
    add_queries_if_present(
            queries,
        _generate_table_scan(permutation, CalibrationQueryGeneratorPredicate::generate_predicate_column_value));

//    add_queries_if_present(
//            queries,
//        _generate_table_scan(permutation, CalibrationQueryGeneratorPredicate::generate_predicate_column_column));

//    add_queries_if_present(
//            queries,
//        _generate_table_scan(permutation, CalibrationQueryGeneratorPredicate::generate_predicate_between_value_value));

//    add_queries_if_present(queries, _generate_table_scan(permutation,
//                                      CalibrationQueryGeneratorPredicate::generate_predicate_between_column_column));
//
//    add_queries_if_present(queries, _generate_table_scan(permutation, CalibrationQueryGeneratorPredicate::generate_predicate_like));
//
//    add_queries_if_present(queries,
//                         _generate_table_scan(permutation, CalibrationQueryGeneratorPredicate::generate_predicate_or));
//
//    add_queries_if_present(queries,
//        _generate_table_scan(permutation, CalibrationQueryGeneratorPredicate::generate_predicate_equi_on_strings));
  }

//  for (const auto& left_table_name : _table_names) {
//    for (const auto& right_table_name : _table_names) {
//      CalibrationQueryGeneratorJoinConfiguration join_configuration{left_table_name, right_table_name,
//                                                                    EncodingType::Unencoded, DataType::Int, false};
      // Generates the same query using all available JoinTypes
//      const auto& join_queries = _generate_join(join_configuration, left_table_name, right_table_name);
//      add_queries_if_present(queries, join_queries);
//    }
//  }

  std::cout << "Generated " << queries.size() << " queries." << std::endl;

  return queries;
}

const std::vector<std::shared_ptr<AbstractLQPNode>> CalibrationQueryGenerator::_generate_table_scan(
    const CalibrationQueryGeneratorPredicateConfiguration& configuration,
    const PredicateGeneratorFunctor& predicate_generator) const {
  const auto table = StoredTableNode::make(configuration.table_name);

  const auto predicates = CalibrationQueryGeneratorPredicate::generate_predicates(predicate_generator, _column_specifications,
                                                                           table, configuration);
  // TODO(Sven): Add test
  if (predicates.empty()) return {};

  for (const auto& predicate_node : predicates) {
    if (configuration.reference_column) {
      const auto validate_node = ValidateNode::make();
      validate_node->set_left_input(table);
      predicate_node->set_left_input(validate_node);
    } else {
      predicate_node->set_left_input(table);
    }
  }

  return predicates;
}

const std::vector<std::shared_ptr<AbstractLQPNode>> CalibrationQueryGenerator::_generate_join(
    const CalibrationQueryGeneratorJoinConfiguration& configuration, const std::string& left_table_name,
    const std::string& right_table_name) const {
  const auto left_table = StoredTableNode::make(left_table_name);
  const auto right_table = StoredTableNode::make(right_table_name);

  const auto join_nodes = CalibrationQueryGeneratorJoin::generate_join(
      configuration, CalibrationQueryGeneratorJoin::generate_join_predicate, left_table, right_table,
      _column_specifications);

  if (join_nodes.empty()) {
    return {};
  }

  for (const auto& join_node : join_nodes) {
    if (configuration.reference_column) {
      const auto left_validate_node = ValidateNode::make();
      const auto right_validate_node = ValidateNode::make();

      left_validate_node->set_left_input(left_table);
      right_validate_node->set_left_input(right_table);

      join_node->set_left_input(left_validate_node);
      join_node->set_right_input(right_validate_node);

    } else {
      join_node->set_left_input(left_table);
      join_node->set_right_input(right_table);
    }
  }

  return join_nodes;
}

const std::vector<std::shared_ptr<AbstractLQPNode>> CalibrationQueryGenerator::_generate_aggregate(
    const std::string& table_name) const {
  const auto table = StoredTableNode::make(table_name);

  const auto aggregate_node = CalibrationQueryGeneratorAggregate::generate_aggregates();

  aggregate_node->set_left_input(table);
  return {aggregate_node};
}

const std::vector<std::shared_ptr<AbstractLQPNode>> CalibrationQueryGenerator::_generate_projection(
    const std::vector<LQPColumnReference>& columns) const {
  return {CalibrationQueryGeneratorProjection::generate_projection(columns)};
}
}  // namespace opossum
