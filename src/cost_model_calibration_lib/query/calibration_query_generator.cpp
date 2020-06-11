#include "calibration_query_generator.hpp"

#include <algorithm>
#include <random>

#include "../configuration/calibration_table_specification.hpp"
#include "calibration_query_generator_aggregates.hpp"
#include "calibration_query_generator_join.hpp"
#include "calibration_query_generator_predicate.hpp"
#include "calibration_query_generator_projection.hpp"
#include "expression/expression_functional.hpp"
#include "hyrise.hpp"
#include "logical_query_plan/validate_node.hpp"
#include "storage/storage_manager.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

CalibrationQueryGenerator::CalibrationQueryGenerator(
    const std::vector<std::pair<std::string, size_t>>& tables,
    const std::vector<CalibrationColumnSpecification>& column_specifications,
    const CalibrationConfiguration& configuration)
    : _column_specifications(column_specifications), _configuration(configuration), _tables(tables) {}

/**
 * This function generates all TableScan permutations for TPCH-6
 */
const std::vector<std::shared_ptr<AbstractLQPNode>> CalibrationQueryGenerator::generate_tpch_6() {
  std::vector<std::shared_ptr<AbstractLQPNode>> queries;

  const auto lineitem = StoredTableNode::make("lineitem");

  const auto shipdate_column = lineitem->get_column("l_shipdate");
  const auto discount_column = lineitem->get_column("l_discount");
  const auto quantity_column = lineitem->get_column("l_quantity");

  const auto shipdate_gte =
      PredicateNode::make(greater_than_equals_(shipdate_column, value_("1994-01-01")));
  const auto shipdate_lt = PredicateNode::make(less_than_(shipdate_column, value_("1995-01-01")));
  const auto discount =
      PredicateNode::make(between_inclusive_(discount_column, value_(0.05), value_(0.07001)));
  const auto quantity = PredicateNode::make(less_than_(quantity_column, value_(24)));

  std::vector<std::vector<std::shared_ptr<AbstractLQPNode>>> predicate_node_permutations = {
      {shipdate_gte, shipdate_lt, discount, quantity}, {shipdate_gte, shipdate_lt, quantity, discount},
      {shipdate_gte, discount, shipdate_lt, quantity}, {shipdate_gte, discount, quantity, shipdate_lt},
      {shipdate_gte, quantity, shipdate_lt, discount}, {shipdate_gte, quantity, discount, shipdate_lt},
      {quantity, shipdate_gte, discount, shipdate_lt}, {quantity, shipdate_gte, shipdate_lt, discount},
      {quantity, shipdate_lt, shipdate_gte, discount}, {quantity, shipdate_lt, discount, shipdate_gte},
      {quantity, discount, shipdate_lt, shipdate_gte}, {quantity, discount, shipdate_gte, shipdate_lt},
      {discount, quantity, shipdate_gte, shipdate_lt}, {discount, quantity, shipdate_lt, shipdate_gte},
      {discount, shipdate_lt, quantity, shipdate_gte}, {discount, shipdate_lt, shipdate_gte, quantity},
      {discount, shipdate_gte, shipdate_lt, quantity}, {discount, shipdate_gte, quantity, shipdate_lt},
      {shipdate_lt, discount, shipdate_gte, quantity}, {shipdate_lt, discount, quantity, shipdate_gte},
      {shipdate_lt, quantity, discount, shipdate_gte}, {shipdate_lt, quantity, shipdate_gte, discount},
      {shipdate_lt, shipdate_gte, discount, quantity}, {shipdate_lt, shipdate_gte, quantity, discount}};

  for (const auto& permutation : predicate_node_permutations) {
    std::shared_ptr<AbstractLQPNode> previous_node = lineitem;
    for (const auto& node : permutation) {
      const auto copied_node = node->deep_copy({{lineitem, lineitem}});
      copied_node->set_left_input(previous_node);
      previous_node = copied_node;
    }

    queries.push_back(previous_node);
  }

  return queries;
}

/**
 * This function generates all TableScan permutations for TPCH-12
 */
const std::vector<std::shared_ptr<AbstractLQPNode>> CalibrationQueryGenerator::generate_tpch_12() {
  std::vector<std::shared_ptr<AbstractLQPNode>> queries;

  const auto lineitem = StoredTableNode::make("lineitem");

  const auto receiptdate_column = lineitem->get_column("l_receiptdate");
  const auto commitdate_column = lineitem->get_column("l_commitdate");
  const auto shipdate_column = lineitem->get_column("l_shipdate");

  const auto shipdate_gte =
      PredicateNode::make(greater_than_equals_(receiptdate_column, value_("1994-01-01")));
  const auto shipdate_lt = PredicateNode::make(less_than_(receiptdate_column, value_("1995-01-01")));
  const auto discount =
      PredicateNode::make(less_than_(commitdate_column, receiptdate_column));
  const auto quantity = PredicateNode::make(less_than_(shipdate_column, commitdate_column));

  std::vector<std::vector<std::shared_ptr<AbstractLQPNode>>> predicate_node_permutations = {
      {shipdate_gte, shipdate_lt, discount, quantity}, {shipdate_gte, shipdate_lt, quantity, discount},
      {shipdate_gte, discount, shipdate_lt, quantity}, {shipdate_gte, discount, quantity, shipdate_lt},
      {shipdate_gte, quantity, shipdate_lt, discount}, {shipdate_gte, quantity, discount, shipdate_lt},
      {quantity, shipdate_gte, discount, shipdate_lt}, {quantity, shipdate_gte, shipdate_lt, discount},
      {quantity, shipdate_lt, shipdate_gte, discount}, {quantity, shipdate_lt, discount, shipdate_gte},
      {quantity, discount, shipdate_lt, shipdate_gte}, {quantity, discount, shipdate_gte, shipdate_lt},
      {discount, quantity, shipdate_gte, shipdate_lt}, {discount, quantity, shipdate_lt, shipdate_gte},
      {discount, shipdate_lt, quantity, shipdate_gte}, {discount, shipdate_lt, shipdate_gte, quantity},
      {discount, shipdate_gte, shipdate_lt, quantity}, {discount, shipdate_gte, quantity, shipdate_lt},
      {shipdate_lt, discount, shipdate_gte, quantity}, {shipdate_lt, discount, quantity, shipdate_gte},
      {shipdate_lt, quantity, discount, shipdate_gte}, {shipdate_lt, quantity, shipdate_gte, discount},
      {shipdate_lt, shipdate_gte, discount, quantity}, {shipdate_lt, shipdate_gte, quantity, discount}};

  for (const auto& permutation : predicate_node_permutations) {
    std::shared_ptr<AbstractLQPNode> previous_node = lineitem;
    for (const auto& node : permutation) {
      const auto copied_node = node->deep_copy({{lineitem, lineitem}});
      copied_node->set_left_input(previous_node);
      previous_node = copied_node;
    }

    queries.push_back(previous_node);
  }

  return queries;
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

  if (_configuration.calibrate_scans) {
    /**
     * ColumnValue scans
     */
    for (const auto& [table_name, table_size] : _tables) {
      auto& sm = Hyrise::get().storage_manager;
      const auto table = sm.get_table(table_name);

      for (const auto& column_spec : _configuration.columns) {
        for (const auto selectivity : _configuration.selectivities) {
          for (const auto scan_type : {ScanType::TableScan, ScanType::IndexScan}) {
            // IndexScans are currently not supported on reference segments and indexes are only created for
            // dictionary-encoded FSBA columns (not a Hyrise restriction, just considered sufficent for calibration).
            // Consequently, we skip queries where the data table scan would be an index scan on other segment encodings.
            if (scan_type == ScanType::IndexScan && !(column_spec.encoding.encoding_type == EncodingType::Dictionary &&
                *column_spec.encoding.vector_compression_type == VectorCompressionType::FixedSizeByteAligned)) {
              continue;
            }

            const auto table_node = StoredTableNode::make(table_name);

            const auto data_table_predicate = CalibrationQueryGeneratorPredicate::generate_concrete_predicate_column_value(
                table_node, column_spec, selectivity);

            for (const auto reference_segment_predicate_selectivity : _configuration.selectivities) {
              if (selectivity <= reference_segment_predicate_selectivity) {
                // A second predicate selectivity >= the first predicate's selectivy would not throw away any rows
                // (since we execute both predicates on the same column as of now), but blows up the query count.
                continue;  
              }

              /**
               * To cover a broad range of filter and validate placements, we yield two queries here.
               * First, a validate on a data table is created with a following predicate (we gain a validate
               * benchmark on a full table and a filter on a "full-sized" pos list) and a final validate.
               * Second, we create two filters (first on a data table), second on a not "full-sized" pos list
               * with a following validate (now, on a rather small pos list).
               */
              const auto reference_table_predicate = CalibrationQueryGeneratorPredicate::generate_concrete_predicate_column_value(
                table_node, column_spec, reference_segment_predicate_selectivity);

              queries.push_back(
                _generate_query_for_validate_and_predicate(table_node, reference_table_predicate, scan_type));
              queries.push_back(
                _generate_query_for_predicate_chain_with_validate(table_node, data_table_predicate, reference_table_predicate, scan_type));

              if (column_spec.data_type == DataType::String && scan_type == ScanType::TableScan) {
                // StringPredicateType::Equality is called as the default
                for (const auto like_type : {StringPredicateType::TrailingLike, StringPredicateType::PrecedingLike}) {
                  const auto like_predicate =
                      CalibrationQueryGeneratorPredicate::generate_concrete_predicate_column_value(
                          table_node, column_spec, selectivity, like_type);
                  queries.push_back(_generate_query_for_predicate_chain_with_validate(table_node, like_predicate, reference_table_predicate, scan_type));
                  queries.push_back(_generate_query_for_predicate_chain_with_validate(table_node, data_table_predicate, like_predicate, scan_type));
                }
              }
            }
          }
        }
      }
    }

    auto permutations = CalibrationQueryGeneratorPredicate::generate_predicate_permutations(_tables, _configuration);

    for (const auto& permutation : permutations) {
      // Reduce number of generated queries for Scans on only one or two columns.
      // We don't need to generate a query for permutation of the (unused) third column.

      // Expect one encoding
      if (!permutation.second_encoding) {
        add_queries_if_present(
            queries,
            _generate_table_scan(permutation, CalibrationQueryGeneratorPredicate::generate_predicate_column_value));

        add_queries_if_present(
            queries, _generate_table_scan(permutation,
                                          CalibrationQueryGeneratorPredicate::generate_predicate_between_value_value));

        if (permutation.data_type == DataType::String) {
          add_queries_if_present(
              queries, _generate_table_scan(permutation, CalibrationQueryGeneratorPredicate::generate_predicate_like));

          add_queries_if_present(
              queries, _generate_table_scan(permutation,
                                            CalibrationQueryGeneratorPredicate::generate_predicate_equi_on_strings));
        }

        add_queries_if_present(
            queries, _generate_table_scan(permutation, CalibrationQueryGeneratorPredicate::generate_predicate_or));
      }

      // // Expect two encodings
      // if (permutation.second_encoding && !permutation.third_encoding) {
      //     add_queries_if_present(
      //             queries,
      //             _generate_table_scan(permutation, CalibrationQueryGeneratorPredicate::generate_predicate_column_column));
      // }

      // // Expect three encodings
      // if (permutation.second_encoding && permutation.third_encoding) {
      //     add_queries_if_present(
      //             queries, _generate_table_scan(permutation,
      //                                           CalibrationQueryGeneratorPredicate::generate_predicate_between_column_column));
      // }
    }
  }

  if (_configuration.calibrate_joins) {
    const auto join_permutations = CalibrationQueryGeneratorJoin::generate_join_permutations(_tables, _configuration);
    for (const auto& permutation : join_permutations) {
      const auto& join_queries = _generate_join(permutation);
      add_queries_if_present(queries, join_queries);
    }
  }

  std::cout << "Generated " << queries.size() << " queries." << std::endl;

  std::random_device rd;
  std::mt19937 g(rd());
  std::shuffle(queries.begin(), queries.end(), g);

  return queries;
}

const std::shared_ptr<AbstractLQPNode> CalibrationQueryGenerator::_generate_query_for_predicate_chain_with_validate(
    const std::shared_ptr<StoredTableNode> table_node, const std::shared_ptr<AbstractExpression> data_table_predicate,
    const std::shared_ptr<AbstractExpression> reference_table_predicate, const ScanType scan_type) const {
  const auto data_table_predicate_node =
      CalibrationQueryGeneratorPredicate::generate_concreate_scan_predicate(data_table_predicate, scan_type);

  // For scans on reference segements, we never create IndexScans.
  const auto reference_table_predicate_node =
      CalibrationQueryGeneratorPredicate::generate_concreate_scan_predicate(reference_table_predicate, ScanType::TableScan);

  const auto validate_node = ValidateNode::make();

  data_table_predicate_node->set_left_input(table_node);
  reference_table_predicate_node->set_left_input(data_table_predicate_node);
  validate_node->set_left_input(reference_table_predicate_node);

  return validate_node;
}

const std::shared_ptr<AbstractLQPNode> CalibrationQueryGenerator::_generate_query_for_validate_and_predicate(
      const std::shared_ptr<StoredTableNode> table_node, const std::shared_ptr<AbstractExpression> reference_table_predicate,
      const ScanType scan_type) const {
  const auto validate_node_begin = ValidateNode::make();
  const auto validate_node_end = ValidateNode::make();

  const auto reference_table_predicate_node =
      CalibrationQueryGeneratorPredicate::generate_concreate_scan_predicate(reference_table_predicate, ScanType::TableScan);

  validate_node_begin->set_left_input(table_node);
  reference_table_predicate_node->set_left_input(validate_node_begin);
  validate_node_end->set_left_input(reference_table_predicate_node);

  return validate_node_end;
}

const std::vector<std::shared_ptr<AbstractLQPNode>> CalibrationQueryGenerator::_generate_table_scan(
    const CalibrationQueryGeneratorPredicateConfiguration& configuration,
    const PredicateGeneratorFunctor& predicate_generator) const {
  const auto table = StoredTableNode::make(configuration.table_name);

  const auto predicates = CalibrationQueryGeneratorPredicate::generate_predicates(
      predicate_generator, _column_specifications, table, configuration);

  // TODO(Sven): Add test
  if (predicates.empty()) {
    return {};
  }

  std::vector<std::shared_ptr<AbstractLQPNode>> output;

  // Use additional ValidateNode to force a reference-segment TableScan
  for (const auto& predicate_node : predicates) {
    if (configuration.reference_column) {
      const auto validate_node = ValidateNode::make();
      validate_node->set_left_input(table);
      predicate_node->set_left_input(validate_node);
    } else {
      predicate_node->set_left_input(table);
    }

    output.push_back(predicate_node);
  }

  return output;
}

const std::vector<std::shared_ptr<AbstractLQPNode>> CalibrationQueryGenerator::_generate_join(
    const CalibrationQueryGeneratorJoinConfiguration& configuration) const {
  const auto left_table = StoredTableNode::make(configuration.left_table_name);
  const auto right_table = StoredTableNode::make(configuration.right_table_name);

  const auto join_nodes =
      CalibrationQueryGeneratorJoin{configuration, _column_specifications}.generate_join(left_table, right_table);

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
    const std::vector<std::shared_ptr<LQPColumnExpression>>& columns) const {
  return {CalibrationQueryGeneratorProjection::generate_projection(columns)};
}
}  // namespace opossum
