#include "calibration_lqp_generator.hpp"

#include "expression/expression_functional.hpp"
#include "hyrise.hpp"
#include "logical_query_plan/aggregate_node.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/validate_node.hpp"
#include "operators/join_hash.hpp"
#include "operators/operator_join_predicate.hpp"
#include "optimizer/strategy/chunk_pruning_rule.hpp"
#include "optimizer/strategy/column_pruning_rule.hpp"
#include "storage/table.hpp"
#include "synthetic_table_generator.hpp"

using opossum::expression_functional::between_inclusive_;
using opossum::expression_functional::equals_;
using opossum::expression_functional::greater_than_;
using opossum::expression_functional::greater_than_equals_;
using opossum::expression_functional::is_not_null_;
using opossum::expression_functional::is_null_;
using opossum::expression_functional::less_than_;
using opossum::expression_functional::like_;
using opossum::expression_functional::min_;
using opossum::expression_functional::not_in_;
using opossum::expression_functional::sum_;

namespace opossum {
void CalibrationLQPGenerator::generate(OperatorType operator_type,
                                       const std::shared_ptr<const CalibrationTableWrapper>& table) {
  switch (operator_type) {
    case OperatorType::TableScan:
      _generate_table_scans(table);
      return;
    default:
      break;
  }

  Fail("Not implemented yet: Only TableScans are currently supported.");
}

void CalibrationLQPGenerator::generate_joins(std::vector<std::shared_ptr<const CalibrationTableWrapper>>& tables,
                                             const float scale_factor) {
  const auto table_size = static_cast<size_t>(scale_factor * 6'000'000);
  const auto table_suffix = "65535_" + std::to_string(table_size);
  for (const auto& table : tables) {
    if (table->get_name().find(table_suffix) != std::string::npos) {
      for (const auto& other_table : tables) {
        if (other_table->get_name().find("1500000") == std::string::npos) {
          _generate_joins(table, other_table);
        }
      }
    }
  }
}

void CalibrationLQPGenerator::_generate_joins(const std::shared_ptr<const CalibrationTableWrapper>& left,
                                              const std::shared_ptr<const CalibrationTableWrapper>& right) {
  // we want steps of one chunk for the probe side
  const auto probe_selectivity_resolution = std::min(left->get_table()->chunk_count(), ChunkID{10});
  constexpr uint32_t build_selectivity_resolution = 5;
  const auto left_stored_table_node = StoredTableNode::make(left->get_name());
  const auto right_stored_table_node = StoredTableNode::make(right->get_name());
  const auto& left_table = left->get_table();
  const auto& right_table = right->get_table();

  const auto column_count = left_table->column_count();
  const auto right_column_count = right_table->column_count();
  const auto left_column_data_types = left_table->column_data_types();
  const auto right_column_data_types = right_table->column_data_types();
  const std::vector<std::string> left_column_names = left_table->column_names();
  const std::vector<std::string> right_column_names = right_table->column_names();

  const auto join_modes = {JoinMode::Inner, JoinMode::Left,           JoinMode::Right,          JoinMode::FullOuter,
                           JoinMode::Semi,  JoinMode::AntiNullAsTrue, JoinMode::AntiNullAsFalse};

  for (ColumnID column_id = ColumnID{0}; column_id < column_count; ++column_id) {
    resolve_data_type(left_column_data_types[column_id], [&](const auto column_data_type) {
      using ColumnDataType = typename decltype(column_data_type)::type;
      if (std::is_same<ColumnDataType, int32_t>::value || std::is_same<ColumnDataType, float>::value) {
        const auto left_column = left_stored_table_node->get_column(left_column_names[column_id]);

        for (ColumnID right_column_id = ColumnID{0}; right_column_id < right_column_count; ++right_column_id) {
          resolve_data_type(right_column_data_types[right_column_id], [&](const auto right_column_data_type) {
            using RightColumnDataType = typename decltype(right_column_data_type)::type;

            if (!std::is_same<ColumnDataType, RightColumnDataType>::value) {
              return;
            }
            const auto right_column = right_stored_table_node->get_column(right_column_names[right_column_id]);
            const auto probe_distribution = left->get_column_data_distribution(column_id);
            const auto build_distribution = right->get_column_data_distribution(right_column_id);
            // clang-format off
            const auto probe_step_size = (probe_distribution.max_value - probe_distribution.min_value) / probe_selectivity_resolution;  // NOLINT
            const auto build_step_size = (build_distribution.max_value - build_distribution.min_value) / build_selectivity_resolution;  // NOLINT
            // clang-format on

            const auto left_column_expression =
                std::make_shared<LQPColumnExpression>(left_stored_table_node, column_id);
            const auto right_column_expression =
                std::make_shared<LQPColumnExpression>(right_stored_table_node, right_column_id);
            const auto join_predicate = std::make_shared<BinaryPredicateExpression>(
                PredicateCondition::Equals, left_column_expression, right_column_expression);

            if (OperatorJoinPredicate::from_expression(*join_predicate, *left_stored_table_node,
                                                       *right_stored_table_node)) {
              for (const auto& join_mode : join_modes) {
                if (JoinHash::supports({join_mode, PredicateCondition::Equals, left_table->column_data_type(column_id),
                                        right_table->column_data_type(right_column_id), false})) {
                  for (uint32_t selectivity_step = 0; selectivity_step <= probe_selectivity_resolution;
                       selectivity_step++) {
                    // in this for-loop we iterate up and go from 100% selectivity to 0% by increasing the lower_bound in steps
                    // for any step there

                    // Get value for current iteration
                    // the cursor is a int representation of where the column has to be cut in this iteration
                    const auto step_cursor = static_cast<uint32_t>(selectivity_step * probe_step_size);

                    const ColumnDataType probe_lower_bound =
                        SyntheticTableGenerator::generate_value<ColumnDataType>(step_cursor);

                    for (uint32_t step = 0; step <= build_selectivity_resolution; step++) {
                      const ColumnDataType build_lower_bound = SyntheticTableGenerator::generate_value<ColumnDataType>(
                          static_cast<uint32_t>(step * build_step_size));
                      const auto right_subtree = _get_predicate_node_based_on(
                          right_column, build_lower_bound, std::shared_ptr<AbstractLQPNode>(right_stored_table_node));

                      // scan before join
                      auto left_subtree_scan = _get_predicate_node_based_on(
                          left_column, probe_lower_bound, std::shared_ptr<AbstractLQPNode>(left_stored_table_node));
                      auto join_node =
                          JoinNode::make(join_mode, join_predicate, JoinType::Hash, left_subtree_scan, right_subtree);

                      auto optimized_lqp = join_node->deep_copy();
                      optimized_lqp = _optimizer->optimize(std::move(optimized_lqp));
                      _generated_lqps.emplace_back(optimized_lqp);

                      //scan after join
                      auto join_node_data = JoinNode::make(join_mode, join_predicate, JoinType::Hash,
                                                           left_stored_table_node, right_subtree);

                      auto scan_after_join =
                          _get_predicate_node_based_on(left_column, probe_lower_bound, join_node_data);
                      auto optimized_lqp_data_reference = scan_after_join->deep_copy();
                      optimized_lqp_data_reference = _optimizer->optimize(std::move(optimized_lqp_data_reference));
                      _generated_lqps.emplace_back(optimized_lqp_data_reference);
                    }
                  }
                }
              }
            }
          });
        }
      }
    });
  }
}

void CalibrationLQPGenerator::generate_aggregates(
    const std::vector<std::shared_ptr<const CalibrationTableWrapper>>& table_wrappers) {
  auto optimizer = std::make_shared<Optimizer>();
  optimizer->add_rule(std::make_unique<ColumnPruningRule>());

  for (const auto& table_wrapper : table_wrappers) {
    if (table_wrapper->get_name().find("aggregate") == std::string::npos) {
      continue;
    }

    const auto table_node = StoredTableNode::make(table_wrapper->get_name());
    const auto validate_node = ValidateNode::make(table_node);

    const auto group_by_column_expression = std::make_shared<LQPColumnExpression>(table_node, ColumnID{0});  // id
    const auto aggregate_column_expression =
        std::make_shared<LQPColumnExpression>(table_node, ColumnID{2});  // quantity

    constexpr size_t MAX_AGGREGATE_COUNT = 5;
    for (size_t aggregate_count = 1; aggregate_count <= MAX_AGGREGATE_COUNT; aggregate_count++) {
      std::vector<std::shared_ptr<AbstractExpression>> aggregate_expressions;
      for (size_t aggregate_id = 0; aggregate_id < aggregate_count; aggregate_id++) {
        aggregate_expressions.push_back(sum_(aggregate_column_expression));
      }

      const auto aggregate_node =
          AggregateNode::make(std::vector<std::shared_ptr<AbstractExpression>>{group_by_column_expression},
                              aggregate_expressions, validate_node);

      auto optimized_aggregate = aggregate_node->deep_copy();
      optimized_aggregate = optimizer->optimize(std::move(optimized_aggregate));

      constexpr size_t NUM_AGGREGATE_EXECUTIONS = 5;
      for (auto execution = 0ul; execution < NUM_AGGREGATE_EXECUTIONS; execution++) {
        _generated_lqps.push_back(optimized_aggregate);
      }
    }
  }
}

void CalibrationLQPGenerator::_generate_table_scans(
    const std::shared_ptr<const CalibrationTableWrapper>& table_wrapper) {
  // do not generate scans on aggregate tables
  if (table_wrapper->get_name().find("aggregate") != std::string::npos) {
    return;
  }
  if (table_wrapper->get_name().find("semi_join") != std::string::npos) {
    return;
  }
  if (table_wrapper->get_name().find("unordered_probe") != std::string::npos) {
    return;
  }
  if (table_wrapper->get_name().find("ordered_build") != std::string::npos) {
    return;
  }

  // std::cout << "scans for " << table_wrapper->get_name() << std::endl;

  // selectivity resolution determines in how many LQPs with a different selectivity are generated
  // increase this value for providing more training data to the model
  // The resulting LQPs are equally distributed from 0% to 100% selectivity.
  constexpr uint32_t selectivity_resolution = 100;

  // for every table scan gets executed on the raw data as well as referenceSegments
  // reference_scan_selectivity_resolution determines how many of these scans on referenceSegments are generated
  // in addition to the raw scan.
  // The resulting referenceSegments reduce the selectivity of the original scan by 0% to 100% selectivity
  // in equally sized steps.
  constexpr uint32_t reference_scan_selectivity_resolution = 10;

  const auto stored_table_node = StoredTableNode::make(table_wrapper->get_name());

  const auto column_count = table_wrapper->get_table()->column_count();
  const std::vector<std::string> column_names = table_wrapper->get_table()->column_names();
  const auto column_data_types = table_wrapper->get_table()->column_data_types();

  for (ColumnID column_id = ColumnID{0}; column_id < column_count; ++column_id) {
    // Column specific values
    const auto column = stored_table_node->get_column(column_names[column_id]);
    const auto distribution = table_wrapper->get_column_data_distribution(column_id);
    const auto step_size = (distribution.max_value - distribution.min_value) / selectivity_resolution;

    // std::cout << "\tcolumn " << column_names[column_id] << std::endl;

    if (_enable_column_vs_column_scans) {
      _generate_column_vs_column_scans(table_wrapper);
    }

    resolve_data_type(column_data_types[column_id], [&](const auto column_data_type) {
      using ColumnDataType = typename decltype(column_data_type)::type;
      const auto min_val =
          SyntheticTableGenerator::generate_value<ColumnDataType>(static_cast<int>(distribution.min_value));
      for (uint32_t selectivity_step = 0; selectivity_step < selectivity_resolution; selectivity_step++) {
        // in this for-loop we iterate up and go from 100% selectivity to 0% by increasing the lower_bound in steps
        // for any step there

        // Get value for current iteration
        // the cursor is a int representation of where the column has to be cut in this iteration
        const auto step_cursor = static_cast<uint32_t>(selectivity_step * step_size);

        const ColumnDataType lower_bound = SyntheticTableGenerator::generate_value<ColumnDataType>(step_cursor);

        // std::cout << "\tstep " << selectivity_step << "\tcursor " << step_cursor << "\tlower bound " << lower_bound << std::endl;

        // Base LQP for current iteration (without any further modifications)
        auto base_scan =
            _get_predicate_node_based_on(column, lower_bound, std::shared_ptr<AbstractLQPNode>(stored_table_node));
        // _generated_lqps.emplace_back(base_scan);
        auto optimized_scan = base_scan->deep_copy();
        optimized_scan = _optimizer->optimize(std::move(optimized_scan));
        _generated_lqps.emplace_back(base_scan);
        _generated_lqps.emplace_back(optimized_scan);

        // Add reference scans
        if (_enable_reference_scans) {
          // generate reference scans to base original LQP on
          // that reduce the overall selectivity stepwise
          const double reference_scan_step_size =
              (distribution.max_value - step_cursor) / reference_scan_selectivity_resolution;
          for (uint32_t step = 0; step < reference_scan_selectivity_resolution; step++) {
            const ColumnDataType upper_bound = SyntheticTableGenerator::generate_value<ColumnDataType>(
                static_cast<uint32_t>(step * reference_scan_step_size));
            auto reference_scan = _get_predicate_node_based_on(
                column, lower_bound, PredicateNode::make(less_than_(column, upper_bound), stored_table_node));
            // _generated_lqps.emplace_back(reference_scan);
            auto optimized_reference_scan = reference_scan->deep_copy();
            optimized_reference_scan = _optimizer->optimize(std::move(optimized_reference_scan));
            _generated_lqps.emplace_back(optimized_reference_scan);

            // add reference scan with full pos list
            _generated_lqps.emplace_back(_get_predicate_node_based_on(
                column, lower_bound, PredicateNode::make(greater_than_equals_(column, min_val), stored_table_node)));
            // add reference scan with empty pos list
            _generated_lqps.emplace_back(_get_predicate_node_based_on(
                column, lower_bound, PredicateNode::make(less_than_(column, min_val), stored_table_node)));

            if (_enable_between_predicates) {
              auto between_scan =
                  PredicateNode::make(between_inclusive_(column, lower_bound, upper_bound), stored_table_node);
              // _generated_lqps.emplace_back(between_scan);
              auto optimized_between_scan = between_scan->deep_copy();
              optimized_between_scan = _optimizer->optimize(std::move(optimized_between_scan));
              _generated_lqps.emplace_back(optimized_between_scan);
              auto between_reference_scan =
                  PredicateNode::make(between_inclusive_(column, lower_bound, upper_bound),
                                      PredicateNode::make(less_than_(column, upper_bound), stored_table_node));
              // _generated_lqps.emplace_back(between_scan);
              auto optimized_between_reference_scan = between_reference_scan->deep_copy();
              optimized_between_reference_scan = _optimizer->optimize(std::move(optimized_between_reference_scan));
              _generated_lqps.emplace_back(optimized_between_reference_scan);
            }
          }
        }
      }

      // LIKE and IN predicates for strings
      if (_enable_like_predicates && std::is_same<ColumnDataType, pmr_string>::value) {
        for (uint32_t step = 0; step < 10; step++) {
          auto const upper_bound = (SyntheticTableGenerator::generate_value<pmr_string>(step));
          _generated_lqps.emplace_back(PredicateNode::make(like_(column, upper_bound + "%"), stored_table_node));
        }

        // 100% selectivity
        _generated_lqps.emplace_back(PredicateNode::make(like_(column, "%"), stored_table_node));
        // 0% selectivity
        _generated_lqps.emplace_back(PredicateNode::make(like_(column, "%not_there%"), stored_table_node));
      }
    });
  }
}

std::vector<CalibrationLQPGenerator::ColumnPair> CalibrationLQPGenerator::_get_column_pairs(
    const std::shared_ptr<const CalibrationTableWrapper>& table_wrapper) const {
  /*
   * ColumnVsColumn Scans occur when the value of a predicate is a column.
   * In this case every value from one column has to be compared to every value of the other
   * making this operation somewhat costly and therefore requiring a dedicated test case.
   *
   * We implement this creating a ColumnVsColumn scan in between all the columns with same data type
   */
  const auto column_definitions = table_wrapper->get_table()->column_definitions();
  auto column_comparison_pairs = std::vector<ColumnPair>();

  auto unmatched_columns = std::vector<TableColumnDefinition>();

  // std::cout << "\tget column pairs" << std::endl;

  for (const auto& column : column_definitions) {
    // std::cout << "\t\t" << column.name << std::endl;
    bool matched = false;
    auto unmatched_iterator = unmatched_columns.begin();
    while (unmatched_iterator < unmatched_columns.end()) {
      // std::cout << "\t\t\t" << unmatched_iterator->name;
      if (column.data_type == unmatched_iterator->data_type) {
        matched = true;
        column_comparison_pairs.emplace_back(ColumnPair(unmatched_iterator->name, column.name));
        unmatched_columns.erase(unmatched_iterator);
        // std::cout << " (x)";
        break;
      }
      // std::cout << std::endl;
      unmatched_iterator++;
    }
    if (!matched) {
      // std::cout << "\t\t(emplace)" << std::endl;
      unmatched_columns.emplace_back(column);
    }
  }
  return column_comparison_pairs;
}

void CalibrationLQPGenerator::_generate_column_vs_column_scans(
    const std::shared_ptr<const CalibrationTableWrapper>& table_wrapper) {
  /*
   * Here we are generating column vs column scans since they are significantly more costly and scale differently
   * compared to regular scans.
   */
  const auto stored_table_node = StoredTableNode::make(table_wrapper->get_name());
  const auto column_vs_column_scan_pairs = _get_column_pairs(table_wrapper);

  // std::cout << "column vs column " << table_wrapper->get_name() << std::endl;

  for (const ColumnPair& pair : column_vs_column_scan_pairs) {
    std::cout << "column vs column " << pair.first << " vs " << pair.second << std::endl;
    _generated_lqps.emplace_back(PredicateNode::make(
        greater_than_(stored_table_node->get_column(pair.first), stored_table_node->get_column(pair.second)),
        stored_table_node));
  }
}

template <typename ColumnDataType>
std::shared_ptr<PredicateNode> CalibrationLQPGenerator::_get_predicate_node_based_on(
    const std::shared_ptr<LQPColumnExpression>& column, const ColumnDataType& lower_bound,
    const std::shared_ptr<AbstractLQPNode>& base) {
  return PredicateNode::make(greater_than_(column, lower_bound), base);
}

CalibrationLQPGenerator::CalibrationLQPGenerator() {
  _generated_lqps = std::vector<std::shared_ptr<AbstractLQPNode>>();
  _optimizer->add_rule(std::make_unique<ChunkPruningRule>());
}

const std::vector<std::shared_ptr<AbstractLQPNode>>& CalibrationLQPGenerator::lqps() { return _generated_lqps; }
}  // namespace opossum
