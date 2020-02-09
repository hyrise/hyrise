#include <expression/expression_functional.hpp>
#include "logical_query_plan/predicate_node.hpp"
#include "lqp_generator.hpp"

#include "expression/expression_functional.hpp"
#include "logical_query_plan/predicate_node.hpp"

#include "calibration_table_wrapper.hpp"

using namespace opossum::expression_functional;

namespace opossum {
    std::vector<std::shared_ptr<AbstractLQPNode>>
    LQPGenerator::generate(OperatorType operator_type, std::shared_ptr<const CalibrationTableWrapper> table) const {
      switch (operator_type) {
        case OperatorType::TableScan:
          return _generate_table_scans(table);
        case OperatorType::JoinHash:
        default:
          break;
      }
      throw std::runtime_error("Not implemented yet: Only TableScans and JoinHashes are currentlysupported");
    }

    std::vector<std::shared_ptr<AbstractLQPNode>>
    LQPGenerator::_generate_table_scans(const std::shared_ptr<const CalibrationTableWrapper>& table) const {
      const int selectivity_resolution = 10;
      const int reference_scan_resolution = 10;

      std::vector<std::shared_ptr<AbstractLQPNode>> generated_lpqs;

      // TODO remove the need of referencing columns by string
      const auto stored_table_node = StoredTableNode::make(table->get_name());

      const int column_count = table->get_table()->column_count();
      const std::vector<std::string> column_names = table->get_table()->column_names();
      const auto column_data_types = table->get_table()->column_data_types();

      for (ColumnID column_id = ColumnID{0}; column_id < column_count; ++column_id) {

        // Column specific values
        const auto column = stored_table_node->get_column(column_names.at(column_id));
        const auto distribution = table->get_column_data_distribution(column_id);
        const auto step_size = (distribution.max_value - distribution.min_value) / selectivity_resolution;

        for (int selectivity_step = 0; selectivity_step < selectivity_resolution; selectivity_step++) {
          // in this for-loop we iterate up and go from 100% selectivity to 0% by increasing the lower_bound in steps
          // for any step there
          resolve_data_type(column_data_types[column_id], [&](const auto column_data_type) {
              using ColumnDataType = typename decltype(column_data_type)::type;

              // Get value for current iteration
              // the cursor is a int representation of where the column has to be cut in this iteration
              const auto step_cursor = static_cast<int>(selectivity_step * step_size);

              const ColumnDataType lower_bound = SyntheticTableGenerator::generate_value<ColumnDataType>(step_cursor);

              // Generate main predicate

              auto get_predicate_node_based_on = [column, lower_bound](const std::shared_ptr<AbstractLQPNode>& base)
                      { return PredicateNode::make(greater_than_(column, lower_bound), base); };
              generated_lpqs.emplace_back(get_predicate_node_based_on(std::shared_ptr<AbstractLQPNode>(stored_table_node)));

              // Add reference scans

              // generate reference scans that are executed before the actual predicate
              // that reduce the overall selectivity stepwise
              const double reference_scan_step_size = (distribution.max_value - step_cursor) / reference_scan_resolution;
              for (int step = 0; step < reference_scan_resolution; step++) {
                const ColumnDataType upper_bound = SyntheticTableGenerator::generate_value<ColumnDataType>(
                        static_cast<int>(step * reference_scan_step_size));
                generated_lpqs.emplace_back(
                        get_predicate_node_based_on(PredicateNode::make(less_than_(column, upper_bound), stored_table_node))
                        );
              }
              // add reference scan with full pos list
              generated_lpqs.emplace_back(get_predicate_node_based_on(PredicateNode::make(is_not_null_(column), stored_table_node)));
              // add reference scan with empty pos list
              generated_lpqs.emplace_back(get_predicate_node_based_on(PredicateNode::make(is_null_(column), stored_table_node)));


              // Like Predicate for Strings
              if (std::is_same<ColumnDataType, std::string>::value) {
                for (int step = 0; step < 10; step++) {
                  auto const upper_bound = (SyntheticTableGenerator::generate_value<pmr_string>(step));
                  generated_lpqs.emplace_back(PredicateNode::make(like_(column, upper_bound + "%"), stored_table_node));
                }

                // 100% selectivity
                generated_lpqs.emplace_back(PredicateNode::make(like_(column, "%"), stored_table_node));
                // 0% selectivity
                generated_lpqs.emplace_back(PredicateNode::make(like_(column, "%not_there%"), stored_table_node));
              }

          });
        }
      }
      _generate_column_vs_column_scans(table, generated_lpqs);
      return generated_lpqs;
    }

    void LQPGenerator::_generate_column_vs_column_scans(
            const std::shared_ptr<const CalibrationTableWrapper> &table, std::vector<std::shared_ptr<AbstractLQPNode>>& lqp_queue) const {
      /*
       * ColumnVsColumn Scans occur when the value of a predicate is a column.
       * In this case every value from one column has to be compared to every value of the other
       * making this operation somewhat costly and therefore requiring a dedicated test case.
       *
       * We implement this creating a columnVsColumn scan in between all the columns with same data type
       */
      const auto column_definitions = table->get_table()->column_definitions();
      using ColumnPair = std::pair<const std::string, const std::string>;
      // TODO shuffle order (otherwise pairing will be solely determined by the order in data generation)
      auto column_vs_column_scan_pairs = std::vector<ColumnPair>();

      // TODO check if reference wrapper makes sense here
      auto singles = std::vector<TableColumnDefinition>();

      // TODO Try to avoid copying in loop
      for (const TableColumnDefinition& column : column_definitions) {
        bool matched = false;
        auto single_iterator = singles.begin();
	      while (single_iterator < singles.end()) {
	        if (column.data_type == single_iterator->data_type) {
            matched = true;
            column_vs_column_scan_pairs.emplace_back(ColumnPair(single_iterator->name, column.name));
            singles.erase(single_iterator);
            break;
          }
	        single_iterator++;
        }
        if (!matched) {
          singles.emplace_back(column);
        }
      }

      const auto stored_table_node = StoredTableNode::make(table->get_name());

      for (const ColumnPair &pair : column_vs_column_scan_pairs) {
        lqp_queue.emplace_back(PredicateNode::make(
                greater_than_(
                        stored_table_node->get_column(pair.first),
                        stored_table_node->get_column(pair.second)
                ), stored_table_node));
      }

    }
}
