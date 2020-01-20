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
        case OperatorType::Aggregate:           break;
        case OperatorType::Alias:               break;
        case OperatorType::Delete:              break;
        case OperatorType::Difference:          break;
        case OperatorType::ExportBinary:        break;
        case OperatorType::ExportCsv:           break;
        case OperatorType::GetTable:            break;
        case OperatorType::ImportBinary:        break;
        case OperatorType::ImportCsv:           break;
        case OperatorType::IndexScan:           break;
        case OperatorType::Insert:              break;
        case OperatorType::JoinHash:            break;
        case OperatorType::JoinIndex:           break;
        case OperatorType::JoinNestedLoop:      break;
        case OperatorType::JoinSortMerge:       break;
        case OperatorType::JoinVerification:    break;
        case OperatorType::Limit:               break;
        case OperatorType::Print:               break;
        case OperatorType::Product:             break;
        case OperatorType::Projection:          break;
        case OperatorType::Sort:                break;
        case OperatorType::TableScan:           return _generate_table_scans(table);
        case OperatorType::TableWrapper:        break;
        case OperatorType::UnionAll:            break;
        case OperatorType::UnionPositions:      break;
        case OperatorType::Update:              break;
        case OperatorType::Validate:            break;
        case OperatorType::CreateTable:         break;
        case OperatorType::CreatePreparedPlan:  break;
        case OperatorType::CreateView:          break;
        case OperatorType::DropTable:           break;
        case OperatorType::DropView:            break;
        case OperatorType::Mock:                break;
      }
      return std::vector<std::shared_ptr<AbstractLQPNode>>(); //TODO How to avoid this? (This line is never reached)
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
        // Column specfic values
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
          });
        }
      }
      return generated_lpqs;
    }
}
