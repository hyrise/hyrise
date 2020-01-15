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
      // TODO remove the need of referencing columns by string
      const int selectivity_resolution = 10;
      std::vector<std::shared_ptr<AbstractLQPNode>> generated_lpqs;

      const auto stored_table_node = StoredTableNode::make(table->get_name());

      const int column_count = table->get_table()->column_count();
      const std::vector<std::string> column_names = table->get_table()->column_names();
      const auto column_data_types = table->get_table()->column_data_types();

      for (ColumnID column_id = ColumnID{0}; column_id < column_count; ++column_id) {
        //Column specfic values
        const auto column_reference = stored_table_node->get_column(column_names.at(column_id));
        const auto distribution = table->get_column_data_distribution(column_id);
        const auto step_size = (distribution.max_value - distribution.min_value) / selectivity_resolution;

        for (int selectivity_step = 0; selectivity_step < selectivity_resolution; selectivity_step++) { //TODO Check this for loop

          resolve_data_type(column_data_types[column_id], [&](const auto column_data_type) {
              using ColumnDataType = typename decltype(column_data_type)::type;

              // Get value
              const auto step_point = selectivity_step * step_size;
              ColumnDataType value = SyntheticTableGenerator::generate_value<ColumnDataType>(step_point);

              // COLUMN_SCAN
              const auto _predicate_node = PredicateNode::make(greater_than_(column_reference, value), stored_table_node);



              const double step_size = (upper_bound - lower_bound_predicate) / reference_scan_resolution;

              const double step_size = (upper_bound - lower_bound_predicate) / reference_scan_resolution;

              for (int step = 0; step < reference_scan_resolution; step++) {
                list.emplace_back(get_predicate(PredicateNode::make(less_than_(column, step*step_size), table)));
              }

              // own predicate
              generated_lpqs.emplace_back(get_predicate(std::shared_ptr<AbstractLQPNode>(table)));
              // full pos list in reference segment
              generated_lpqs.emplace_back(get_predicate(PredicateNode::make(is_not_null_(column), table)));
              // empty pos list in reference segment
              generated_lpqs.emplace_back(get_predicate(PredicateNode::make(is_null_(column), table)));
          });
        }
      }
      return generated_lpqs;
    }
}
