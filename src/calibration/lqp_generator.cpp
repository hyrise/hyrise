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
        const auto distribution = table->get_column_data_distribution(column_id);

        switch (column_data_types.at(column_id)) {
          case DataType::Null:
            break;
          case DataType::String:
            // TODO check how to do different selectivity levels
          {
            auto column = stored_table_node->get_column(column_names.at(column_id));
            generated_lpqs.emplace_back(PredicateNode::make(greater_than_(column, "yo"), stored_table_node));
            break;
          }
          case DataType::Int:
          case DataType::Long:
          case DataType::Double:
          case DataType::Float:
            if (distribution.distribution_type == DataDistributionType::Uniform) {
              // change selectivity in steps
              double step_size = (distribution.max_value - distribution.min_value) / selectivity_resolution;
              for (int selectivity_step = 0; selectivity_step < selectivity_resolution + 1; selectivity_step++) {
                double lower_bound = distribution.min_value + step_size * selectivity_step;
                _add_lqps_with_reference_scans(generated_lpqs, lower_bound, distribution.max_value, stored_table_node, column_names.at(column_id));
              }
            } else {
              // for non uniform distributions: 100% selectivity
              _add_lqps_with_reference_scans(generated_lpqs, distribution.min_value, distribution.max_value, stored_table_node, column_names.at(column_id));
            }
            break;
        }
      }
      return generated_lpqs;
    }

    void LQPGenerator::_add_lqps_with_reference_scans(std::vector<std::shared_ptr<AbstractLQPNode>> &list,
                                                      const double lower_bound_predicate, const double upper_bound,
                                                      std::shared_ptr<StoredTableNode> table,
                                                      const std::string &column_name) const {

      const int reference_scan_resolution = 10;

      auto column = table->get_column(column_name);
      auto get_predicate = [column,lower_bound_predicate](const std::shared_ptr<AbstractLQPNode>& base)
              { return PredicateNode::make(greater_than_(column, lower_bound_predicate), base); };
      const double step_size = (upper_bound - lower_bound_predicate) / reference_scan_resolution;

      for (int step = 0; step < reference_scan_resolution; step++) {
        list.emplace_back(get_predicate(PredicateNode::make(less_than_(column, step*step_size), table)));
      }

      // own predicate
      list.emplace_back(get_predicate(std::shared_ptr<AbstractLQPNode>(table)));
      // full pos list in reference segment
      list.emplace_back(get_predicate(PredicateNode::make(is_not_null_(column), table)));
      // empty pos list in reference segment
      list.emplace_back(get_predicate(PredicateNode::make(is_null_(column), table)));
    }

}