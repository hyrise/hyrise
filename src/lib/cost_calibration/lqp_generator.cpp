
#include <expression/expression_functional.hpp>
#include <utility>
#include "logical_query_plan/stored_table_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "lqp_generator.hpp"
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
      // TODO implement Scan on Reference Segments
      int selectivity_resolution = 10;
      std::vector<std::shared_ptr<AbstractLQPNode>> generated_lpqs;

      const auto _stored_table_node = StoredTableNode::make(table->get_name());

      int column_count = table->get_table()->column_count();
      std::vector<std::string> column_names = table->get_table()->column_names();
      auto column_data_types = table->get_table()->column_data_types();

      for (ColumnID column_id = ColumnID{0}; column_id < column_count; ++column_id) {
        auto distribution = table->get_column_data_distribution(column_id);
        auto column = _stored_table_node->get_column(column_names.at(column_id));

        switch (column_data_types.at(column_id)) {
          case DataType::Null:
            break;
          case DataType::String:
            generated_lpqs.emplace_back(PredicateNode::make(greater_than_(column, "yo"), _stored_table_node));
            break;
          case DataType::Int:
          case DataType::Long:
          case DataType::Double:
          case DataType::Float:
            if (distribution.distribution_type == DataDistributionType::Uniform) {
              // change selectivity in steps
              double step_size = (distribution.max_value - distribution.min_value) / selectivity_resolution;
              for (int selectivity_step = 0; selectivity_step < selectivity_resolution + 1; selectivity_step++) {
                double lower_bound = distribution.min_value + step_size * selectivity_step;
                generated_lpqs.emplace_back(PredicateNode::make(greater_than_(column, lower_bound), _stored_table_node));
              }
            } else {
              // for non uniform distributions: 100% selectivity
              generated_lpqs.emplace_back(PredicateNode::make(greater_than_(column, distribution.min_value), _stored_table_node));
            }
            break;
        }
      }
      return generated_lpqs;
    }
}