
#include <expression/expression_functional.hpp>
#include "logical_query_plan/stored_table_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "lqp_generator.hpp"
#include "calibration_table_wrapper.hpp"

using namespace opossum::expression_functional;

namespace opossum {
    std::vector<std::shared_ptr<AbstractLQPNode>>
    LQPGenerator::generate(OperatorType operator_type, std::shared_ptr<const CalibrationTableWrapper> table) const {
      switch (operator_type) {
        case OperatorType::Aggregate:
          break;
        case OperatorType::Alias:
          break;
        case OperatorType::Delete:
          break;
        case OperatorType::Difference:
          break;
        case OperatorType::ExportBinary:
          break;
        case OperatorType::ExportCsv:
          break;
        case OperatorType::GetTable:
          break;
        case OperatorType::ImportBinary:
          break;
        case OperatorType::ImportCsv:
          break;
        case OperatorType::IndexScan:
          break;
        case OperatorType::Insert:
          break;
        case OperatorType::JoinHash:
          break;
        case OperatorType::JoinIndex:
          break;
        case OperatorType::JoinNestedLoop:
          break;
        case OperatorType::JoinSortMerge:
          break;
        case OperatorType::JoinVerification:
          break;
        case OperatorType::Limit:
          break;
        case OperatorType::Print:
          break;
        case OperatorType::Product:
          break;
        case OperatorType::Projection:
          break;
        case OperatorType::Sort:
          break;
        case OperatorType::TableScan:
          return _generate_table_scans(table);
        case OperatorType::TableWrapper:
          break;
        case OperatorType::UnionAll:
          break;
        case OperatorType::UnionPositions:
          break;
        case OperatorType::Update:
          break;
        case OperatorType::Validate:
          break;
        case OperatorType::CreateTable:
          break;
        case OperatorType::CreatePreparedPlan:
          break;
        case OperatorType::CreateView:
          break;
        case OperatorType::DropTable:
          break;
        case OperatorType::DropView:
          break;
        case OperatorType::Mock:
          break;
      }
      return std::vector<std::shared_ptr<AbstractLQPNode>>(); //TODO How to avoid this? (This line is never reached)
    }

    std::vector<std::shared_ptr<AbstractLQPNode>>
    LQPGenerator::_generate_table_scans(std::shared_ptr<const CalibrationTableWrapper> table) const {
      int selectivity_steps = 10;
      std::vector<std::shared_ptr<AbstractLQPNode>> generated_lpqs;

      const auto _t_a = StoredTableNode::make(table->get_name());

      int column_count = table->get_table()->column_count();
      std::vector<std::string> column_names = table->get_table()->column_names();
      auto column_data_types = table->get_table()->column_data_types();

      for (int j = 0; j < column_count; ++j) {

        auto distribution = table->get_column_data_distribution(ColumnID(static_cast<const uint16_t>(j)));

        auto column = _t_a->get_column(column_names.at(j));

        switch (column_data_types.at(j)) {
          case DataType::Null:
            break;
          case DataType::String:
            generated_lpqs.emplace_back(PredicateNode::make(greater_than_(column, "yo"), _t_a));
            break;
          case DataType::Int:
          case DataType::Long:
          case DataType::Double:
          case DataType::Float:
            if (distribution.distribution_type == DataDistributionType::Uniform) {
              // change selecticitc programatically
//                auto step = distribution.min_value + (distribution.max_value - distribution.min_value);
              for (int i = 0; i < selectivity_steps; i++) {
                generated_lpqs.emplace_back(PredicateNode::make(greater_than_(column, 10), _t_a));
              }
            } else {
              generated_lpqs.emplace_back(PredicateNode::make(greater_than_(column, 10), _t_a));
            }
            break;
        }
      }
      return generated_lpqs;
    }
}