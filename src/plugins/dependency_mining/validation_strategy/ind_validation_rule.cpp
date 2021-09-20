#include "ind_validation_rule.hpp"

#include <numeric>
#include <random>

#include "hyrise.hpp"
#include "operators/sort.hpp"
#include "operators/get_table.hpp"
#include "operators/aggregate_hash.hpp"
#include "operators/pqp_utils.hpp"

namespace opossum {

INDValidationRule::INDValidationRule() : AbstractDependencyValidationRule(DependencyType::Inclusion) {}

std::shared_ptr<ValidationResult> INDValidationRule::_on_validate(const DependencyCandidate& candidate) const {
  Assert(candidate.determinants.size() == 1, "Invalid determinats for IND");
  Assert(candidate.dependents.size() == 1, "Invalid dependents for IND");

  const auto determinant = candidate.determinants[0];
  const auto dependent = candidate.dependents[0];
  const auto determinant_table = Hyrise::get().storage_manager.get_table(determinant.table_name);
  const auto dependent_table = Hyrise::get().storage_manager.get_table(dependent.table_name);
  auto det_column_type = determinant_table->column_data_type(determinant.column_id);
  if (det_column_type == DataType::Double) {
    det_column_type = DataType::Float;
  } else if (det_column_type == DataType::Long) {
    det_column_type = DataType::Int;
  }
  auto dep_column_type = dependent_table->column_data_type(dependent.column_id);
  if (dep_column_type == DataType::Double) {
    dep_column_type = DataType::Float;
  } else if (dep_column_type == DataType::Long) {
    dep_column_type = DataType::Int;
  }

  if (dep_column_type != det_column_type) return INVALID_VALIDATION_RESULT;

  std::vector<ColumnID> determinant_pruned_columns;
  std::vector<ColumnID> dependent_pruned_columns;

  for (auto column_id = ColumnID{0}; column_id < determinant_table->column_count(); ++column_id) {
    if (column_id != determinant.column_id) {
      determinant_pruned_columns.emplace_back(column_id);
    }
  }
  for (auto column_id = ColumnID{0}; column_id < dependent_table->column_count(); ++column_id) {
    if (column_id != dependent.column_id) {
      dependent_pruned_columns.emplace_back(column_id);
    }
  }

  const auto execute_pqp = [](const std::shared_ptr<AbstractOperator>& root_node) {
    std::vector<std::shared_ptr<AbstractOperator>> operators;
    visit_pqp(root_node, [&operators](const auto& node){
      operators.emplace_back(node);
      return PQPVisitation::VisitInputs;
    });
    for(auto it = operators.rbegin(); it != operators.rend(); ++it) {
      const auto& op = *it;
      op->execute();
    }
  };


  const auto determinant_get_table = std::make_shared<GetTable>(determinant.table_name, std::vector<ChunkID>{}, determinant_pruned_columns);
  const auto dependent_get_table = std::make_shared<GetTable>(determinant.table_name, std::vector<ChunkID>{}, dependent_pruned_columns);
  const auto group_by_column = std::vector<ColumnID>{ColumnID{0}};
  const auto determinant_aggregate = std::make_shared<AggregateHash>(determinant_get_table, std::vector<std::shared_ptr<AggregateExpression>>{}, group_by_column);
  const auto dependent_aggregate = std::make_shared<AggregateHash>(dependent_get_table, std::vector<std::shared_ptr<AggregateExpression>>{}, group_by_column);
  const auto sort_definition = std::vector<SortColumnDefinition>{SortColumnDefinition{ColumnID{0}}};
  const auto determinant_sort = std::make_shared<Sort>(determinant_aggregate, sort_definition);
  const auto dependent_sort = std::make_shared<Sort>(dependent_aggregate, sort_definition);

  execute_pqp(determinant_sort);
  execute_pqp(dependent_sort);
  if (determinant_sort->get_output()->row_count() < dependent_sort->get_output()->row_count()) return INVALID_VALIDATION_RESULT;

  auto determinant_rows = determinant_sort->get_output()->get_rows();
  const auto dependent_rows = dependent_sort->get_output()->get_rows();
  determinant_sort->clear_output();
  dependent_sort->clear_output();

  std::unordered_set<AllTypeVariant> determinant_values;
  std::unordered_set<AllTypeVariant> dependent_values;

  for (const auto& row : determinant_rows) {
    determinant_values.emplace(row.at(0));
  }
  determinant_rows.clear();
  for (const auto& row : dependent_rows) {
    if (!determinant_values.count(row.at(0))) return INVALID_VALIDATION_RESULT;
  }

  const auto result = std::make_shared<ValidationResult>(DependencyValidationStatus::Valid);
  const auto table_inclusion_constraint = std::make_shared<TableInclusionConstraint>(candidate.determinants, std::vector<ColumnID>{dependent.column_id});
  result->constraints[dependent.table_name].emplace_back(table_inclusion_constraint);

  if (determinant_values.size() == dependent_rows.size()) {
    const auto inclusion_constraint = std::make_shared<TableInclusionConstraint>(candidate.dependents, std::vector<ColumnID>{determinant.column_id});
    result->constraints[determinant.table_name].emplace_back(inclusion_constraint);
  }

  return result;
}

}  // namespace opossum
