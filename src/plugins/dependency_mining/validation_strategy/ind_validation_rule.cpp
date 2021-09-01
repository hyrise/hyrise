#include "ind_validation_rule.hpp"

#include <numeric>
#include <random>

#include "hyrise.hpp"
#include "sql/sql_pipeline_builder.hpp"

namespace opossum {

INDValidationRule::INDValidationRule(
    tbb::concurrent_unordered_map<std::string, std::shared_ptr<std::mutex>>& table_constraint_mutexes)
    : AbstractDependencyValidationRule(DependencyType::Inclusion, table_constraint_mutexes) {}

ValidationResult INDValidationRule::_on_validate(const DependencyCandidate& candidate) const {
  Assert(candidate.determinants.size() == 1, "Invalid determinats for IND");
  Assert(candidate.dependents.size() == 1, "Invalid dependents for IND");

  const auto determinant = candidate.determinants[0];
  const auto dependent = candidate.dependents[0];
  auto det_column_type =
      Hyrise::get().storage_manager.get_table(determinant.table_name)->column_data_type(determinant.column_id);
  if (det_column_type == DataType::Double) {
    det_column_type = DataType::Float;
  } else if (det_column_type == DataType::Long) {
    det_column_type = DataType::Int;
  }
  auto dep_column_type =
      Hyrise::get().storage_manager.get_table(dependent.table_name)->column_data_type(dependent.column_id);
  if (dep_column_type == DataType::Double) {
    dep_column_type = DataType::Float;
  } else if (dep_column_type == DataType::Long) {
    dep_column_type = DataType::Int;
  }

  if (dep_column_type != det_column_type) return INVALID_VALIDATION_RESULT;

  const auto [det_status, det_result] =
      SQLPipelineBuilder{"SELECT DISTINCT " + determinant.column_name() + " placeholder_name FROM " +
                         determinant.table_name + " ORDER BY " + determinant.column_name()}
          .create_pipeline()
          .get_result_table();
  if (det_status != SQLPipelineStatus::Success) return UNCERTAIN_VALIDATION_RESULT;

  const auto [dep_status, dep_result] =
      SQLPipelineBuilder{"SELECT DISTINCT " + dependent.column_name() + " placeholder_name FROM " +
                         dependent.table_name + " ORDER BY " + dependent.column_name()}
          .create_pipeline()
          .get_result_table();
  if (dep_status != SQLPipelineStatus::Success) return UNCERTAIN_VALIDATION_RESULT;

  if (dep_result->row_count() > det_result->row_count()) return UNCERTAIN_VALIDATION_RESULT;

  const auto dep_rows = dep_result->get_rows();
  const auto det_rows = det_result->get_rows();

  auto dep_iter = dep_rows.begin();
  auto det_iter = det_rows.begin();

  while (dep_iter != dep_rows.end()) {
    if (*dep_iter != *dep_iter) {
      return INVALID_VALIDATION_RESULT;
    }
    ++dep_iter;
    ++det_iter;
  }

  // to do: return constraints based on bi/one-directional case
  /*if (dep_rows.size() == det_rows.size()) {
    out << "    VALID (bidirectional)" << std::endl;
    return true;
  }*/
  return {DependencyValidationStatus::Valid, {}};
}

}  // namespace opossum
