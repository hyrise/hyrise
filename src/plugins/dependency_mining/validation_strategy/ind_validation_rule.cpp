#include "ind_validation_rule.hpp"

#include "hyrise.hpp"
#include "operators/aggregate_hash.hpp"
#include "operators/get_table.hpp"
#include "operators/pqp_utils.hpp"
#include "operators/sort.hpp"
#include "resolve_type.hpp"
#include "statistics/attribute_statistics.hpp"
#include "statistics/table_statistics.hpp"
#include "utils/timer.hpp"

namespace opossum {

INDValidationRule::INDValidationRule() : AbstractDependencyValidationRule(DependencyType::Inclusion) {}

std::shared_ptr<ValidationResult> INDValidationRule::_on_validate(const DependencyCandidate& candidate) const {
  Assert(candidate.determinants.size() == 1, "Invalid determinants for IND");
  Assert(candidate.dependents.size() == 1, "Invalid dependents for IND");

  Timer timer;
  const auto determinant = candidate.determinants[0];
  const auto dependent = candidate.dependents[0];
  const auto determinant_table = Hyrise::get().storage_manager.get_table(determinant.table_name);
  const auto dependent_table = Hyrise::get().storage_manager.get_table(dependent.table_name);
  auto det_column_type = determinant_table->column_data_type(determinant.column_id);
  /*if (det_column_type == DataType::Double) {
    det_column_type = DataType::Float;
  } else if (det_column_type == DataType::Long) {
    det_column_type = DataType::Int;
  }*/
  auto dep_column_type = dependent_table->column_data_type(dependent.column_id);
  /*if (dep_column_type == DataType::Double) {
    dep_column_type = DataType::Float;
  } else if (dep_column_type == DataType::Long) {
    dep_column_type = DataType::Int;
  }*/

  if (dep_column_type != det_column_type) return INVALID_VALIDATION_RESULT;
  bool is_valid = true;

  const auto& determinant_statistics = determinant_table->table_statistics();
  const auto& dependent_statistics = dependent_table->table_statistics();
  if (determinant_statistics && dependent_statistics) {
    resolve_data_type(dep_column_type, [&](auto type) {
      using ColumnDataType = typename decltype(type)::type;
      const auto min_max_value = [](const auto& table_statistics, const auto column_id) {
        const auto& column_statistics = table_statistics->column_statistics;
        const auto& attribute_statistics =
            static_cast<AttributeStatistics<ColumnDataType>&>(*column_statistics.at(column_id));
        const auto& histogram = attribute_statistics.histogram;
        return std::make_pair(histogram->bin_minimum(BinID{0}), histogram->bin_maximum(histogram->bin_count() - 1));
      };
      const auto [determinant_min, determinant_max] = min_max_value(determinant_statistics, determinant.column_id);
      const auto [dependent_min, dependent_max] = min_max_value(dependent_statistics, dependent.column_id);
      if (dependent_min < determinant_min || dependent_max > determinant_max) {
        is_valid = false;
      }
    });
  } else {
    resolve_data_type(dep_column_type, [&](auto type) {
      using ColumnDataType = typename decltype(type)::type;
      const auto min_max_value = [](const auto& table, const auto column_id) {
        Assert(table && table->type() == TableType::Data, "Expected Data table");
        auto min_val = ColumnDataType{0};
        auto max_val = ColumnDataType{0};
        bool init = false;
        for (auto chunk_id = ChunkID{0}; chunk_id < table->chunk_count(); ++chunk_id) {
          const auto& chunk = table->get_chunk(chunk_id);
          if (!chunk) continue;
          const auto& segment = chunk->get_segment(column_id);
          if (const auto& dictionary_segment = std::dynamic_pointer_cast<DictionarySegment<ColumnDataType>>(segment)) {
            const auto& dictionary = dictionary_segment->dictionary();
            if (dictionary->empty()) continue;
            if (!init) {
              min_val = dictionary->front();
              max_val = dictionary->back();
              init = true;
            } else {
              min_val = std::min(min_val, dictionary->front());
              max_val = std::max(max_val, dictionary->back());
            }
          } else {
            Fail("Could not resolve dictionary segment");
          }
        }
        return std::make_pair(min_val, max_val);
      };

      const auto [determinant_min, determinant_max] = min_max_value(determinant_table, determinant.column_id);
      const auto [dependent_min, dependent_max] = min_max_value(dependent_table, dependent.column_id);
      if (dependent_min < determinant_min || dependent_max > determinant_max) {
        is_valid = false;
      }
    });
  }

  if (!is_valid) return INVALID_VALIDATION_RESULT;

  const auto prune_columns = [](const auto& table, const auto needed_column_id) {
    std::vector<ColumnID> pruned_columns;
    for (auto column_id = ColumnID{0}; column_id < table->column_count(); ++column_id) {
      if (column_id != needed_column_id) pruned_columns.emplace_back(column_id);
    }
    return pruned_columns;
  };
  const auto determinant_pruned_columns = prune_columns(determinant_table, determinant.column_id);
  const auto dependent_pruned_columns = prune_columns(dependent_table, dependent.column_id);
  const auto determinant_get_table =
      std::make_shared<GetTable>(determinant.table_name, std::vector<ChunkID>{}, determinant_pruned_columns);
  const auto dependent_get_table =
      std::make_shared<GetTable>(dependent.table_name, std::vector<ChunkID>{}, dependent_pruned_columns);
  const auto group_by_column = std::vector<ColumnID>{ColumnID{0}};
  const auto determinant_aggregate = std::make_shared<AggregateHash>(
      determinant_get_table, std::vector<std::shared_ptr<AggregateExpression>>{}, group_by_column);
  const auto dependent_aggregate = std::make_shared<AggregateHash>(
      dependent_get_table, std::vector<std::shared_ptr<AggregateExpression>>{}, group_by_column);

  determinant_get_table->execute();
  dependent_get_table->execute();
  determinant_aggregate->execute();
  dependent_aggregate->execute();
  if (determinant_aggregate->get_output()->row_count() < dependent_aggregate->get_output()->row_count())
    return INVALID_VALIDATION_RESULT;

  const auto sort_definition = std::vector<SortColumnDefinition>{SortColumnDefinition{ColumnID{0}}};
  const auto determinant_sort = std::make_shared<Sort>(determinant_aggregate, sort_definition);
  const auto dependent_sort = std::make_shared<Sort>(dependent_aggregate, sort_definition);

  determinant_sort->execute();
  dependent_sort->execute();
  const auto determinant_rows = determinant_sort->get_output()->get_rows();
  const auto dependent_rows = dependent_sort->get_output()->get_rows();
  determinant_sort->clear_output();
  dependent_sort->clear_output();

  auto dependent_iter = dependent_rows.cbegin();
  auto determinant_iter = determinant_rows.cbegin();
  bool is_bidirect = true;
  while (dependent_iter != dependent_rows.cend() && determinant_iter != determinant_rows.cend()) {
    if ((*dependent_iter)[0] == (*determinant_iter)[0]) {
      ++determinant_iter;
      ++dependent_iter;
      continue;
    }
    if ((*dependent_iter)[0] > (*determinant_iter)[0]) {
      ++determinant_iter;
      is_bidirect = false;
      continue;
    }
    // std::cout << "        i(i) " << timer.lap_formatted() << std::endl;
    return INVALID_VALIDATION_RESULT;
  }
  // std::cout << "        i(ii) " << timer.lap_formatted() << std::endl;
  if (dependent_iter != dependent_rows.cend()) return INVALID_VALIDATION_RESULT;

  const auto result = std::make_shared<ValidationResult>(DependencyValidationStatus::Valid);
  const auto table_inclusion_constraint =
      std::make_shared<TableInclusionConstraint>(candidate.determinants, std::vector<ColumnID>{dependent.column_id});
  result->constraints[dependent.table_name].emplace_back(table_inclusion_constraint);
  if (is_bidirect) {
    const auto inclusion_constraint =
        std::make_shared<TableInclusionConstraint>(candidate.dependents, std::vector<ColumnID>{determinant.column_id});
    result->constraints[determinant.table_name].emplace_back(inclusion_constraint);
  }

  return result;
}

}  // namespace opossum
