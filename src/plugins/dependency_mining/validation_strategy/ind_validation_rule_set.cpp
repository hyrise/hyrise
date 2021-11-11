#include "ind_validation_rule_set.hpp"

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

INDValidationRuleSet::INDValidationRuleSet() : AbstractDependencyValidationRule(DependencyType::Inclusion) {}

std::shared_ptr<ValidationResult> INDValidationRuleSet::_on_validate(const DependencyCandidate& candidate) const {
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

  bool is_bidirect = false;

  resolve_data_type(dep_column_type, [&](auto type) {
    using ColumnDataType = typename decltype(type)::type;
    const auto collect_dictionaries = [](const auto& table, const auto column_id) {
      Assert(table && table->type() == TableType::Data, "Expected Data table");
      std::vector<std::shared_ptr<const pmr_vector<ColumnDataType>>> dictionaries;
      dictionaries.reserve(table->chunk_count());
      for (auto chunk_id = ChunkID{0}; chunk_id < table->chunk_count(); ++chunk_id) {
        const auto& chunk = table->get_chunk(chunk_id);
        if (!chunk) continue;
        const auto& segment = chunk->get_segment(column_id);
        if (const auto& dictionary_segment = std::dynamic_pointer_cast<DictionarySegment<ColumnDataType>>(segment)) {
          dictionaries.emplace_back(dictionary_segment->dictionary());
        } else {
          Fail("Could not resolve dictionary segment");
        }
      }
      return dictionaries;
    };

    const auto& determinant_dictionaries = collect_dictionaries(determinant_table, determinant.column_id);
    const auto& dependent_dictionaries = collect_dictionaries(dependent_table, dependent.column_id);

    const auto add_values = [](auto& set, auto& dictionaries) {
      size_t max_dict_size{0};
      for (const auto& dictionary : dictionaries) {
        max_dict_size = std::max(max_dict_size, dictionary->size());
      }

      set.reserve(max_dict_size);
      for (const auto& dictionary : dictionaries) {
        set.insert(dictionary->cbegin(), dictionary->cend());
      }
    };

    std::unordered_set<ColumnDataType> determinant_values;
    std::unordered_set<ColumnDataType> dependent_values;
    add_values(determinant_values, determinant_dictionaries);
    add_values(dependent_values, dependent_dictionaries);

    if (determinant_values.size() < dependent_values.size()) {
      is_valid = false;
      return;
    }
    for (const auto& element : dependent_values) {
      if (!determinant_values.contains(element)) {
        is_valid = false;
        return;
      }
    }
    if (determinant_values.size() == dependent_values.size()) {
      is_bidirect = true;
    }
  });
  if (!is_valid) return INVALID_VALIDATION_RESULT;

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
