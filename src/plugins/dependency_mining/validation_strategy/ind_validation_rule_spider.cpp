#include "ind_validation_rule_spider.hpp"

#include "hyrise.hpp"
#include "resolve_type.hpp"
#include "statistics/attribute_statistics.hpp"
#include "statistics/table_statistics.hpp"
#include "utils/timer.hpp"

namespace opossum {

INDValidationRuleSpider::INDValidationRuleSpider() : AbstractDependencyValidationRule(DependencyType::Inclusion) {}

std::shared_ptr<ValidationResult> INDValidationRuleSpider::_on_validate(const DependencyCandidate& candidate) const {
  Assert(candidate.determinants.size() == 1, "Invalid determinants for IND");
  Assert(candidate.dependents.size() == 1, "Invalid dependents for IND");

  const auto determinant = candidate.determinants[0];
  const auto dependent = candidate.dependents[0];
  const auto determinant_table = Hyrise::get().storage_manager.get_table(determinant.table_name);
  const auto dependent_table = Hyrise::get().storage_manager.get_table(dependent.table_name);
  auto det_column_type = determinant_table->column_data_type(determinant.column_id);
  auto dep_column_type = dependent_table->column_data_type(dependent.column_id);
  if (dep_column_type != det_column_type) return INVALID_VALIDATION_RESULT;
  bool is_valid = true;

  const auto& determinant_table_statistics = determinant_table->table_statistics();
  const auto& dependent_table_statistics = dependent_table->table_statistics();
  if (determinant_table_statistics && dependent_table_statistics) {
    resolve_data_type(dep_column_type, [&](auto type) {
      using ColumnDataType = typename decltype(type)::type;
      const auto get_column_statistics = [](const auto& table_statistics, const auto column_id) {
        const auto& column_statistics = table_statistics->column_statistics;
        const auto& attribute_statistics =
            static_cast<AttributeStatistics<ColumnDataType>&>(*column_statistics.at(column_id));
        const auto& histogram = attribute_statistics.histogram;
        return ColumnStatistics{histogram->bin_minimum(BinID{0}), histogram->bin_maximum(histogram->bin_count() - 1),
                                histogram->total_count()};
      };
      const auto determinant_statistics = get_column_statistics(determinant_table_statistics, determinant.column_id);
      const auto dependent_statistics = get_column_statistics(dependent_table_statistics, dependent.column_id);

      // We use INDs to eliminate Joins. If dependent table has NULL values, they will not match.
      // Thus, we use NULL != NULL semantics
      if (static_cast<float>(dependent_table->row_count()) - dependent_statistics.not_null_value_count >= 1) {
        is_valid = false;
      }
    });
  } else {
    return UNCERTAIN_VALIDATION_RESULT;
  }

  if (!is_valid) {
    return INVALID_VALIDATION_RESULT;
  }

  bool is_bidirect = true;
  resolve_data_type(dep_column_type, [&](auto type) {
    using ColumnDataType = typename decltype(type)::type;
    using Dictionaries = std::vector<std::shared_ptr<const pmr_vector<ColumnDataType>>>;
    Dictionaries determinant_dictionaries;
    Dictionaries dependent_dictionaries;

    const auto gather_dictionaries = [](const auto& table, const auto column_id, auto& dictionaries) {
      for (auto chunk_id = ChunkID{0}; chunk_id < table->chunk_count(); ++chunk_id) {
        const auto& chunk = table->get_chunk(chunk_id);
        if (!chunk) continue;
        const auto& segment = chunk->get_segment(column_id);
        if (const auto& dictionary_segment = std::dynamic_pointer_cast<DictionarySegment<ColumnDataType>>(segment)) {
          const auto dictionary = dictionary_segment->dictionary();
          if (!dictionary->empty()) {
            dictionaries.emplace_back(dictionary);
          }
        } else {
          Fail("Could not resolve dictionary segment");
        }
      }
    };

    gather_dictionaries(determinant_table, determinant.column_id, determinant_dictionaries);
    gather_dictionaries(dependent_table, dependent.column_id, dependent_dictionaries);
    const auto get_min_max = [](auto& dictionaries) {
      ColumnDataType min_val = dictionaries[0]->front();
      ColumnDataType max_val = dictionaries[0]->back();
      if (dictionaries.size() == 1) return std::make_pair(min_val, max_val);
      for (auto dictionary_id = ChunkID{0}; dictionary_id < dictionaries.size(); ++dictionary_id) {
        min_val = std::min(min_val, dictionaries[dictionary_id]->front());
        max_val = std::max(max_val, dictionaries[dictionary_id]->back());
      }
      return std::make_pair(min_val, max_val);
    };
    const auto [determinant_min, determinant_max] = get_min_max(determinant_dictionaries);
    const auto [dependent_min, dependent_max] = get_min_max(dependent_dictionaries);

    if (dependent_min < determinant_min || dependent_max > determinant_max) {
      is_valid = false;
      return;
    }

    std::vector<ChunkOffset> determinant_positions(determinant_dictionaries.size(), ChunkOffset{0});
    std::vector<ChunkOffset> dependent_positions(dependent_dictionaries.size(), ChunkOffset{0});
    Assert(determinant_dictionaries.size() == determinant_positions.size(), "invalid determinant positions");
    Assert(dependent_dictionaries.size() == dependent_positions.size(), "invalid dependent positions");
    auto determinant_next_value = ColumnDataType{0};
    auto dependent_next_value = ColumnDataType{0};
    auto determinant_finished_dictionaries = ChunkID{0};
    auto dependent_finished_dictionaries = ChunkID{0};
    const auto fetch_next_value = [](auto& positions, const auto& dictionaries, const auto& next_value,
                                     auto& finished_dictionaries) {
      auto current_smallest_value = ColumnDataType{0};
      auto current_smallest_dictionary = ChunkID{0};
      bool init = false;
      for (auto dictionary_id = ChunkID{0}; dictionary_id < positions.size(); ++dictionary_id) {
        const auto& value_pointer = positions[dictionary_id];
        const auto& dictionary = dictionaries.at(dictionary_id);
        if (value_pointer == dictionary->size()) continue;
        if (!init || dictionary->at(value_pointer) < current_smallest_value) {
          init = true;
          current_smallest_value = dictionary->at(value_pointer);
          current_smallest_dictionary = dictionary_id;
          continue;
        }
      }
      for (auto dictionary_id = ChunkID{0}; dictionary_id < positions.size(); ++dictionary_id) {
        const auto& value_pointer = positions[dictionary_id];
        const auto& dictionary = dictionaries.at(dictionary_id);
        if (value_pointer == dictionary->size()) continue;
        if (dictionary->at(value_pointer) == current_smallest_value) {
          if (value_pointer + 1 == dictionary->size()) ++finished_dictionaries;
          ++positions[dictionary_id];
        }
      }
      return current_smallest_value;
    };

    while (dependent_finished_dictionaries < dependent_dictionaries.size()) {
      Assert(determinant_finished_dictionaries < determinant_dictionaries.size(), "Dependent larger than Determinant");
      if (determinant_next_value == dependent_next_value) {
        determinant_next_value = fetch_next_value(determinant_positions, determinant_dictionaries,
                                                  determinant_next_value, determinant_finished_dictionaries);
        dependent_next_value = fetch_next_value(dependent_positions, dependent_dictionaries, dependent_next_value,
                                                dependent_finished_dictionaries);
        continue;
      }
      if (dependent_next_value > determinant_next_value) {
        is_bidirect = false;
        determinant_next_value = fetch_next_value(determinant_positions, determinant_dictionaries,
                                                  determinant_next_value, determinant_finished_dictionaries);
        continue;
      }
      is_valid = false;
      return;
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
