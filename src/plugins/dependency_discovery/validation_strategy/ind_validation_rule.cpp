#include "ind_validation_rule.hpp"

#include "dependency_discovery/validation_strategy/validation_utils.hpp"
#include "hyrise.hpp"
#include "resolve_type.hpp"
#include "storage/segment_iterate.hpp"
#include "storage/table.hpp"
#include "utils/print_utils.hpp"

namespace {

using namespace hyrise;  // NOLINT(build/namespaces)

template <typename T>
std::unordered_set<T> collect_values(const std::shared_ptr<const Table>& table, const ColumnID column_id) {
  auto distinct_values = std::unordered_set<T>(table->row_count());
  const auto chunk_count = table->chunk_count();

  for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
    const auto& chunk = table->get_chunk(chunk_id);
    if (!chunk) {
      continue;
    }

    const auto& segment = chunk->get_segment(column_id);
    if (const auto& value_segment = std::dynamic_pointer_cast<ValueSegment<T>>(segment)) {
      // Directly insert all values.
      const auto& values = value_segment->values();
      distinct_values.insert(values.cbegin(), values.cend());
    } else if (const auto& dictionary_segment = std::dynamic_pointer_cast<DictionarySegment<T>>(segment)) {
      // Directly insert dictionary entries.
      const auto& dictionary = dictionary_segment->dictionary();
      distinct_values.insert(dictionary->cbegin(), dictionary->cend());
    } else {
      // Fallback: Iterate the whole segment and decode its values.
      segment_iterate<T>(*segment, [&](const auto& position) {
        if (!position.is_null()) {
          distinct_values.insert(position.value());
        }
      });
    }
  }

  return distinct_values;
}

template <typename T>
ValidationStatus perform_set_based_inclusion_check(
    const std::shared_ptr<Table>& including_table, const ColumnID including_column_id,
    const std::shared_ptr<Table>& included_table, const ColumnID included_column_id,
    std::unordered_map<std::shared_ptr<Table>, std::shared_ptr<AbstractTableConstraint>>& constraints,
    const std::optional<std::pair<T, T>>& including_min_max, const std::optional<std::pair<T, T>>& included_min_max) {
  const auto including_values = collect_values<T>(including_table, including_column_id);

  if constexpr (std::is_integral_v<T>) {
    Assert(!including_values.empty(), "Empty tables not considered.");
    if (including_min_max) {
      const auto domain = static_cast<size_t>(including_min_max->second - including_min_max->first);
      if (domain == including_table->row_count() - 1) {
        constraints[including_table] =
            std::make_shared<TableKeyConstraint>(std::set<ColumnID>{including_column_id}, KeyConstraintType::UNIQUE);
      }

      // Skip probing if primary key continuous.
      Assert(included_min_max, "Could not obtain min/max values.");
      if (domain == including_values.size() - 1 && including_min_max->first <= included_min_max->first &&
          including_min_max->second >= included_min_max->second) {
        return ValidationStatus::Valid;
      }
    } else {
      PerformanceWarning("Could not obtain min/max values.");
    }
  }

  auto status = ValidationStatus::Valid;
  const auto chunk_count = included_table->chunk_count();
  for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
    if (status == ValidationStatus::Invalid) {
      break;
    }
    const auto& chunk = included_table->get_chunk(chunk_id);
    if (!chunk) {
      continue;
    }

    const auto& segment = chunk->get_segment(included_column_id);
    if (const auto& dictionary_segment = std::dynamic_pointer_cast<DictionarySegment<T>>(segment)) {
      for (const auto& value : *dictionary_segment->dictionary()) {
        if (!including_values.contains(value)) {
          status = ValidationStatus::Invalid;
          break;
        }
      }
    } else {
      segment_with_iterators<T>(*segment, [&](auto it, const auto end) {
        while (it != end) {
          if (!it->is_null() && !including_values.contains(it->value())) {
            status = ValidationStatus::Invalid;
            return;
          }
          ++it;
        }
      });
    }
  }

  return status;
}

}  // namespace

namespace hyrise {

IndValidationRule::IndValidationRule() : AbstractDependencyValidationRule{DependencyType::Inclusion} {}

ValidationResult IndValidationRule::_on_validate(const AbstractDependencyCandidate& candidate) const {
  const auto& ind_candidate = static_cast<const IndCandidate&>(candidate);

  const auto& included_table = Hyrise::get().storage_manager.get_table(ind_candidate.foreign_key_table);
  const auto included_column_id = ind_candidate.foreign_key_column_id;

  const auto& including_table = Hyrise::get().storage_manager.get_table(ind_candidate.primary_key_table);
  const auto including_column_id = ind_candidate.primary_key_column_id;

  if (including_table->column_data_type(including_column_id) != included_table->column_data_type(included_column_id)) {
    return ValidationResult{ValidationStatus::Invalid};
  }

  auto result = ValidationResult{ValidationStatus::Uncertain};
  resolve_data_type(included_table->column_data_type(included_column_id), [&](const auto data_type_t) {
    using ColumnDataType = typename decltype(data_type_t)::type;

    const auto& included_min_max =
        ValidationUtils<ColumnDataType>::get_column_min_max_value(included_table, included_column_id);

    if (!included_min_max) {
      PerformanceWarning("Could not obtain min/max values.");
      result.status = perform_set_based_inclusion_check<ColumnDataType>(including_table, including_column_id,
                                                                        included_table, included_column_id,
                                                                        result.constraints, std::nullopt, std::nullopt);
      return;
    }

    auto including_min_max = std::optional<std::pair<ColumnDataType, ColumnDataType>>{};

    if constexpr (std::is_integral_v<ColumnDataType>) {
      auto including_unique_by_ucc = false;
      auto including_continuous = false;

      const auto& key_constraints = including_table->soft_key_constraints();
      for (const auto& key_constraint : key_constraints) {
        // Already checked that min/max values match. If unique, then IND must be valid.
        if (key_constraint.columns().size() != 1 || *key_constraint.columns().cbegin() != including_column_id) {
          continue;
        }
        including_unique_by_ucc = true;
        including_min_max =
            ValidationUtils<ColumnDataType>::get_column_min_max_value(including_table, including_column_id);
        if (!including_min_max) {
          result.status = perform_set_based_inclusion_check<ColumnDataType>(
              including_table, including_column_id, included_table, included_column_id, result.constraints,
              including_min_max, included_min_max);
          return;
        }

        const auto min = including_min_max->first;
        const auto max = including_min_max->second;
        if (min > included_min_max->first || max < included_min_max->second) {
          result.status = ValidationStatus::Invalid;
          return;
        }

        const auto domain = max - min;
        including_continuous = static_cast<uint64_t>(domain) == including_table->row_count() - 1;
      }

      auto including_unique_by_statistics = false;
      if (!including_unique_by_ucc) {
        const auto& including_statistics =
            ValidationUtils<ColumnDataType>::collect_column_statistics(including_table, including_column_id);
        if (including_statistics.min && including_statistics.max) {
          const auto min = *including_statistics.min;
          const auto max = *including_statistics.max;
          including_min_max = std::make_pair(min, max);
          if (min > included_min_max->first || max < included_min_max->second) {
            result.status = ValidationStatus::Invalid;
            return;
          }

          including_unique_by_statistics =
              including_statistics.all_segments_unique && including_statistics.segments_disjoint;
          including_continuous = including_statistics.segments_continuous;
          if (including_unique_by_statistics) {
            result.constraints[including_table] = std::make_shared<TableKeyConstraint>(
                std::set<ColumnID>{including_column_id}, KeyConstraintType::UNIQUE);
          }
        }
      }

      if ((including_unique_by_ucc || including_unique_by_statistics) && including_continuous) {
        result.status = ValidationStatus::Valid;
        return;
      }
    } else {
      including_min_max =
          ValidationUtils<ColumnDataType>::get_column_min_max_value(including_table, including_column_id);
      if (including_min_max && (including_min_max->first > included_min_max->first ||
                                including_min_max->second < included_min_max->second)) {
        result.status = ValidationStatus::Invalid;
        return;
      }
    }

    result.status = perform_set_based_inclusion_check<ColumnDataType>(
        including_table, including_column_id, included_table, included_column_id, result.constraints, including_min_max,
        included_min_max);
  });

  if (result.status == ValidationStatus::Valid) {
    result.constraints[included_table] = _constraint_from_candidate(candidate);
  }

  return result;
}

}  // namespace hyrise
