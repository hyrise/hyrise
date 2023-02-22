#include "ind_validation_rule.hpp"

#include "dependency_discovery/validation_strategy/validation_utils.hpp"
#include "hyrise.hpp"
#include "resolve_type.hpp"
#include "storage/segment_iterate.hpp"
#include "storage/table.hpp"

namespace {

using namespace hyrise;  // NOLINT(build/namespaces)

template <typename T>
std::unordered_set<T> collect_values(const std::shared_ptr<Table>& table, const ColumnID column_id) {
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
      segment_with_iterators<T>(*segment, [&](auto it, const auto end) {
        while (it != end) {
          if (!it->is_null()) {
            distinct_values.insert(it->value());
          }
          ++it;
        }
      });
    }
  }

  return distinct_values;
}

}  // namespace

namespace hyrise {

IndValidationRule::IndValidationRule() : AbstractDependencyValidationRule{DependencyType::Inclusion} {}

ValidationResult IndValidationRule::_on_validate(const AbstractDependencyCandidate& candidate) const {
  const auto& ind_candidate = static_cast<const IndCandidate&>(candidate);

  const auto& included_table = Hyrise::get().storage_manager.get_table(ind_candidate.table_name);
  const auto included_column_id = ind_candidate.column_id;

  const auto& including_table = Hyrise::get().storage_manager.get_table(ind_candidate.foreign_key_table);
  const auto including_column_id = ind_candidate.foreign_key_column_id;

  if (including_table->column_data_type(including_column_id) != included_table->column_data_type(included_column_id)) {
    return ValidationResult{ValidationStatus::Invalid};
  }

  auto status = ValidationStatus::Uncertain;
  resolve_data_type(included_table->column_data_type(included_column_id), [&](const auto data_type_t) {
    using ColumnDataType = typename decltype(data_type_t)::type;

    const auto& included_min_max =
        ValidationUtils<ColumnDataType>::get_column_min_max_value(included_table, included_column_id);

    if (included_min_max) {
      if constexpr (std::is_integral_v<ColumnDataType>) {
        auto including_unique = false;
        auto including_continuous = false;

        const auto& key_constraints = including_table->soft_key_constraints();
        for (const auto& key_constraint : key_constraints) {
          // Already checked that min/max values match. If unique, then IND must be valid.
          if (key_constraint.columns().size() == 1 && key_constraint.columns().front() == including_column_id) {
            including_unique = true;
            const auto& including_min_max =
                ValidationUtils<ColumnDataType>::get_column_min_max_value(including_table, including_column_id);
            if (including_min_max) {
              const auto min = including_min_max->first;
              const auto max = including_min_max->second;
              if (min > included_min_max->first || max < included_min_max->second) {
                status = ValidationStatus::Invalid;
                return;
              }

              const auto domain = max - min - 1;

              including_continuous = domain >= 0 && static_cast<uint64_t>(domain) == including_table->row_count();
            }
          }
        }

        if (!including_unique) {
          const auto& including_statistics =
              ValidationUtils<ColumnDataType>::collect_column_statistics(including_table, included_column_id);
          if (including_statistics.min && including_statistics.max) {
            const auto min = *including_statistics.min;
            const auto max = *including_statistics.max;
            if (min > included_min_max->first || max < included_min_max->second) {
              status = ValidationStatus::Invalid;
              return;
            }

            including_unique = including_statistics.all_segments_unique && including_statistics.segments_disjoint;
            including_continuous = including_statistics.segments_continuous;
          }
        }

        if (including_unique && including_continuous) {
          status = ValidationStatus::Valid;
          return;
        }
      } else {
        const auto& including_min_max =
            ValidationUtils<ColumnDataType>::get_column_min_max_value(including_table, including_column_id);
        if (including_min_max && (including_min_max->first > included_min_max->first ||
                                  including_min_max->second < included_min_max->second)) {
          status = ValidationStatus::Invalid;
          return;
        }
      }
    }

    const auto& including_values = collect_values<ColumnDataType>(including_table, including_column_id);
    const auto& included_values = collect_values<ColumnDataType>(included_table, included_column_id);

    if (included_values.size() > including_values.size()) {
      status = ValidationStatus::Invalid;
      return;
    }

    for (const auto& value : included_values) {
      if (!including_values.contains(value)) {
        status = ValidationStatus::Invalid;
        return;
      }
    }

    status = ValidationStatus::Valid;
  });

  auto result = ValidationResult(status);
  if (status == ValidationStatus::Valid) {
    result.constraints = {_constraint_from_candidate(candidate)};
  }

  return result;
}

}  // namespace hyrise
