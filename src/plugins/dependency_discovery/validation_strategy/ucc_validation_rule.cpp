#include "ucc_validation_rule.hpp"

#include "expression/expression_utils.hpp"
#include "hyrise.hpp"
#include "resolve_type.hpp"
#include "storage/fixed_string_dictionary_segment.hpp"
#include "storage/segment_iterate.hpp"
#include "storage/table.hpp"

namespace hyrise {

UccValidationRule::UccValidationRule() : AbstractDependencyValidationRule{DependencyType::UniqueColumn} {}

ValidationResult UccValidationRule::_on_validate(const AbstractDependencyCandidate& candidate) const {
  const auto& ucc_candidate = static_cast<const UccCandidate&>(candidate);

  auto status = ValidationStatus::Uncertain;
  const auto& table = Hyrise::get().storage_manager.get_table(ucc_candidate.table_name);
  const auto column_id = ucc_candidate.column_id;

  resolve_data_type(table->column_data_type(column_id), [&](const auto data_type_t) {
    using ColumnDataType = typename decltype(data_type_t)::type;

    // Utilize efficient check for uniqueness inside each dictionary segment for a potential early out.
    if (_dictionary_segments_contain_duplicates<ColumnDataType>(table, column_id)) {
      status = ValidationStatus::Invalid;
      return;
    }

    // If we reach here, we have to run the more expensive cross-segment duplicate check.
    if (!_uniqueness_holds_across_segments<ColumnDataType>(table, column_id)) {
      status = ValidationStatus::Invalid;
      return;
    }

    status = ValidationStatus::Valid;
  });

  auto result = ValidationResult(status);
  if (status == ValidationStatus::Valid) {
    result.constraints = {_constraint_from_candidate(candidate)};
  }

  return result;
}

template <typename ColumnDataType>
bool UccValidationRule::_dictionary_segments_contain_duplicates(const std::shared_ptr<Table>& table,
                                                                ColumnID column_id) {
  const auto chunk_count = table->chunk_count();
  // Trigger an early-out if a dictionary-encoded segment's attribute vector is larger than the dictionary. This indica-
  // tes that at least one duplicate value or a NULL value is contained.
  for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
    const auto source_chunk = table->get_chunk(chunk_id);
    if (!source_chunk) {
      continue;
    }
    const auto source_segment = source_chunk->get_segment(column_id);
    if (!source_segment) {
      continue;
    }

    if (const auto& dictionary_segment = std::dynamic_pointer_cast<DictionarySegment<ColumnDataType>>(source_segment)) {
      if (dictionary_segment->unique_values_count() != dictionary_segment->size()) {
        return true;
      }
    } else if (const auto& fixed_string_dictionary_segment =
                   std::dynamic_pointer_cast<FixedStringDictionarySegment<pmr_string>>(source_segment)) {
      if (fixed_string_dictionary_segment->unique_values_count() != fixed_string_dictionary_segment->size()) {
        return true;
      }
    } else {
      // If any segment is not dictionary-encoded, we have to perform a full validation.
      return false;
    }
  }
  return false;
}

template <typename ColumnDataType>
bool UccValidationRule::_uniqueness_holds_across_segments(const std::shared_ptr<Table>& table, ColumnID column_id) {
  const auto chunk_count = table->chunk_count();
  // `distinct_values` collects the segment values from all chunks.
  auto distinct_values = std::unordered_set<ColumnDataType>{};

  for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
    const auto source_chunk = table->get_chunk(chunk_id);
    if (!source_chunk) {
      continue;
    }
    const auto source_segment = source_chunk->get_segment(column_id);
    if (!source_segment) {
      continue;
    }

    const auto expected_distinct_value_count = distinct_values.size() + source_segment->size();

    if (const auto& value_segment = std::dynamic_pointer_cast<ValueSegment<ColumnDataType>>(source_segment)) {
      // Directly insert all values.
      const auto& values = value_segment->values();
      distinct_values.insert(values.cbegin(), values.cend());
    } else if (const auto& dictionary_segment =
                   std::dynamic_pointer_cast<DictionarySegment<ColumnDataType>>(source_segment)) {
      // Directly insert dictionary entries.
      const auto& dictionary = dictionary_segment->dictionary();
      distinct_values.insert(dictionary->cbegin(), dictionary->cend());
    } else {
      // Fallback: Iterate the whole segment and decode its values.
      auto distinct_value_count = distinct_values.size();
      segment_with_iterators<ColumnDataType>(*source_segment, [&](auto it, const auto end) {
        while (it != end) {
          if (it->is_null()) {
            break;
          }
          distinct_values.insert(it->value());
          if (distinct_value_count + 1 != distinct_values.size()) {
            break;
          }
          ++distinct_value_count;
          ++it;
        }
      });
    }

    // If not all elements have been inserted, there must be a duplicate, so the UCC is violated.
    if (distinct_values.size() != expected_distinct_value_count) {
      return false;
    }
  }

  return true;
}
}  // namespace hyrise
