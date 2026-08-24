#pragma once

#include <algorithm>
#include <cstddef>
#include <optional>
#include <vector>

#include "all_type_variant.hpp"
#include "operators/aggregate_dyod/aggregate_dyod_config.hpp"
#include "operators/aggregate_dyod/key_primitives.hpp"
#include "operators/aggregate_dyod/mixed_key_schema.hpp"
#include "operators/aggregate_dyod/numeric_arbitrary_key_schema.hpp"
#include "operators/aggregate_dyod/numeric_short_key_schema.hpp"
#include "operators/aggregate_dyod/string_only_key_schema.hpp"
#include "storage/dictionary_segment.hpp"
#include "storage/table.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace hyrise {

// Key-schema selection for AggregateDYOD.
//
// The schema type is selected once per query by resolve_key_schema() (below), which inspects the group-by columns and
// dispatches to one of a bounded set of concrete schema types; the scatter and merge pipelines are then instantiated
// over that one concrete type. The types (each in its own header, all built on the primitives in key_primitives.hpp):
//
//   NumericShortKeySchema<width>: width in {4,8,12,16,20,24} bytes (numeric-only group-by. hash/equals fixed-size).
//   NumericArbitraryKeySchema: numeric-only group-by wider than 24 bytes; runtime-length hash/equals.
//   MixedKeySchema<len_width>: at least one string and at least one non-string column. len_width in {1,2,4,8} is the
//                             per-string length-prefix field width.
//   StringOnlyKeySchema<len_width>: all columns are strings; a MixedKeySchema with a zero-width numeric prefix.

/**
 * How a query's string key fields are sized: the per-string length-prefix field width and the total inline blob
 * capacity, as chosen by choose_string_key_budget.
 */
struct StringKeyBudget {
  size_t length_field_width{4};
  std::optional<size_t> blob_bytes;
};

/**
 * Derive the tightest string-key field sizing the input table's encodings prove correct.
 *
 * A string group-by column stored as a DictionarySegment<pmr_string> in every chunk has its value lengths bounded
 * exactly by those dictionaries: no row of that column can be longer than the longest dictionary entry. When that holds
 * for every string group-by column and every maximum fits a 1-byte length field, the key can carry 1-byte length fields
 * and an inline blob sized to the summed maxima instead of the flat STRING_BLOB_BYTES_PER_COLUMN per column. The blob
 * is capped at the default capacity, so one long dictionary outlier cannot widen every key; within the cap the bound
 * covers every row and keys never spill, past it the affected rows spill exactly as on the default sizing. Any column
 * outside that shape puts the whole key back on the default sizing, which handles arbitrary lengths via the spill path.
 */
inline StringKeyBudget choose_string_key_budget(const std::vector<ColumnID>& group_by_column_ids,
                                                const Table& input_table, const size_t dictionary_scan_limit) {
  const auto chunk_count = input_table.chunk_count();
  auto blob_bytes = size_t{0};
  auto string_column_count = size_t{0};
  for (const auto column_id : group_by_column_ids) {
    if (input_table.column_data_type(column_id) != DataType::String) {
      continue;
    }
    ++string_column_count;

    auto scanned_entries = size_t{0};
    auto max_length = size_t{0};
    for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
      const auto chunk = input_table.get_chunk(chunk_id);
      if (!chunk) {
        continue;
      }
      const auto segment = chunk->get_segment(column_id);
      const auto* dictionary_segment = dynamic_cast<const DictionarySegment<pmr_string>*>(segment.get());
      if (!dictionary_segment) {
        return {};
      }

      const auto& dictionary = *dictionary_segment->dictionary();
      scanned_entries += dictionary.size();
      if (scanned_entries > dictionary_scan_limit) {
        return {};
      }
      for (const auto& entry : dictionary) {
        max_length = std::max(max_length, entry.size());
      }
    }

    if (max_length > 255) {
      return {};
    }
    blob_bytes += max_length;
  }
  return {.length_field_width = 1,
          .blob_bytes = std::min(blob_bytes, STRING_BLOB_BYTES_PER_COLUMN * string_column_count)};
}

/**
 * The schema family, short-width bucket, and string field sizing resolve_key_schema dispatches on, computed by
 * choose_key_schema.
 */
struct KeySchemaChoice {
  KeyComposition composition{KeyComposition::NumericOnly};
  size_t short_packed_width{0};
  StringKeyBudget string_budget{};
};

/**
 * Inspect the group-by columns and compute which schema type (and, for numeric-only tuples, which short-width
 * bucket) fits them; the type dispatch itself happens in resolve_key_schema.
 */
inline KeySchemaChoice choose_key_schema(const std::vector<ColumnID>& group_by_column_ids, const Table& input_table) {
  auto has_string = false;
  auto has_numeric = false;
  for (const auto column_id : group_by_column_ids) {
    if (input_table.column_data_type(column_id) == DataType::String) {
      has_string = true;
    } else {
      has_numeric = true;
    }
  }

  if (!has_string) {
    const auto layout = compute_key_layout(group_by_column_ids, input_table, 0);
    const auto width = layout.fixed_part_width;
    return {.composition = KeyComposition::NumericOnly, .short_packed_width = width <= 24 ? width : size_t{0}};
  }
  return {.composition = has_numeric ? KeyComposition::Mixed : KeyComposition::StringOnly,
          .short_packed_width = 0,
          .string_budget = choose_string_key_budget(group_by_column_ids, input_table, DICTIONARY_BOUND_SCAN_LIMIT)};
}

/**
 * Resolve the concrete key-schema type for a query's group-by columns and invoke `functor` with the built schema.
 *
 * Inspects the group-by columns, selects one of NumericShortKeySchema / NumericArbitraryKeySchema / MixedKeySchema /
 * StringOnlyKeySchema, builds it, and calls functor with that concrete instance, mirroring resolve_data_type's
 * compile-time dispatch. The entire scatter+merge pipeline runs inside the functor, monomorphized over the concrete
 * schema type so pack/unpack/hash/equals compile to fixed, branch-free code.
 */
template <typename Functor>
void resolve_key_schema(const std::vector<ColumnID>& group_by_column_ids, const Table& input_table,
                        const Functor& functor) {
  Assert(!group_by_column_ids.empty(), "resolve_key_schema requires at least one group-by column.");
  const auto choice = choose_key_schema(group_by_column_ids, input_table);
  switch (choice.composition) {
    case KeyComposition::NumericOnly:
      switch (choice.short_packed_width) {
        case 4:
          functor(NumericShortKeySchema<4>::build(group_by_column_ids, input_table));
          return;
        case 8:
          functor(NumericShortKeySchema<8>::build(group_by_column_ids, input_table));
          return;
        case 12:
          functor(NumericShortKeySchema<12>::build(group_by_column_ids, input_table));
          return;
        case 16:
          functor(NumericShortKeySchema<16>::build(group_by_column_ids, input_table));
          return;
        case 20:
          functor(NumericShortKeySchema<20>::build(group_by_column_ids, input_table));
          return;
        case 24:
          functor(NumericShortKeySchema<24>::build(group_by_column_ids, input_table));
          return;
        default:
          functor(NumericArbitraryKeySchema::build(group_by_column_ids, input_table));
          return;
      }
    case KeyComposition::Mixed:
      switch (choice.string_budget.length_field_width) {
        case 1:
          functor(MixedKeySchema<1>::build(group_by_column_ids, input_table, choice.string_budget.blob_bytes));
          return;
        default:
          functor(MixedKeySchema<4>::build(group_by_column_ids, input_table, choice.string_budget.blob_bytes));
          return;
      }
    case KeyComposition::StringOnly:
      switch (choice.string_budget.length_field_width) {
        case 1:
          functor(StringOnlyKeySchema<1>::build(group_by_column_ids, input_table, choice.string_budget.blob_bytes));
          return;
        default:
          functor(StringOnlyKeySchema<4>::build(group_by_column_ids, input_table, choice.string_budget.blob_bytes));
          return;
      }
  }
  Fail("Invalid KeyComposition.");
}

}  // namespace hyrise
