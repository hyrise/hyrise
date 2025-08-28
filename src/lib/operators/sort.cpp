#include "sort.hpp"

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <string>
#include <memory>
#include <unordered_map>
#include <utility>
#include <vector>

#include <boost/sort/pdqsort/pdqsort.hpp>

#include "all_type_variant.hpp"
#include "operators/abstract_operator.hpp"
#include "operators/abstract_read_only_operator.hpp"
#include "operators/operator_performance_data.hpp"
#include "resolve_type.hpp"
#include "storage/abstract_segment.hpp"
#include "storage/base_segment_accessor.hpp"
#include "storage/chunk.hpp"
#include "storage/pos_lists/row_id_pos_list.hpp"
#include "storage/reference_segment.hpp"
#include "storage/table.hpp"
#include "storage/value_segment.hpp"
#include "types.hpp"
#include "utils/assert.hpp"
#include "utils/key_normalizer.h"

namespace {

using namespace hyrise;  // NOLINT

// Ceiling of integer division
size_t div_ceil(const size_t lhs, const ChunkOffset rhs) {
  DebugAssert(rhs > 0, "Divisor must be larger than 0.");
  return (lhs + rhs - 1u) / rhs;
}

/**
 *        ____  _  _  __  ____  ____   __  ____   ___
 *       (    \( \/ )/  \(    \(___ \ /  \(___ \ / __)
 *        ) D ( )  /(  O )) D ( / __/(  0 )/ __/(___ \
 *       (____/(__/  \__/(____/(____) \__/(____)(____/
 *
 * 
 * Notes on Segment Accessors:
 *   As discussed on June 30th, you do not need to use segment accessors. They can be handy, but for almost all cases,
 *   using segment_iterate (which will use SegmentAccessors in the background) will be the better option.
 */

// Given an unsorted_table and a pos_list that defines the output order, this materializes all columns in the table,
// creating chunks of output_chunk_size rows at maximum.
std::shared_ptr<Table> write_materialized_output_table(const std::shared_ptr<const Table>& unsorted_table,
                                                       RowIDPosList pos_list, const ChunkOffset output_chunk_size) {
  // First, we create a new table as the output
  // We have decided against duplicating MVCC data in https://github.com/hyrise/hyrise/issues/408
  auto output = std::make_shared<Table>(unsorted_table->column_definitions(), TableType::Data, output_chunk_size);

  // After we created the output table and initialized the column structure, we can start adding values. Because the
  // values are not sorted by input chunks anymore, we can't process them chunk by chunk. Instead the values are copied
  // column by column for each output row.

  const auto output_chunk_count = div_ceil(pos_list.size(), output_chunk_size);
  Assert(pos_list.size() == unsorted_table->row_count(), "Mismatching size of input table and PosList");

  // Vector of segments for each chunk
  auto output_segments_by_chunk = std::vector<Segments>{output_chunk_count};

  // Materialize column by column, starting a new ValueSegment whenever output_chunk_size is reached
  const auto input_chunk_count = unsorted_table->chunk_count();
  const auto output_column_count = unsorted_table->column_count();
  const auto row_count = unsorted_table->row_count();
  for (auto column_id = ColumnID{0}; column_id < output_column_count; ++column_id) {
    const auto column_data_type = output->column_data_type(column_id);
    const auto column_is_nullable = unsorted_table->column_is_nullable(column_id);

    resolve_data_type(column_data_type, [&](auto type) {
      using ColumnDataType = typename decltype(type)::type;

      auto chunk_it = output_segments_by_chunk.begin();
      auto current_segment_size = size_t{0};

      auto value_segment_value_vector = pmr_vector<ColumnDataType>{};
      auto value_segment_null_vector = pmr_vector<bool>{};

      {
        const auto next_chunk_size = std::min(static_cast<size_t>(output_chunk_size), static_cast<size_t>(row_count));
        value_segment_value_vector.reserve(next_chunk_size);
        if (column_is_nullable) {
          value_segment_null_vector.reserve(next_chunk_size);
        }
      }

      auto accessor_by_chunk_id =
          std::vector<std::unique_ptr<AbstractSegmentAccessor<ColumnDataType>>>(unsorted_table->chunk_count());
      for (auto input_chunk_id = ChunkID{0}; input_chunk_id < input_chunk_count; ++input_chunk_id) {
        const auto& abstract_segment = unsorted_table->get_chunk(input_chunk_id)->get_segment(column_id);
        accessor_by_chunk_id[input_chunk_id] = create_segment_accessor<ColumnDataType>(abstract_segment);
      }

      for (auto row_index = size_t{0}; row_index < row_count; ++row_index) {
        const auto [chunk_id, chunk_offset] = pos_list[row_index];

        auto& accessor = accessor_by_chunk_id[chunk_id];
        const auto typed_value = accessor->access(chunk_offset);
        const auto is_null = !typed_value;
        value_segment_value_vector.push_back(is_null ? ColumnDataType{} : typed_value.value());
        if (column_is_nullable) {
          value_segment_null_vector.push_back(is_null);
        }

        ++current_segment_size;

        // Check if value segment is full
        if (current_segment_size >= output_chunk_size) {
          current_segment_size = 0;

          std::shared_ptr<ValueSegment<ColumnDataType>> value_segment;
          if (column_is_nullable) {
            value_segment = std::make_shared<ValueSegment<ColumnDataType>>(std::move(value_segment_value_vector),
                                                                           std::move(value_segment_null_vector));
          } else {
            value_segment = std::make_shared<ValueSegment<ColumnDataType>>(std::move(value_segment_value_vector));
          }

          chunk_it->push_back(value_segment);
          value_segment_value_vector = pmr_vector<ColumnDataType>{};
          value_segment_null_vector = pmr_vector<bool>{};

          const auto next_chunk_size =
              std::min(static_cast<size_t>(output_chunk_size), static_cast<size_t>(row_count - row_index));
          value_segment_value_vector.reserve(next_chunk_size);
          if (column_is_nullable) {
            value_segment_null_vector.reserve(next_chunk_size);
          }

          ++chunk_it;
        }
      }

      // Last segment has not been added
      if (current_segment_size > 0) {
        std::shared_ptr<ValueSegment<ColumnDataType>> value_segment;
        if (column_is_nullable) {
          value_segment = std::make_shared<ValueSegment<ColumnDataType>>(std::move(value_segment_value_vector),
                                                                         std::move(value_segment_null_vector));
        } else {
          value_segment = std::make_shared<ValueSegment<ColumnDataType>>(std::move(value_segment_value_vector));
        }
        chunk_it->push_back(value_segment);
      }
    });
  }

  for (auto& segments : output_segments_by_chunk) {
    output->append_chunk(segments);
  }

  return output;
}

// Given an unsorted_table and an input_pos_list that defines the output order, this writes the output table as a
// reference table. This is usually faster, but can only be done if a single column in the input table does not
// reference multiple tables. An example where this restriction applies is the sorted result of a union between two
// tables. The restriction is needed because a ReferenceSegment can only reference a single table. It does, however,
// not necessarily apply to joined tables, so two tables referenced in different columns is fine.
//
// If unsorted_table is of TableType::Data, this is trivial and the input_pos_list is used to create the output
// reference table. If the input is already a reference table, the double indirection needs to be resolved.
std::shared_ptr<Table> write_reference_output_table(const std::shared_ptr<const Table>& unsorted_table,
                                                    RowIDPosList input_pos_list, const ChunkOffset output_chunk_size) {
  // First we create a new table as the output
  // We have decided against duplicating MVCC data in https://github.com/hyrise/hyrise/issues/408
  auto output_table = std::make_shared<Table>(unsorted_table->column_definitions(), TableType::References);

  const auto resolve_indirection = unsorted_table->type() == TableType::References;
  const auto column_count = output_table->column_count();

  const auto output_chunk_count = div_ceil(input_pos_list.size(), output_chunk_size);
  Assert(input_pos_list.size() == unsorted_table->row_count(), "Mismatching size of input table and PosList");

  // Vector of segments for each chunk
  auto output_segments_by_chunk = std::vector<Segments>(output_chunk_count, Segments(column_count));

  if (!resolve_indirection && input_pos_list.size() <= output_chunk_size) {
    // Shortcut: No need to copy RowIDs if input_pos_list is small enough and we do not need to resolve the indirection.
    const auto output_pos_list = std::make_shared<RowIDPosList>(std::move(input_pos_list));
    auto& output_segments = output_segments_by_chunk.at(0);
    for (auto column_id = ColumnID{0}; column_id < column_count; ++column_id) {
      output_segments[column_id] = std::make_shared<ReferenceSegment>(unsorted_table, column_id, output_pos_list);
    }
  } else {
    for (auto column_id = ColumnID{0}; column_id < column_count; ++column_id) {
      // To keep the implementation simple, we write the output ReferenceSegments column by column. This means that even
      // if input ReferenceSegments share a PosList, the output will contain independent PosLists. While this is
      // slightly more expensive to generate and slightly less efficient for following operators, we assume that the
      // lion's share of the work has been done before the Sort operator is executed and that the relative cost of this
      // is acceptable. In the future, this could be improved.
      auto output_pos_list = std::make_shared<RowIDPosList>();
      output_pos_list->reserve(output_chunk_size);

      // Collect all input segments for the current column
      const auto input_chunk_count = unsorted_table->chunk_count();
      auto input_segments = std::vector<std::shared_ptr<AbstractSegment>>(input_chunk_count);
      for (auto input_chunk_id = ChunkID{0}; input_chunk_id < input_chunk_count; ++input_chunk_id) {
        input_segments[input_chunk_id] = unsorted_table->get_chunk(input_chunk_id)->get_segment(column_id);
      }

      const auto first_reference_segment = std::dynamic_pointer_cast<ReferenceSegment>(input_segments.at(0));
      const auto referenced_table = resolve_indirection ? first_reference_segment->referenced_table() : unsorted_table;
      const auto referenced_column_id =
          resolve_indirection ? first_reference_segment->referenced_column_id() : column_id;

      // write_output_pos_list creates an output reference segment for a given ChunkID, ColumnID and PosList.
      auto output_chunk_id = ChunkID{0};
      const auto write_output_pos_list = [&] {
        DebugAssert(!output_pos_list->empty(), "Asked to write empty output_pos_list");
        output_segments_by_chunk.at(output_chunk_id)[column_id] =
            std::make_shared<ReferenceSegment>(referenced_table, referenced_column_id, output_pos_list);
        ++output_chunk_id;

        output_pos_list = std::make_shared<RowIDPosList>();
        if (output_chunk_id < output_chunk_count) {
          output_pos_list->reserve(output_chunk_size);
        }
      };

      // Iterate over rows in sorted input pos list, dereference them if necessary, and write a chunk every
      // `output_chunk_size` rows.
      const auto input_pos_list_size = input_pos_list.size();
      for (auto input_pos_list_offset = size_t{0}; input_pos_list_offset < input_pos_list_size;
           ++input_pos_list_offset) {
        const auto& row_id = input_pos_list[input_pos_list_offset];
        if (resolve_indirection) {
          const auto& input_reference_segment = static_cast<ReferenceSegment&>(*input_segments[row_id.chunk_id]);
          DebugAssert(input_reference_segment.referenced_table() == referenced_table,
                      "Input column references more than one table");
          DebugAssert(input_reference_segment.referenced_column_id() == referenced_column_id,
                      "Input column references more than one column");
          const auto& input_reference_pos_list = input_reference_segment.pos_list();
          output_pos_list->emplace_back((*input_reference_pos_list)[row_id.chunk_offset]);
        } else {
          output_pos_list->emplace_back(row_id);
        }

        if (output_pos_list->size() == output_chunk_size) {
          write_output_pos_list();
        }
      }
      if (!output_pos_list->empty()) {
        write_output_pos_list();
      }
    }
  }

  for (auto& segments : output_segments_by_chunk) {
    output_table->append_chunk(segments);
  }

  return output_table;
}

}  // namespace

namespace hyrise {

Sort::Sort(const std::shared_ptr<const AbstractOperator>& input_operator,
           const std::vector<SortColumnDefinition>& sort_definitions, const ChunkOffset output_chunk_size,
           const ForceMaterialization force_materialization)
    : AbstractReadOnlyOperator(OperatorType::Sort, input_operator, nullptr,
                               std::make_unique<OperatorPerformanceData<OperatorSteps>>()),
      _sort_definitions(sort_definitions),
      _output_chunk_size(output_chunk_size),
      _force_materialization(force_materialization) {
  DebugAssert(!_sort_definitions.empty(), "Expected at least one sort criterion");
}

const std::vector<SortColumnDefinition>& Sort::sort_definitions() const {
  return _sort_definitions;
}

const std::string& Sort::name() const {
  static const auto name = std::string{"Sort"};
  return name;
}

std::shared_ptr<AbstractOperator> Sort::_on_deep_copy(
    const std::shared_ptr<AbstractOperator>& copied_left_input,
    const std::shared_ptr<AbstractOperator>& /*copied_right_input*/,
    std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& /*copied_ops*/) const {
  return std::make_shared<Sort>(copied_left_input, _sort_definitions, _output_chunk_size, _force_materialization);
}

void Sort::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {}

std::shared_ptr<const Table> Sort::_on_execute() {
  const auto& input_table = left_input_table();

  for (const auto& column_sort_definition : _sort_definitions) {
    Assert(column_sort_definition.column != INVALID_COLUMN_ID, "Sort: Invalid column in sort definition");
    Assert(column_sort_definition.column < input_table->column_count(),
           "Sort: Column ID is greater than table's column count");
  }

  if (_sort_definitions.empty()) {
    return input_table;
  }

  const auto row_count = input_table->row_count();

  if (row_count == 0) {
    if (_force_materialization == ForceMaterialization::Yes && input_table->type() == TableType::References) {
      return Table::create_dummy_table(input_table->column_definitions());
    }
    return input_table;
  }

  auto [normalized_keys, key_size] = KeyNormalizer::normalize_keys_for_table(input_table, _sort_definitions);

  auto key_pointers = std::vector<const unsigned char*>();
  key_pointers.reserve(row_count);
  const auto num_bytes_of_normalized_keys = normalized_keys.size();
  for (auto key_offset = size_t{0}; key_offset < num_bytes_of_normalized_keys; key_offset += key_size) {
    key_pointers.push_back(&normalized_keys[key_offset]);
  }

  {
    struct StableKeyComparator {
      size_t key_size;
      const Table* table;
      const std::vector<SortColumnDefinition>* sort_definitions;
      uint32_t offset_for_row_id = key_size - sizeof(RowID);

      bool operator()(const unsigned char* const a_ptr, const unsigned char* const b_ptr) const {
        const auto& a_row_id = *reinterpret_cast<const RowID*>(a_ptr + offset_for_row_id);
        const auto& b_row_id = *reinterpret_cast<const RowID*>(b_ptr + offset_for_row_id);

        const int key_cmp = std::memcmp(a_ptr, b_ptr, key_size);

        if (key_cmp != 0) {
          return key_cmp < 0;
        }

        for (const auto& sort_def : *sort_definitions) {
          const auto& segment_a = table->get_chunk(a_row_id.chunk_id)->get_segment(sort_def.column);
          const auto& segment_b = table->get_chunk(b_row_id.chunk_id)->get_segment(sort_def.column);

          const auto val_a = (*segment_a)[a_row_id.chunk_offset];
          const auto val_b = (*segment_b)[b_row_id.chunk_offset];

          if (val_a == val_b) {
            continue;
          }

          if (variant_is_null(val_a))
            return sort_def.sort_mode == SortMode::AscendingNullsFirst ||
                   sort_def.sort_mode == SortMode::DescendingNullsFirst;
          if (variant_is_null(val_b))
            return sort_def.sort_mode != SortMode::AscendingNullsFirst &&
                     sort_def.sort_mode != SortMode::DescendingNullsFirst;

          const auto ascending =
              sort_def.sort_mode == SortMode::AscendingNullsFirst || sort_def.sort_mode == SortMode::AscendingNullsLast;

          return ascending ? val_a < val_b : val_b < val_a;
        }

        return false;
      }
    };

    boost::sort::pdqsort(key_pointers.begin(), key_pointers.end(),
                         StableKeyComparator{
                           .key_size = key_size,
                           .table = input_table.get(),
                           .sort_definitions = &_sort_definitions
                         });
  }

  const auto row_id_offset = key_size - sizeof(RowID);
  auto sorted_pos_list = RowIDPosList();
  sorted_pos_list.reserve(row_count);
  for (const auto* key_ptr : key_pointers) {
    sorted_pos_list.emplace_back(*reinterpret_cast<const RowID*>(key_ptr + row_id_offset));
  }

  auto sorted_table = std::shared_ptr<Table>{};
  auto must_materialize = _force_materialization == Sort::ForceMaterialization::Yes;

  {
    const auto input_chunk_count = input_table->chunk_count();
    if (!must_materialize && input_table->type() == TableType::References && input_chunk_count > 1) {
      const auto input_column_count = input_table->column_count();

      for (auto input_column_id = ColumnID{0}; input_column_id < input_column_count; ++input_column_id) {
        const auto& first_segment = input_table->get_chunk(ChunkID{0})->get_segment(input_column_id);
        const auto& first_reference_segment = static_cast<ReferenceSegment&>(*first_segment);

        const auto& common_referenced_table = first_reference_segment.referenced_table();
        const auto& common_referenced_column_id = first_reference_segment.referenced_column_id();

        for (auto input_chunk_id = ChunkID{1}; input_chunk_id < input_chunk_count; ++input_chunk_id) {
          const auto& segment = input_table->get_chunk(input_chunk_id)->get_segment(input_column_id);
          const auto& referenced_table = static_cast<ReferenceSegment&>(*segment).referenced_table();
          const auto& referenced_column_id = static_cast<ReferenceSegment&>(*segment).referenced_column_id();

          if (common_referenced_table != referenced_table || common_referenced_column_id != referenced_column_id) {
            must_materialize = true;
            break;
          }
        }
      }
    }
  }

  if (must_materialize) {
    sorted_table = write_materialized_output_table(input_table, std::move(sorted_pos_list), _output_chunk_size);
  } else {
    sorted_table = write_reference_output_table(input_table, std::move(sorted_pos_list), _output_chunk_size);
  }

  {
    const auto output_chunk_count = sorted_table->chunk_count();
    for (auto output_chunk_id = ChunkID{0}; output_chunk_id < output_chunk_count; ++output_chunk_id) {
      const auto& output_chunk = sorted_table->get_chunk(output_chunk_id);
      output_chunk->set_immutable();
      output_chunk->set_individually_sorted_by(_sort_definitions);
    }
  }

  return sorted_table;
}

}  // namespace hyrise
