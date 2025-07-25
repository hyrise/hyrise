#include "sort.hpp"

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <queue>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "all_type_variant.hpp"
#include "boost/sort/pdqsort/pdqsort.hpp"
#include "hyrise.hpp"
#include "operators/abstract_operator.hpp"
#include "operators/abstract_read_only_operator.hpp"
#include "operators/operator_performance_data.hpp"
#include "resolve_type.hpp"
#include "scheduler/abstract_task.hpp"
#include "scheduler/job_task.hpp"
#include "storage/abstract_segment.hpp"
#include "storage/base_segment_accessor.hpp"
#include "storage/chunk.hpp"
#include "storage/pos_lists/row_id_pos_list.hpp"
#include "storage/reference_segment.hpp"
#include "storage/segment_iterate.hpp"
#include "storage/table.hpp"
#include "storage/value_segment.hpp"
#include "types.hpp"
#include "utils/assert.hpp"
#include "utils/format_duration.hpp"
#include "utils/timer.hpp"

namespace {

using namespace hyrise;  // NOLINT

// Ceiling of integer division
size_t div_ceil(const size_t lhs, const ChunkOffset rhs) {
  DebugAssert(rhs > 0, "Divisor must be larger than 0.");
  return (lhs + rhs - 1u) / rhs;
}

bool is_descending(const SortMode& mode) {
  return mode == SortMode::DescendingNullsFirst || mode == SortMode::DescendingNullsLast;
}

bool is_nulls_first(const SortMode& mode) {
  return mode == SortMode::AscendingNullsFirst || mode == SortMode::DescendingNullsFirst;
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

  // Materialize column by column, starting a new ValueSegment whenever output_chunk_size is reached
  const auto input_chunk_count = unsorted_table->chunk_count();
  const auto output_column_count = unsorted_table->column_count();
  const auto row_count = unsorted_table->row_count();

  // Vector of segments for each chunk
  auto output_segments_by_chunk = std::vector<Segments>(output_chunk_count, Segments(output_column_count));

  auto jobs = std::vector<std::shared_ptr<AbstractTask>>{};
  jobs.reserve(static_cast<size_t>(output_column_count));

  for (auto column_id = ColumnID{0}; column_id < output_column_count; ++column_id) {
    jobs.emplace_back(std::make_shared<JobTask>([&, column_id]() {
      const auto column_data_type = output->column_data_type(column_id);
      const auto column_is_nullable = unsorted_table->column_is_nullable(column_id);

      resolve_data_type(column_data_type, [&](auto type) {
        using ColumnDataType = typename decltype(type)::type;

        auto output_chunk_id = size_t{0};
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

            output_segments_by_chunk[output_chunk_id][column_id] = value_segment;
            // segments_by_columns[column_id].push_back(value_segment);
            value_segment_value_vector = pmr_vector<ColumnDataType>{};
            value_segment_null_vector = pmr_vector<bool>{};

            const auto next_chunk_size =
                std::min(static_cast<size_t>(output_chunk_size), static_cast<size_t>(row_count - row_index));
            value_segment_value_vector.reserve(next_chunk_size);
            if (column_is_nullable) {
              value_segment_null_vector.reserve(next_chunk_size);
            }

            ++output_chunk_id;
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
          output_segments_by_chunk[output_chunk_id][column_id] = value_segment;
        }
      });
    }));
    jobs.back()->schedule();  // schedule job immediately
  }

  Hyrise::get().scheduler()->wait_for_tasks(jobs);

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
    auto jobs = std::vector<std::shared_ptr<AbstractTask>>{};
    jobs.reserve(static_cast<size_t>(column_count));

    for (auto column_id = ColumnID{0}; column_id < column_count; ++column_id) {
      jobs.emplace_back(std::make_shared<JobTask>([&, column_id]() {
        // To keep the implementation simple, we write the output ReferenceSegments column by column.
        // This means that even if input ReferenceSegments share a PosList,
        //  the output will contain independent PosLists. While this is
        // slightly more expensive to generate and slightly less efficient for following operators,
        // we assume that the lion's share of the work has been done
        // before the Sort operator is executed and that the relative cost of this
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
        const auto referenced_table =
            resolve_indirection ? first_reference_segment->referenced_table() : unsorted_table;
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
      }));
      jobs.back()->schedule();  // schedule job immediately
    }

    Hyrise::get().scheduler()->wait_for_tasks(jobs);
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
  auto timer = Timer{};
  const auto& input_table = left_input_table();

  // validate sort definitions
  for (const auto& column_sort_definition : _sort_definitions) {
    Assert(column_sort_definition.column != INVALID_COLUMN_ID, "Sort: Invalid column in sort definition");
    Assert(column_sort_definition.column < input_table->column_count(),
           "Sort: Column ID is greater than table's column count");
  }

  // edge case: empty input table
  if (input_table->row_count() == 0) {
    if (_force_materialization == ForceMaterialization::Yes && input_table->type() == TableType::References) {
      return Table::create_dummy_table(input_table->column_definitions());
    }

    return input_table;
  }

  // After the first (least significant) sort operation has been completed, this holds the order of the table as it has
  // been determined so far. This is not a completely proper PosList on the input table as it might point to
  // ReferenceSegments.
  auto sorted_table = std::shared_ptr<Table>{};

  const auto chunk_count = input_table->chunk_count();
  const auto row_count = input_table->row_count();
  const auto sort_definitions_size = _sort_definitions.size();

  // === precompute field_width, key_width and key_offsets ===

  // based on the sizes of the columns to be sorted by, e.g. if sorting by int, string it should be [4, 8]
  auto field_width = std::vector<size_t>();
  field_width.reserve(sort_definitions_size);
  auto string_columns = std::vector<size_t>();  // indices of sort_definitions that sort columns of type string

  for (auto index = size_t{0}; index < sort_definitions_size; ++index) {
    const auto& def = _sort_definitions[index];
    const auto sort_col = def.column;
    resolve_data_type(input_table->column_data_type(sort_col), [&](auto type) {
      using ColumnDataType = typename decltype(type)::type;
      if constexpr (std::is_same_v<ColumnDataType, pmr_string>) {
        string_columns.emplace_back(index);  // keep track of string columns for fallback comparisons

        // iterate over all chunks to find the longest string in the column
        auto max_string_length = size_t{0};
        for (ChunkID chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
          const auto abstract_segment = input_table->get_chunk(chunk_id)->get_segment(sort_col);
          segment_iterate<ColumnDataType>(*abstract_segment, [&](const auto& val) {
            if (!val.is_null()) {
              const auto string_length = val.value().size();
              if (string_length > max_string_length) {
                max_string_length = string_length;
              }
            }
          });
        }

        field_width.emplace_back(max_string_length + 2);  // store size of the string prefix + 2 for string length
      } else if constexpr (std::is_same_v<ColumnDataType, float>) {
        field_width.push_back(sizeof(double));  // encode float as double for sorting
      } else {
        field_width.push_back(
            sizeof(ColumnDataType));  // store size of the column type, e.g. 4 for int, 8 for double, etc.
      }
    });
  }

  // total width of each normalized key (width of all columns to be sorted by plus null bytes)
  auto key_width = size_t{0};
  for (const auto& column : field_width) {
    key_width += column + 1;  // +1 for null byte
  }

  /**
   * Offsets for each column in the key, i.e. `key_offsets[i]` is the offset of the i-th column in the key.
   * This means, that `buffer[key_offsets[i]]` is the location of the i-th column's value in the key.
  */
  auto key_offsets = std::vector<size_t>(sort_definitions_size);
  key_offsets[0] = 0;  // first column starts at offset 0
  for (auto index = size_t{1}; index < sort_definitions_size; ++index) {
    key_offsets[index] = key_offsets[index - 1] + field_width[index - 1] + 1;  // +1 for null byte
  }

  auto key_buffer = std::vector<uint8_t>();  // buffer to hold all keys for sorting
  auto total_buffer_size = size_t{0};        // total size of the key buffer

  auto chunk_sizes = std::vector<size_t>();  // number of rows per chunk in the input table
  chunk_sizes.reserve(chunk_count);          // reserve space for chunk sizes

  auto row_ids = RowIDPosList{};  // vector to hold row IDs for each row in the table
  row_ids.reserve(row_count);     // reserve space for row IDs

  for (ChunkID chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
    auto chunk = input_table->get_chunk(chunk_id);
    auto row_count_for_chunk = chunk->size();

    chunk_sizes.emplace_back(row_count_for_chunk);
    total_buffer_size += row_count_for_chunk * key_width;  // total size of the keys for this chunk

    for (ChunkOffset row = ChunkOffset{0}; row < row_count_for_chunk; ++row) {
      row_ids.emplace_back(chunk_id, row);  // build array of row ids
    }
  }

  auto row_id_offsets = std::vector<size_t>();  // offsets for each chunk's row IDs
  row_id_offsets.reserve(chunk_count);          // reserve space for offsets

  row_id_offsets.emplace_back(0);  // first chunk starts at offset 0
  for (ChunkID chunk_id = ChunkID{1}; chunk_id < chunk_count; ++chunk_id) {
    auto offset = row_id_offsets[chunk_id - 1] + chunk_sizes[chunk_id - 1];
    row_id_offsets.emplace_back(offset);  // offset for the next chunk
  }

  key_buffer.reserve(total_buffer_size);  // reserve space for all keys
  auto preparation_time = timer.lap();

  /**************************************************************************************************************
   ************************************ Key-Generation and sorting of chunks ************************************
   **************************************************************************************************************/

  // Custom comparator function used for sorting
  auto compare_rows = [&](const RowID a, const RowID b) {
    auto* key_a = &key_buffer[(row_id_offsets[a.chunk_id] + a.chunk_offset) * key_width];
    auto* key_b = &key_buffer[(row_id_offsets[b.chunk_id] + b.chunk_offset) * key_width];

    return memcmp(key_a, key_b, key_width) < 0;
  };

  // job queue for generating keys in parallel
  auto keygen_jobs = std::vector<std::shared_ptr<AbstractTask>>{};
  keygen_jobs.reserve(static_cast<size_t>(chunk_count));
  // for each chunk in table
  for (ChunkID chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
    // spawn thread to generate keys
    keygen_jobs.emplace_back(std::make_shared<JobTask>([&, chunk_id]() {
      auto chunk = input_table->get_chunk(chunk_id);

      // buffer points to the start of this chunk's keys in the global key_buffer
      auto* buffer = &key_buffer[row_id_offsets[chunk_id] * key_width];

      // TODO(someone): How to handle huge number of keycolumns? maybe max. number for key generation
      for (auto index = size_t{0}; index < sort_definitions_size; ++index) {
        auto sort_col = _sort_definitions[index].column;
        auto nulls_first = is_nulls_first(_sort_definitions[index].sort_mode);
        auto descending = is_descending(_sort_definitions[index].sort_mode);

        const auto abstract_segment = chunk->get_segment(sort_col);
        resolve_data_type(input_table->column_data_type(sort_col), [&](auto type) {
          using ColumnDataType = typename decltype(type)::type;

          segment_iterate<ColumnDataType>(*abstract_segment, [&](const auto& val) {
            const auto row = val.chunk_offset();        // get the row offset in the chunk
            auto* key_ptr = &buffer[row * key_width];   // pointer to the start of the key for this row
            auto* dest = key_ptr + key_offsets[index];  // pointer to the destination in the key buffer for this column

            const ColumnDataType value = val.value();
            const auto data_length = field_width[index];

            // Set the first byte to indicate if the value is null or not
            auto null_byte = !val.is_null() ? 0x00 : 0xFF;
            if (nulls_first) {
              null_byte = ~null_byte;
            }
            dest[0] = static_cast<uint8_t>(null_byte);

            // encode the value into the key based on the data type
            if constexpr (std::is_same_v<ColumnDataType, pmr_string>) {
              auto string_len = value.size();
              memset(dest + 1, 0, data_length);            // set all bytes to 0
              memcpy(dest + 1, value.data(), string_len);  // copy the string data into the key buffer
              memset(dest + 1 + data_length - 2, static_cast<uint16_t>(string_len), 2);  // store actual string length
            } else if constexpr (std::is_same_v<ColumnDataType, double>) {
              // Encode double value; reinterpret double as raw 64-bit bits
              auto bits = uint64_t{0};
              memcpy(&bits, &value, sizeof(bits));

              // Flip the bits to ensure lexicographic order matches numeric order
              if (std::signbit(value)) {
                bits = ~bits;  // Negative values are bitwise inverted
              } else {
                bits ^= 0x8000000000000000ULL;  // Flip the sign bit for positive values
              }

              // Write to buffer in big-endian order (MSB first)
              for (auto byte_idx = uint32_t{0}; byte_idx < 8; ++byte_idx) {
                dest[1 + byte_idx] = static_cast<uint8_t>(bits >> ((7 - byte_idx) * 8));
              }
            } else if constexpr (std::is_same_v<ColumnDataType, float>) {
              // Encode float value
              const double value_as_double = val.is_null() ? double() : static_cast<double>(value);
              // Reinterpret double as raw 64-bit bits
              auto bits = uint64_t{0};
              // static_assert(sizeof(double) == sizeof(uint64_t), "Size mismatch");
              memcpy(&bits, &value_as_double, sizeof(bits));

              // Flip the bits to ensure lexicographic order matches numeric order
              if (std::signbit(value_as_double)) {
                bits = ~bits;  // Negative values are bitwise inverted
              } else {
                bits ^= 0x8000000000000000ULL;  // Flip the sign bit for positive values
              }

              // Write to buffer in big-endian order (MSB first)
              for (auto byte_idx = uint32_t{0}; byte_idx < 8; ++byte_idx) {
                dest[1 + byte_idx] = static_cast<uint8_t>(bits >> ((7 - byte_idx) * 8));
              }
            } else if constexpr (std::is_integral<ColumnDataType>::value && std::is_signed<ColumnDataType>::value) {
              // encode int value
              // Bias the value to get a lexicographically sortable encoding
              using UnsignedT = typename std::make_unsigned<ColumnDataType>::type;
              UnsignedT biased =
                  static_cast<UnsignedT>(value) ^ (UnsignedT(1) << (data_length * 8 - 1));  // flip sign bit

              // Store bytes in big-endian order starting at dest[1]
              for (auto byte_idx = size_t{0}; byte_idx < data_length; ++byte_idx) {
                dest[1 + byte_idx] = static_cast<uint8_t>(biased >> ((data_length - 1 - byte_idx) * 8));
              }
            } else {
              throw std::logic_error("Unsupported data type for sorting: " +
                                     std::string(typeid(ColumnDataType).name()));
            }

            // Invert for descending order (excluding the null byte)
            if (descending) {
              for (auto idx = size_t{1}; idx <= data_length; ++idx) {
                dest[idx] = ~dest[idx];
              }
            }
          });
        });
      }  // end of keygen

      // start sorting the current chunk (i.e. the row_id PosList) as part of the current job
      auto chunk_start = row_id_offsets[chunk_id];
      auto chunk_size = chunk_sizes[chunk_id];
      // TODO(someone): use better sorting algorithm, e.g. merge sort
      boost::sort::pdqsort_branchless(row_ids.begin() + chunk_start, row_ids.begin() + chunk_start + chunk_size,
                                      compare_rows);
    }));
    keygen_jobs.back()->schedule();  // schedule job immediately
  }  // end of chunk iteration

  Hyrise::get().scheduler()->wait_for_tasks(keygen_jobs);  // wait for all chunks to be materialized
  auto key_generation_and_sorting_time = timer.lap();

  /**************************************************************************************************************
   ************************************************** Merging ***************************************************
   **************************************************************************************************************/

  //  Prepare first round or runs of chunks [A][B][C][D]->[AB][CD]
  struct Run {
    size_t begin;
    size_t end;
  };  // [begin,end)

  auto runs = std::vector<Run>{};
  runs.reserve(chunk_count);

  for (auto index = size_t{0}; index < chunk_count; ++index) {
    const auto begin = row_id_offsets[index];
    const auto end = begin + chunk_sizes[index];
    runs.emplace_back(Run{begin, end});
  }

  auto work_buffer = RowIDPosList{};
  work_buffer.resize(row_count);
  auto* src = &row_ids;
  auto* dst = &work_buffer;

  while (runs.size() > 1) {
    //  one Job per run pair
    auto merge_jobs = std::vector<std::shared_ptr<AbstractTask>>{};
    //  odd tail, copy unchanged
    if (runs.size() & 1u) {
      merge_jobs.reserve((runs.size() / 2) + 1);
      const auto tail = runs.back();
      std::copy(src->begin() + tail.begin, src->begin() + tail.end, dst->begin() + tail.begin);
      merge_jobs.emplace_back(std::make_shared<JobTask>([&, tail] {
        std::copy(src->begin() + tail.begin, src->begin() + tail.end, dst->begin() + tail.begin);
      }));
      merge_jobs.back()->schedule();
    } else {
      merge_jobs.reserve(runs.size() / 2);
    }

    for (auto index = size_t{0}; index + 1 < runs.size(); index += 2) {
      //  schedule merging of two partitions
      const auto left_part = runs[index];
      const auto right_part = runs[index + 1];

      merge_jobs.emplace_back(std::make_shared<JobTask>([&, left_part, right_part] {
        std::merge(src->begin() + left_part.begin, src->begin() + left_part.end, src->begin() + right_part.begin,
                   src->begin() + right_part.end, dst->begin() + left_part.begin, compare_rows);
      }));
      merge_jobs.back()->schedule();
    }

    Hyrise::get().scheduler()->wait_for_tasks(merge_jobs);
    std::swap(src, dst);

    //  rebuild runs for the next pass
    auto next_runs = std::vector<Run>{};
    next_runs.reserve((runs.size() + 1) / 2);
    for (auto index = size_t{0}; index < runs.size(); index += 2) {
      const auto begin = runs[index].begin;
      const auto end = (index + 1 < runs.size()) ? runs[index + 1].end : runs[index].end;
      next_runs.emplace_back(Run{begin, end});
    }
    runs.swap(next_runs);
  }

  auto& sorted_row_ids = *src;

  assert(sorted_row_ids.size() == row_ids.size() && "We lost a comrade along the way");
  auto merge_sort_time = timer.lap();

  auto& step_performance_data = dynamic_cast<OperatorPerformanceData<OperatorSteps>&>(*performance_data);
  step_performance_data.set_step_runtime(OperatorSteps::Preparation, preparation_time);
  step_performance_data.set_step_runtime(OperatorSteps::MaterializeSortColumns, key_generation_and_sorting_time);
  step_performance_data.set_step_runtime(OperatorSteps::Sort, merge_sort_time);

  // We have to materialize the output (i.e., write ValueSegments) if
  //  (a) it is requested by the user,
  //  (b) a column in the table references multiple tables (see write_reference_output_table for details), or
  //  (c) a column in the table references multiple columns in the same table (which is an unlikely edge case).
  // Cases (b) and (c) can only occur if there is more than one ReferenceSegment in an input chunk.
  auto must_materialize = _force_materialization == ForceMaterialization::Yes;
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
      if (must_materialize) {
        break;
      }
    }
  }

  if (must_materialize) {
    sorted_table = write_materialized_output_table(input_table, std::move(sorted_row_ids), _output_chunk_size);
  } else {
    sorted_table = write_reference_output_table(input_table, std::move(sorted_row_ids), _output_chunk_size);
  }

  const auto& final_sort_definition = _sort_definitions[0];
  // Set the sorted_by attribute of the output's chunks according to the most significant sort operation, which is the
  // column the table was sorted by last.
  const auto output_chunk_count = sorted_table->chunk_count();
  for (auto output_chunk_id = ChunkID{0}; output_chunk_id < output_chunk_count; ++output_chunk_id) {
    const auto& output_chunk = sorted_table->get_chunk(output_chunk_id);
    output_chunk->set_immutable();
    output_chunk->set_individually_sorted_by(final_sort_definition);
  }

  auto write_output_time = timer.lap();
  step_performance_data.set_step_runtime(OperatorSteps::WriteOutput, write_output_time);

  return sorted_table;
}

}  // namespace hyrise
